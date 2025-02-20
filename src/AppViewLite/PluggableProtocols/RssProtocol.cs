using AngleSharp.Io;
using AppViewLite.Models;
using AppViewLite;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.IO.Hashing;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Runtime.InteropServices;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace AppViewLite.PluggableProtocols.Rss
{
    public class RssProtocol : PluggableProtocol
    {
        public new const string DidPrefix = "did:rss:";
        public RssProtocol() : base(DidPrefix)
        {
            Instance = this;
            RefreshFeed = new(TryRefreshFeedCoreAsync);
        }


        public static RssProtocol? Instance;

        public override string? GetIndexableDidText(string did)
        {
            var url = DidToUrl(did);
            return url.Host + " " + url.PathAndQuery;
        }
        public async override Task DiscoverAsync(CancellationToken ct)
        {

            string[] feeds = [
           
                ];
            foreach (var feed in feeds.Take(1))
            {
                RetryInfiniteLoopAsync(async ct =>
                {
                    //await PollFeedAsync(new Uri(feed), ct);
                    await Task.Delay(TimeSpan.FromHours(3), ct);

                }, ct).FireAndForget();
            }

        }

        private static XElement? GetChild(XElement? element, string name)
        {
            return GetChildren(element, name).FirstOrDefault();
        }
        private static XElement[] GetChildren(XElement? element, string name)
        {
            if (element == null) return [];
            return GetNames(name).SelectMany(x => element.Elements(x)).ToArray();
        }

        private static string? GetValue(XElement? element, string name)
        {
            return GetChild(element, name)?.Value;
        }
        private static string? GetAttribute(XElement? element, string name)
        {
            if (element == null) return null;
            return Normalize(
                GetNames(name).Select(x => element.Attribute(name)?.Value).FirstOrDefault(x => x != null)
            );
        }
        public override string? GetDefaultBannerColor(string did)
        {
            return "#999";
        }
        private async Task<RssRefreshInfo> TryRefreshFeedCoreAsync(string did)
        {
            var feedUrl = DidToUrl(did);
            if (UrlToDid(feedUrl) != did)
                throw new Exception("RSS/did roundtrip failed.");


            var (plc, refreshInfo) = Apis.WithRelationshipsLockForDid(did, (plc, rels) => (plc, rels.GetRssRefreshInfo(plc)));
            var now = DateTime.UtcNow;
            refreshInfo ??= new RssRefreshInfo { FirstRefresh = now };

            if (refreshInfo.RedirectsTo != null && (now - refreshInfo.LastRefreshAttempt).TotalDays < 30) return refreshInfo;

            var lastRefreshSucceeded = refreshInfo.RssErrorMessage == null;

            refreshInfo.LastRefreshAttempt = now;
            refreshInfo.LastHttpError = default;
            refreshInfo.LastHttpStatus = default;
            refreshInfo.OtherException = null;
            try
            {

                refreshInfo.RedirectsTo = null;
                using var request = new HttpRequestMessage(System.Net.Http.HttpMethod.Get, feedUrl);

                if (lastRefreshSucceeded)
                {
                    if (refreshInfo.HttpLastModified != null)
                    {
                        request.Headers.IfModifiedSince = new DateTimeOffset(refreshInfo.HttpLastModified.Value, TimeSpan.FromSeconds(refreshInfo.HttpLastModifiedTzOffset));
                    }

                    if (refreshInfo.HttpLastETag != null)
                    {
                        request.Headers.IfNoneMatch.Add(System.Net.Http.Headers.EntityTagHeaderValue.Parse(refreshInfo.HttpLastETag));
                    }
                }


                using var response = await BlueskyEnrichedApis.DefaultHttpClientNoAutoRedirect.SendAsync(request);
                if (response.StatusCode == System.Net.HttpStatusCode.NotModified)
                {
                    return refreshInfo;
                }
                refreshInfo.LastHttpStatus = response.StatusCode;
                var redirectLocation = response.Headers.Location;
                if (redirectLocation != null)
                {
                    if (redirectLocation.AbsoluteUri == feedUrl.AbsoluteUri)
                        throw new UnexpectedFirehoseDataException("Redirect loop");
                    refreshInfo.RedirectsTo = redirectLocation.AbsoluteUri;
                    return refreshInfo;
                }
                response.EnsureSuccessStatusCode();

                var xml = await response.Content.ReadAsStringAsync();
                var xmlTrimmed = xml.AsSpan().Trim();
                if (xmlTrimmed.StartsWith("<!doctype html", StringComparison.OrdinalIgnoreCase) || xmlTrimmed.StartsWith("<html", StringComparison.OrdinalIgnoreCase))
                    throw new UnexpectedFirehoseDataException("Not an RSS feed.");
                var dom = XDocument.Parse(xml).Root;

                var rss = dom;
                rss = GetChild(rss, "channel") ?? rss;
                rss = GetChild(rss, "feed") ?? rss;




                var title = GetValue(rss, "title");
                if (title != null)
                {
                    title = Regex.Replace(Regex.Replace(title, @"\b(RSS feed|Atom feed|RSS|'s blog|blog|'s newsletter|newsletter|posts|articles|medium)\b", string.Empty, RegexOptions.IgnoreCase), @"[\(\[]\s*[\)\]]", string.Empty).Trim([' ', '-', '•']);
                    if (string.IsNullOrEmpty(title))
                        title = null;

                }
                var altUrl = GetAlternateLink(rss);
                if (altUrl != null && string.Equals(NormalizeForApproxUrlEquality(altUrl), NormalizeForApproxUrlEquality(feedUrl), StringComparison.OrdinalIgnoreCase))
                    altUrl = null;
                var altUrlOrFallback = altUrl;
                if (altUrlOrFallback == null)
                    altUrlOrFallback = new Uri(feedUrl.GetLeftPart(UriPartial.Authority) + "/");
                var description = GetValue(rss, "description");
                if (description == feedUrl.Host || description == altUrlOrFallback.ToString()) description = null;
                var subtitle = GetValue(rss, "subtitle");





                var items = GetChildren(rss, "item").Concat(GetChildren(rss, "entry"));
                if (rss != dom)
                    items = items.Concat(GetChildren(dom, "item").Concat(GetChildren(dom, "entry")));
                var minDate = DateTime.MaxValue;
                var maxDate = DateTime.MinValue;
                var postCount = 0;
                Uri? firstUrl = null;
                foreach (var item in items)
                {
                    try
                    {
                        var (date, postUrl) = AddPost(did, item);
                        firstUrl ??= postUrl;
                        if (date < minDate) minDate = date;
                        if (date > maxDate) maxDate = date;
                        postCount++;
                    }
                    catch (Exception ex)
                    {
                        Console.Error.WriteLine(ex);
                    }
                }

                var imageUrlFromXml = GetValue(rss.Element("image"), "url");
                if (imageUrlFromXml != null)
                {
                    refreshInfo.FaviconUrl = BlueskyRelationships.CompressBpe(imageUrlFromXml);
                }
                
                if (refreshInfo.FaviconUrl == null && !refreshInfo.DidAttemptFaviconRetrieval)
                {
                    try
                    {
                        refreshInfo.FaviconUrl = BlueskyRelationships.CompressBpe("!" + (await BlueskyEnrichedApis.GetFaviconUrlAsync(altUrl ?? firstUrl ?? altUrlOrFallback ?? new Uri(feedUrl.GetLeftPart(UriPartial.Authority)))).AbsoluteUri);
                    }
                    catch (Exception)
                    {
                    }
                    refreshInfo.DidAttemptFaviconRetrieval = true;
                }


                OnProfileDiscovered(did, new BlueskyProfileBasicInfo
                {
                    DisplayName = title,
                    Description = (subtitle + "\n\n" + description)?.Trim(),
                    CustomFields = [new CustomFieldProto("web", altUrlOrFallback?.AbsoluteUri)],
                    AvatarCidBytes = refreshInfo.FaviconUrl,
                });

                if (postCount != 0)
                {
                    refreshInfo.HttpLastETag = response.Headers.ETag?.ToString();
                    refreshInfo.HttpLastModified = response.Content.Headers.LastModified?.UtcDateTime;
                    refreshInfo.HttpLastModifiedTzOffset = (int)((response.Content.Headers.LastModified?.Offset.Ticks ?? 0) / TimeSpan.TicksPerSecond);
                    refreshInfo.HttpLastDate = response.Headers.Date?.UtcDateTime;
                    refreshInfo.LastSuccessfulRefresh = now;
                    refreshInfo.XmlOldestPost = minDate;
                    refreshInfo.XmlNewestPost = maxDate;
                    refreshInfo.XmlPostCount = postCount;
                }
            }
            catch (HttpRequestException ex)
            {
                refreshInfo.LastHttpStatus = ex.StatusCode ?? default;
                refreshInfo.LastHttpError = ex.HttpRequestError;
            }
            catch (TaskCanceledException ex)
            {
                refreshInfo.LastHttpError = TimeoutError;
            }
            catch (Exception ex)
            {
                refreshInfo.OtherException = ex.Message;
            }
            finally
            {
                Apis.WithRelationshipsWriteLock(rels => rels.RssRefreshInfos.AddRange(plc, BlueskyRelationships.SerializeProto(refreshInfo)));
            }
            return refreshInfo;
        }

        private static string? NormalizeForApproxUrlEquality(Uri url)
        {
            var host = url.Host;
            if (host.StartsWith("www.", StringComparison.Ordinal))
                host = host.Substring(4);
            return host + "/" + url.AbsolutePath.Trim('/') + url.Query;
        }

        public const HttpRequestError TimeoutError = (HttpRequestError)1001;
        private (DateTime Date, Uri? Url) AddPost(string did, XElement item)
        {
            var title = GetValue(item, "title");
            var url = GetAlternateLink(item);
            var summaryHtml = Normalize(GetValue(item, "description") ?? GetValue(item, "summary"));
            var fullContentHtml = Normalize(item.Element(NsContent + "encoded")) ?? GetValue(item, "content");

            var summaryText = StringUtils.ParseHtmlToText(summaryHtml, out var summaryDom);
            var date = GetValue(item, "pubDate") ??
                GetValue(item, "published") ??
                GetValue(item, "updated") ??
                GetValue(item, "date");

            var guid = GetValue(item, "guid");

            var postId = guid ?? url.AbsoluteUri;
            var dateParsed = date != null ? DateTime.Parse(date, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal) : DateTime.UtcNow;
            var bodyAsText = StringUtils.ParseHtmlToText(fullContentHtml ?? summaryHtml, out var bodyDom);
            if (bodyAsText?.Length >= 500)
                bodyAsText = string.Concat(bodyAsText.AsSpan(0, 400), "…");
            var data = new BlueskyPostData();
            if (url != null)
            {
                if (summaryText != null)
                {
                    if (summaryText.Length < 400 && !IsTrimmedText(bodyAsText, summaryText))
                    {
                        var readMore = Regex.Match(summaryText, @"(?:…|\.\.\.|\b[Rr]ead [Mm]ore|[Cc]ontinue [Rr]eading)\W*$");
                        if (readMore.Success) 
                        {
                            summaryText = summaryText.Substring(0, readMore.Index).TrimEnd([' ', '\n', '.', '…']) + "…";
                        }


                        data.Text = summaryText;
                    }

                }
                data.ExternalUrl = url.AbsoluteUri;
                data.ExternalDescription = bodyAsText;
                data.ExternalTitle = title;
                var img = (bodyDom ?? summaryDom)?.QuerySelector("img");
                if (img != null)
                {
                    try
                    {
                        var size = img.GetAttribute("height") ?? img.GetAttribute("width");
                        if (size == null || int.Parse(size) > 60)
                        {
                            data.ExternalThumbCid = BlueskyRelationships.CompressBpe(new Uri(url, img.GetAttribute("src")).AbsoluteUri);
                        }
                    }
                    catch
                    {
                    }
                }
            }
            else
            {
                data.Text = bodyAsText ?? title;
            }
            OnPostDiscovered(new QualifiedPluggablePostId(did, new NonQualifiedPluggablePostId(CreateSyntheticTid(dateParsed, postId), XxHash64.Hash(MemoryMarshal.AsBytes<char>(postId)))), null, null, data);
            return (dateParsed, url);
        }

        private static bool IsTrimmedText(string? bodyAsText, string summaryText)
        {
            if (bodyAsText == null) return false;
            var withoutEllipsis = summaryText.Replace("read more", null, StringComparison.OrdinalIgnoreCase).Replace("continue reading", null, StringComparison.OrdinalIgnoreCase);
            string Normalize(string text) => Regex.Replace(text, @"\W", string.Empty);
            return Normalize(bodyAsText).StartsWith(Normalize(withoutEllipsis), StringComparison.OrdinalIgnoreCase);
        }

        public override string? GetDefaultAvatar(string did)
        {
            return "/assets/default-rss-avatar.svg";
        }

        private static Uri? GetAlternateLink(XElement item)
        {
            var links = GetChildren(item, "link");
            var link = links.FirstOrDefault(x => GetAttribute(x, "rel") == "alternate") ??
                links.FirstOrDefault(x => GetAttribute(x, "rel") == null);
            var url = Normalize(GetAttribute(link, "href")) ?? Normalize(link);
            return url != null ? new Uri(url) : null;
        }

        private static string Normalize(XElement? element) => Normalize(element?.Value);
        private static string Normalize(string? value)
        {
            var v = value?.Trim();
            return !string.IsNullOrEmpty(v) ? v : null;
        }

        private readonly static XNamespace NsContent = "http://purl.org/rss/1.0/modules/content/";
        private readonly static XNamespace NsAtom = "http://www.w3.org/2005/Atom";
        private readonly static XNamespace NsRdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
        private readonly static XNamespace NsRss = "http://purl.org/rss/1.0/";
        private readonly static XNamespace NsDc = "http://purl.org/dc/elements/1.1/";
        private static IEnumerable<XName> GetNames(string name) => [name, NsAtom + name, NsRss + name, NsRdf + name, NsDc + name];
        protected internal override void EnsureValidDid(string did)
        {
            DidToUrl(did);
        }


        public override string? TryGetOriginalProfileUrl(BlueskyProfile profile)
        {
            return profile.BasicData?.CustomFields?.FirstOrDefault(x => x.Name == "web")?.Value;
        }

        protected Uri DidToUrl(string did)
        {
            var parts = did.Substring(DidPrefixLength).Split(':');
            var schemePrefix = "https://";
            if (parts[0] == "http")
            {
                parts = parts.AsSpan(1).ToArray();
                schemePrefix = "http://";
            }
            if (parts.Length <= 1) throw new UnexpectedFirehoseDataException("Invalid RSS did.");
            return new Uri(schemePrefix + string.Join("/", parts.Select(x => Uri.UnescapeDataString(x.Replace("_", "%")))));
        }

        public static string UrlToDid(Uri url)
        {
            if (url.Scheme != Uri.UriSchemeHttp && url.Scheme != Uri.UriSchemeHttps)
                throw new ArgumentException();
            if (!url.IsDefaultPort || !string.IsNullOrEmpty(url.UserInfo))
                throw new NotSupportedException();
            return
               DidPrefix + (url.Scheme == Uri.UriSchemeHttp ? "http:" : null) + Uri.EscapeDataString(url.AbsoluteUri.AsSpan(url.Scheme.Length + 3)).Replace("_", "%5F").Replace("%2F", ":").Replace("%", "_");
        }


        public async override Task<BlobResult> GetBlobAsync(string did, byte[] cid, ThumbnailSize preferredSize, CancellationToken ct)
        {
            var url = BlueskyRelationships.DecompressBpe(cid)!;
            bool isFavicon = false;
            if (url.StartsWith('!')) { url = url.Substring(1); isFavicon = true; }
            var result = await BlueskyEnrichedApis.GetBlobFromUrl(new Uri(url), preferredSize: preferredSize, ct: ct);
            //result.IsFavIcon = isFavicon;
            return result;
        }

        public override string? GetDisplayNameFromDid(string did)
        {
            var url = DidToUrl(did);
            var host = url.Host;
            if (host.StartsWith("www.", StringComparison.Ordinal))
                host = host.Substring(4);
            if (host.StartsWith("blog.", StringComparison.Ordinal))
                host = host.Substring(5);
            if (host.StartsWith("blogs.", StringComparison.Ordinal))
                host = host.Substring(6);

            var parts = host.Split(".");

            var tld = parts[^1];
            if (tld.Length >= 4) return host; // example.business -> example.business

            var second = parts[^2];
            if (second.Length <= 3 && parts.Length >= 3) // example.co.uk -> example
            {
                return string.Join(".", parts.SkipLast(2));
            }

            return string.Join(".", parts.SkipLast(1)); // example.com -> example

        }

        public override string? TryGetOriginalPostUrl(QualifiedPluggablePostId postId, BlueskyPost post)
        {
            return post.Data?.ExternalUrl;
        }

        public override TimeSpan GetProfilePageMaxPostAge()
        {
            return TimeSpan.FromDays(365 * 50);
        }

        private readonly TaskDictionary<string, RssRefreshInfo> RefreshFeed;

        public async Task<RssRefreshInfo> MaybeRefreshFeedAsync(string did)
        {
            var refreshData = Apis.WithRelationshipsLockForDid(did, (plc, rels) => rels.GetRssRefreshInfo(plc));

            var now = DateTime.UtcNow;
            if (refreshData == null || (now - refreshData.LastRefreshAttempt).TotalHours > 6)
            {
                return await RefreshFeed.GetValueAsync(did);
            }
            return refreshData;
        }

        public static async Task<Uri?> TryGetFeedUrlFromPageAsync(string responseText, Uri url)
        {
            var dom = StringUtils.ParseHtml(responseText);
            var feedUrl = dom.QuerySelectorAll("link[type='application/atom+xml'],link[type='application/rss+xml']")
                .Select(x => Uri.TryCreate(url, x.GetAttribute("href"), out var u) ? u : null)
                .Where(x => x != null)
                .MinBy(x => x!.AbsoluteUri.Length);
            return feedUrl;
        }
    }
}

