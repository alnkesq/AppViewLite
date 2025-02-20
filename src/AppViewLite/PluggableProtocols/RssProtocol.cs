using AppViewLite.Models;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO.Hashing;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace AppViewLite.PluggableProtocols.Rss
{
    public class RssProtocol : PluggableProtocol
    {
        private new const string DidPrefix = "did:rss:";
        public RssProtocol() : base(DidPrefix)
        {
        }

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
                    await PollFeedAsync(new Uri(feed), ct);
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
        private async Task PollFeedAsync(Uri feedUrl, CancellationToken ct)
        {
            var did = UrlToDid(feedUrl);
            var roundtrip = DidToUrl(did);
            if (roundtrip != feedUrl)
                throw new Exception("RSS/did roundtrip failed.");
            var xml = await BlueskyEnrichedApis.DefaultHttpClient.GetStringAsync(feedUrl, ct);
            var dom = XDocument.Parse(xml).Root;

            var rss = dom;
            rss = GetChild(rss, "channel") ?? rss;
            rss = GetChild(rss, "feed") ?? rss;



            {
                var title = GetValue(rss, "title");
                if (title != null)
                {
                    title = Regex.Replace(Regex.Replace(title, @"\b(RSS feed|Atom feed|RSS|'s blog|blog|'s newsletter|newsletter|posts|articles|medium)\b", string.Empty, RegexOptions.IgnoreCase), @"[\(\[]\s*[\)\]]", string.Empty).Trim([' ', '-', '•']);
                    if (string.IsNullOrEmpty(title))
                        title = null;

                }
                var url = GetAlternateLink(rss);
                if (url == null)
                    url = feedUrl.GetLeftPart(UriPartial.Authority) + "/";
                var description = GetValue(rss, "description");
                if (description == feedUrl.Host || description == url) description = null;
                var subtitle = GetValue(rss, "subtitle");

                var rssFavicon = BlueskyRelationships.CompressBpe(GetValue(rss.Element("image"), "url"));
                if (rssFavicon == null)
                {
                    var prev = Apis.WithRelationshipsLockForDid(did, (plc, rels) => rels.GetProfileBasicInfo(plc));
                    rssFavicon = prev?.AvatarCidBytes
                        ?? BlueskyRelationships.CompressBpe("!" + (await BlueskyEnrichedApis.GetFaviconUrlAsync(new Uri(url ?? feedUrl.GetLeftPart(UriPartial.Authority)))).AbsoluteUri);
                }
                OnProfileDiscovered(did, new BlueskyProfileBasicInfo
                {
                    DisplayName = title,
                    Description = (subtitle + "\n\n" + description)?.Trim(),
                    CustomFields = [new CustomFieldProto("web", url)],
                    AvatarCidBytes = rssFavicon
                });

            }

            var items = GetChildren(rss, "item").Concat(GetChildren(rss, "entry"));
            if (rss != dom)
                items = items.Concat(GetChildren(dom, "item").Concat(GetChildren(dom, "entry")));
            foreach (var item in items)
            {
                try
                {
                    AddPost(did, item);
                }
                catch (Exception ex)
                {
                    Console.Error.WriteLine(ex);
                }
            }
        }
        private void AddPost(string did, XElement item)
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

            var postId = guid ?? url;
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
                        var readMore = Regex.Match(summaryText, @"(?:…|\.\.\.|\b[Rr]ead [Mm]ore)\W*$");
                        if (readMore.Success) 
                        {
                            summaryText = summaryText.Substring(0, readMore.Index).TrimEnd([' ', '\n', '.', '…']) + "…";
                        }


                        data.Text = summaryText;
                    }

                }
                data.ExternalUrl = url;
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
                            data.ExternalThumbCid = BlueskyRelationships.CompressBpe(new Uri(new Uri(url), img.GetAttribute("src")).AbsoluteUri);
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
        }

        private static bool IsTrimmedText(string? bodyAsText, string summaryText)
        {
            if (bodyAsText == null) return false;
            var withoutEllipsis = summaryText.Replace("read more", null, StringComparison.OrdinalIgnoreCase);
            string Normalize(string text) => Regex.Replace(text, @"\W", string.Empty);
            return Normalize(bodyAsText).StartsWith(Normalize(withoutEllipsis), StringComparison.OrdinalIgnoreCase);
        }

        private static string? GetAlternateLink(XElement item)
        {
            var links = GetChildren(item, "link");
            var link = links.FirstOrDefault(x => GetAttribute(x, "rel") == "alternate") ??
                links.FirstOrDefault(x => GetAttribute(x, "rel") == null);
            var url = Normalize(GetAttribute(link, "href")) ?? Normalize(link);
            return url;
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
    }
}

