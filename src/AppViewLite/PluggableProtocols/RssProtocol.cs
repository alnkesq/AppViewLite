using AngleSharp.Dom;
using AppViewLite.Models;
using AppViewLite.Numerics;
using AppViewLite.Storage;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO.Hashing;
using System.Linq;
using System.Net.Http;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;
using System.Net.Http.Json;
using AppViewLite.PluggableProtocols.Reddit;

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

        private ConcurrentSet<Plc> ScheduledRefreshes = new();

        public override async Task DiscoverAsync(CancellationToken ct)
        {
            if (!(AppViewLiteConfiguration.GetBool(AppViewLiteParameter.APPVIEWLITE_ENABLE_RSS, false)))
                return;
            while (true)
            {

                await Task.Delay(TimeSpan.FromMinutes(10), ct);

                var followerToActualFeeds = new Dictionary<Plc, HashSet<Plc>>();
                var ctx = RequestContext.CreateForFirehose("Rss");
                Apis.WithRelationshipsLock(rels =>
                {
                    var rssToRefreshInfo = rels.RssRefreshInfos
                        .EnumerateSortedGrouped()
                        .Select(x => (Plc: x.Key, LatestRssRefreshInfo: x.ValueChunks[^1]));

                    var rssToFollowers = rels.RssFeedToFollowers.EnumerateSortedGrouped();
                    var rssAll = SimpleJoin.JoinPresortedAndUnique(rssToRefreshInfo, x => x.Plc, rssToFollowers, x => x.Key);

                    foreach (var item in rssAll)
                    {
                        var rssPlc = item.Key;
                        var followers = item.Right.ValueChunks;
                        if (followers == null) continue;

                        if (ScheduledRefreshes.Contains(rssPlc)) continue;

                        var refreshInfo = item.Left.LatestRssRefreshInfo.Length != 0 ? BlueskyRelationships.DeserializeProto<RssRefreshInfo>(item.Left.LatestRssRefreshInfo.AsSmallSpan()) : new RssRefreshInfo() { FirstRefresh = DateTime.UtcNow };
                        var nextRefreshTime = GetNextRefreshTime(refreshInfo);

                        if (nextRefreshTime != null)
                        {
                            var remainingTime = nextRefreshTime.Value - DateTime.UtcNow;
                            if (remainingTime < TimeSpan.Zero)
                                remainingTime = TimeSpan.Zero;

                            if (remainingTime.TotalHours < 6)
                            {
                                var isStillFollowed = followers.Any(chunk => chunk.AsEnumerable().Any(user =>
                                {
                                    if (!followerToActualFeeds.TryGetValue(user, out var followees))
                                    {
                                        var profileProto = rels.AppViewLiteProfiles.TryGetPreserveOrderSpanLatest(user, out var appviewProfileBytes) ? BlueskyRelationships.DeserializeProto<AppViewLiteProfileProto>(appviewProfileBytes.AsSmallSpan()) : null;
                                        followerToActualFeeds[user] = followees = profileProto?.PrivateFollows?.Select(x => new Plc(x.Plc)).ToHashSet() ?? [];
                                    }
                                    return followees.Contains(rssPlc);
                                }));
                                if (!isStillFollowed) continue;

                                ScheduledRefreshes.TryAdd(rssPlc);
                                var did = rels.GetDid(rssPlc);
                                Apis.DispatchOutsideTheLock(() =>
                                {
                                    ScheduleRefreshAsync(rssPlc, did, remainingTime, ct).FireAndForget();
                                });
                            }
                        }

                    }

                }, ctx);

            }
        }

        private async Task ScheduleRefreshAsync(Plc rssPlc, string did, TimeSpan remainingTime, CancellationToken ct)
        {
            await Task.Delay(remainingTime, ct);

            if (!ScheduledRefreshes.Contains(rssPlc)) return;

            await RefreshFeed.GetValueAsync(did, RequestContext.CreateForFirehose("Rss"));
        }

        public static DateTime? GetNextRefreshTime(RssRefreshInfo refreshInfo)
        {
            if (refreshInfo.LastRefreshAttempt == default) return DateTime.UtcNow;

            var averageDaysBetweenPosts = GetAverageDaysBetweenPosts(refreshInfo);
            if (averageDaysBetweenPosts == null) return null;

            averageDaysBetweenPosts = Math.Clamp(averageDaysBetweenPosts.Value * 0.5, TimeSpan.FromMinutes(15).TotalDays, TimeSpan.FromDays(90).TotalDays);
            return refreshInfo.LastRefreshAttempt.AddDays(averageDaysBetweenPosts.Value);

        }

        public static double? GetAverageDaysBetweenPosts(RssRefreshInfo refreshInfo)
        {
            if (refreshInfo.XmlOldestPost == null || refreshInfo.LastSuccessfulRefresh == null || refreshInfo.XmlPostCount == 0)
            {
                return null;
            }
            return Math.Max(0, (refreshInfo.LastSuccessfulRefresh.Value - refreshInfo.XmlOldestPost.Value).TotalDays) / refreshInfo.XmlPostCount;
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

        private async Task<RssRefreshInfo> TryRefreshFeedCoreAsync(string did, RequestContext ctx)
        {
            if (!AppViewLiteConfiguration.GetBool(AppViewLiteParameter.APPVIEWLITE_ENABLE_RSS, false))
                throw new NotSupportedException("RSS is not enabled on the current instance.");
            var feedUrl = DidToUrl(did)!;

            if (feedUrl.HasHostSuffix("reddit.com"))
            {
                if ((feedUrl.Host != "www.reddit.com" ||
                    feedUrl.AbsolutePath != feedUrl.AbsolutePath.ToLowerInvariant()) ||
                    feedUrl.AbsolutePath.EndsWith("/", StringComparison.Ordinal) ||
                    feedUrl.GetSegments().Length != 2 ||
                    feedUrl.AbsolutePath.Contains("//", StringComparison.Ordinal) ||
                    feedUrl.Query.Length != 0 ||
                    feedUrl.Fragment.Length != 0
                    )
                    throw new Exception("Reddit RSS host should be normalized to www.reddit.com, path lowercased, no trailing slash.");
                var segments = feedUrl.GetSegments();
                if (segments.Length < 2 || !(segments[0] is "r" or "user"))
                    throw new Exception("Only subreddit and user URLs are supported.");
            }
            if (UrlToDid(feedUrl) != did)
                throw new Exception("RSS/did roundtrip failed.");

            var (plc, refreshInfo) = Apis.WithRelationshipsLockForDid(did, (plc, rels) => (plc, rels.GetRssRefreshInfo(plc)), ctx);
            ScheduledRefreshes.Remove(plc);
            var now = DateTime.UtcNow;
            refreshInfo ??= new RssRefreshInfo { FirstRefresh = now };

            if (refreshInfo.RedirectsTo != null && (now - refreshInfo.LastRefreshAttempt).TotalDays < 30 && IsBadRedirect(new Uri(refreshInfo.RedirectsTo)) == null) return refreshInfo;

            var lastRefreshSucceeded = refreshInfo.RssErrorMessage == null;

            refreshInfo.LastRefreshAttempt = now;
            refreshInfo.LastHttpError = default;
            refreshInfo.LastHttpStatus = default;
            refreshInfo.OtherException = null;
            try
            {

                refreshInfo.RedirectsTo = null;

                var (virtualRssRedirect, virtualRss) = TryGetVirtualRssDelegate(feedUrl, did);
                if (virtualRssRedirect != null) throw new UnexpectedFirehoseDataException("The specified virtual RSS DID was recognized, but it is not canonical. The canonical DID is " + UrlToDid(virtualRssRedirect));

                if (virtualRss != null)
                {
                    var result = await virtualRss();
                    var posts = result.Posts;
                    if (posts.Length == 0) throw new UnexpectedFirehoseDataException("Virtual RSS feed returned zero posts.");
                    foreach (var post in posts)
                    {
                        OnPostDiscovered(post.PostId, null, null, post.Data, ctx);
                    }
                    if (result.Profile != null)
                    {
                        OnProfileDiscovered(did, result.Profile, ctx);
                    }
                    refreshInfo.HttpLastETag = null;
                    refreshInfo.HttpLastModified = null;
                    refreshInfo.HttpLastModifiedTzOffset = 0;
                    refreshInfo.HttpLastDate = null;
                    refreshInfo.LastSuccessfulRefresh = now;
                    refreshInfo.XmlOldestPost = posts.Min(x => x.Date);
                    refreshInfo.XmlNewestPost = posts.Max(x => x.Date);
                    refreshInfo.XmlPostCount = posts.Length;
                    return refreshInfo;

                }

                var feedUrlForRequest = (
                    feedUrl.HasHostSuffix("reddit.com") ?
                    (
                        feedUrl.AbsolutePath.StartsWith("/r/", StringComparison.Ordinal) ? new Uri(feedUrl.AbsoluteUri + "/top/.rss?sort=top&t=day") :
                        feedUrl.AbsolutePath.StartsWith("/user/", StringComparison.Ordinal) ? new Uri(feedUrl.AbsoluteUri + "/submitted/.rss?sort=new") :
                        null
                    ) : null)
                    ?? feedUrl;

                using var request = new HttpRequestMessage(System.Net.Http.HttpMethod.Get, feedUrlForRequest);

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
                var redirectLocation = response.GetRedirectLocationUrl();
                if (redirectLocation != null)
                {
                    var error = IsBadRedirect(redirectLocation);
                    if (error != null)
                        throw new UnexpectedFirehoseDataException(error);
                    refreshInfo.RedirectsTo = redirectLocation.AbsoluteUri;
                    return refreshInfo;
                }

                if (response.StatusCode == System.Net.HttpStatusCode.NotFound && feedUrl.HasHostSuffix("tumblr.com"))
                    throw new UnexpectedFirehoseDataException("Tumblr blog not found, or blog requires login.");

                response.EnsureSuccessStatusCode();

                var xml = await response.Content.ReadAsStringAsync();
                var xmlTrimmed = xml.AsSpan().Trim();
                if (xmlTrimmed.StartsWith("<!doctype html", StringComparison.OrdinalIgnoreCase) || xmlTrimmed.StartsWith("<html", StringComparison.OrdinalIgnoreCase))
                    throw new UnexpectedFirehoseDataException("Not an RSS feed.");
                var dom = XDocument.Parse(xml).Root!;

                var rss = dom;
                rss = GetChild(rss, "channel") ?? rss;
                rss = GetChild(rss, "feed") ?? rss;




                var title = GetValue(rss, "title");
                if (title != null)
                {
                    if (title.StartsWith("https://", StringComparison.Ordinal) && StringUtils.TryParseUri(title) is Uri url && url.HasHostSuffix(feedUrl.GetDomainTrimWww()))
                    {
                        title = null;
                    }
                    else
                    {
                        title = Regex.Replace(Regex.Replace(title, @"\b(RSS feed|Atom feed|RSS|'s blog|'s newsletter|posts|articles|medium)\b", string.Empty, RegexOptions.IgnoreCase), @"[\(\[]\s*[\)\]]", string.Empty).Trim([' ', '-', '•']);
                        if (string.IsNullOrEmpty(title))
                            title = null;
                    }

                }


                var altUrl = GetAlternateLink(rss);
                if (altUrl != null && string.Equals(NormalizeForApproxUrlEquality(altUrl), NormalizeForApproxUrlEquality(feedUrl), StringComparison.OrdinalIgnoreCase))
                    altUrl = null;


                if (feedUrl.HasHostSuffix("reddit.com"))
                {
                    var label = GetAttribute(GetChild(rss, "category"), "label");
                    if (label != null && label.StartsWith("r/", StringComparison.Ordinal))
                    {
                        // get display case
                        title = label.Split('/')[1];
                    }
                    else title = null;

                }


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
                        var (date, postUrl) = AddPost(did, item, postIndex: postCount, feedUrl, ctx);
                        firstUrl ??= postUrl;
                        if (date < minDate) minDate = date;
                        if (date > maxDate) maxDate = date;
                        postCount++;
                    }
                    catch (Exception ex)
                    {
                        LogLowImportanceException(ex);
                    }
                }

                var imageUrlFromXml = GetValue(rss.Element("image"), "url");
                if (imageUrlFromXml != null)
                {
                    refreshInfo.FaviconUrl = UrlToCid(imageUrlFromXml);
                }

                if (refreshInfo.FaviconUrl == null && !refreshInfo.DidAttemptFaviconRetrieval && !feedUrl.HasHostSuffix("youtube.com") && !feedUrl.HasHostSuffix("reddit.com"))
                {
                    try
                    {
                        var faviconUrl = await BlueskyEnrichedApis.GetFaviconUrlAsync(altUrl ?? firstUrl ?? altUrlOrFallback ?? new Uri(feedUrl.GetLeftPart(UriPartial.Authority)));
                        if (faviconUrl.Host != "assets.tumblr.com")
                            refreshInfo.FaviconUrl = UrlToCid("!" + faviconUrl.AbsoluteUri);
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
                }, ctx);

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
            catch (TaskCanceledException)
            {
                refreshInfo.LastHttpStatus = default;
                refreshInfo.LastHttpError = TimeoutError;
            }
            catch (Exception ex)
            {
                refreshInfo.LastHttpStatus = default;
                refreshInfo.LastHttpError = default;
                refreshInfo.OtherException = ex.Message;
            }
            finally
            {
                if (refreshInfo.LastSuccessfulRefresh != now)
                    Log("RSS refresh failed for " + feedUrl + ": " + refreshInfo.RssErrorMessage);
                Apis.WithRelationshipsWriteLock(rels => rels.RssRefreshInfos.AddRange(plc, BlueskyRelationships.SerializeProto(refreshInfo)), ctx);
            }
            return refreshInfo;
        }

        private static string? IsBadRedirect(Uri redirectsTo)
        {
            if (redirectsTo.HasHostSuffix("tumblr.com") && redirectsTo.AbsolutePath.StartsWith("/privacy/consent", StringComparison.OrdinalIgnoreCase))
                return "Tumblr returned a cookie consent form instead of an RSS feed.";
            if (redirectsTo.HasHostSuffix("tumblr.com") && redirectsTo.AbsolutePath == "/safe-mode")
                return "This Tumblr blog isn't accessible via RSS because it is considered adult content.";
            return null;
        }

        private static string? NormalizeForApproxUrlEquality(Uri url)
        {
            var host = url.GetDomainTrimWww();
            return host + "/" + url.AbsolutePath.Trim('/') + url.Query;
        }

        public const HttpRequestError TimeoutError = (HttpRequestError)1001;
        private (DateTime Date, Uri? Url) AddPost(string did, XElement item, int postIndex, Uri feedUrl, RequestContext ctx)
        {
            var title = GetValue(item, "title");
            if (title != null && title.Contains('&')) title = StringUtils.ParseHtmlToText(title, out _, x => null).Text;
            var url = GetAlternateLink(item);
            var summaryHtml = Normalize(GetValue(item, "description") ?? GetValue(item, "summary"));
            var mediaGroup = item.Element(NsMedia + "group");
            var fullContentHtml =
                Normalize(item.Element(NsContent + "encoded")) ??
                GetValue(item, "content") ??
                Normalize(mediaGroup?.Element(NsMedia + "description"));

            var (summaryText, summaryFacets) = StringUtils.ParseHtmlToText(summaryHtml, out var summaryDom, x => StringUtils.DefaultElementToFacet(x, url));
            var date = GetValue(item, "pubDate") ??
                GetValue(item, "published") ??
                GetValue(item, "updated") ??
                GetValue(item, "date");

            var guid = GetValue(item, "guid");



            var dateParsed = date != null ? DateTime.Parse(date, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal) : DateTime.UtcNow;


            var (bodyAsText, bodyFacets) = StringUtils.ParseHtmlToText(fullContentHtml ?? summaryHtml, out var bodyDom, x => StringUtils.DefaultElementToFacet(x, url));
            NonQualifiedPluggablePostId postId;
            var hasFullContent = false;

            int? pluggableLikeCountForScoring = null;
            if (feedUrl.HasHostSuffix("reddit.com"))
            {
                var isUserRss = feedUrl.AbsolutePath.StartsWith("/user/", StringComparison.Ordinal);
                if (!isUserRss) // for user RSS, we sort by new, so rank isn't meaningful
                    pluggableLikeCountForScoring = EstimateLikesFromRank(postIndex, 1.1);
                var commentsUrl = url;
                var link = bodyDom?.QuerySelectorAll("a").FirstOrDefault(x => x.TextContent == "[link]");
                if (link != null)
                {
                    var comment = bodyDom!.Descendants<IComment>().LastOrDefault(x => x.TextContent.Trim() == "SC_ON");
                    if (comment != null)
                    {
                        while (comment.NextSibling != null)
                            comment.NextSibling.RemoveFromParent();
                    }
                    else
                    {
                        var submittedBy = bodyDom!.Descendants<IText>().FirstOrDefault(x => x.TextContent.Trim() == "submitted by");
                        if (submittedBy != null)
                        {
                            if (isUserRss)
                            {
                                var to = submittedBy.NextElementSibling?.NextSibling;
                                to?.NextSibling?.RemoveFromParent();
                                to?.RemoveFromParent();
                            }
                            submittedBy.NextElementSibling?.Remove();
                            submittedBy.Remove();
                        }
                    }
                    link.Remove();
                    bodyDom!.QuerySelectorAll("a").FirstOrDefault(x => x.TextContent == "[comments]")?.Remove();
                    url = new Uri(feedUrl, link.GetAttribute("href"));
                    if (url.HasHostSuffix("reddit.com") || url.HasHostSuffix("redd.it"))
                    {
                        hasFullContent = true;
                        foreach (var externalImg in bodyDom!.QuerySelectorAll("img[src*='external-preview.redd.it']").ToArray())
                        {
                            externalImg.ParentElement!.Remove();
                        }
                    }
                    (bodyAsText, bodyFacets) = StringUtils.HtmlToFacets(bodyDom, x => StringUtils.DefaultElementToFacet(x, url, includeInlineImages: true));

                }
                var segments = commentsUrl!.GetSegments();
                var h = isUserRss ? "/r/" + segments[1] + "/comments/" + segments[3] : segments[3];
                postId = new NonQualifiedPluggablePostId(CreateSyntheticTid(dateParsed, h), h);
            }
            else if (feedUrl.HasHostSuffix("tumblr.com"))
            {
                hasFullContent = true;

                if (bodyDom == null)
                {
                    bodyDom = StringUtils.AsWrappedTextNode(title ?? string.Empty);
                }

                foreach (var altTag in bodyDom!.QuerySelectorAll(".tmblr-alt-text-helper").ToArray())
                {
                    altTag.Remove();
                }

                var leafPostId = GetTumblrPostId(url!, CreateSyntheticTid(dateParsed, url!.AbsoluteUri));

                if (leafPostId.BlogId != feedUrl!.Host.Split('.')[0])
                    throw new Exception("Non-matching tumblr host");
                var leafPost = UnfoldTumblrPosts(bodyDom!, leafPostId, null, null);





                if (leafPost.QuotedPost == null && IsTumblrAttribution(leafPost.Content) is { } shortenedThread)
                {
                    var text = shortenedThread.TextContent;
                    var space = text.IndexOf(' ');
                    if (space != -1)
                    {
                        var prevColon = text.AsSpan(0, space).LastIndexOf(':');
                        if (prevColon != -1)
                            text = text.Substring(prevColon + 1);
                    }
                    OnPostDiscovered(leafPostId.AsQualifiedPostId, null, null, new BlueskyPostData
                    {
                        ExternalTitle = text,
                        ExternalUrl = "https://www.tumblr.com/" + leafPostId.BlogId + "/" + leafPostId.PostId,
                    }, ctx: ctx);
                }
                else
                {

                    if (title != null && (bodyAsText == null || NormalizeTumblrTextForComparison(bodyAsText).Contains(NormalizeTumblrTextForComparison(title), StringComparison.OrdinalIgnoreCase)))
                    {
                        title = null;
                    }


                    var sequence = new List<TumblrPost>();
                    while (leafPost != null)
                    {
                        sequence.Add(leafPost);
                        leafPost = leafPost.QuotedPost;
                    }
                    sequence.Reverse();

                    leafPost = sequence[^1];
                    if (leafPost.Content.Length == 0)
                    {
                        sequence.RemoveAt(sequence.Count - 1);
                        OnRepostDiscovered(leafPostId.AsQualifiedPostId.Did, sequence[^1].PostId.AsQualifiedPostId, dateParsed, ctx: ctx);
                    }

                    QualifiedPluggablePostId? prev = null;
                    TumblrPostId rootPostId = default;
                    foreach (var post in sequence)
                    {
                        if (rootPostId == default) rootPostId = post.PostId;

                        var pair = StringUtils.HtmlToFacets(post.Content, x => StringUtils.DefaultElementToFacet(x, null));

                        if (title != null)
                        {
                            if (!title.StartsWith(post.PostId.BlogId + ":", StringComparison.OrdinalIgnoreCase))
                            {
                                var prefix = ">" + title + "\n";
                                var offset = Encoding.UTF8.GetByteCount(prefix);
                                pair.Text = prefix + pair.Text;
                                if (pair.Facets != null)
                                {
                                    foreach (var facet in pair.Facets)
                                    {
                                        facet.Start += offset;
                                    }
                                }
                            }
                            title = null;
                        }

                        var subpostData = new BlueskyPostData
                        {
                            Text = pair.Text,
                            Facets = pair.Facets,
                            Media = GetMediaFromDom(post.Content),
                            ExternalTitle = post.ExternalLinkTitle,
                            ExternalUrl = post.ExternalLinkUrl?.AbsoluteUri,
                        };



                        if (post.PostId != default)
                        {
                            OnPostDiscovered(post.PostId.AsQualifiedPostId, prev, rootPostId.AsQualifiedPostId, subpostData, ctx: ctx, replyIsSemanticallyRepost: true);
                            prev = post.PostId.AsQualifiedPostId;
                        }

                    }
                }
                return (dateParsed, url);
            }
            else
            {
                var h = guid ?? url?.AbsoluteUri ?? date;
                postId = new NonQualifiedPluggablePostId(CreateSyntheticTid(dateParsed, h), XxHash64.Hash(MemoryMarshal.AsBytes<char>(h)));
            }




            var maxLength = hasFullContent ? 1500 : 500;
            if (bodyAsText?.Length >= maxLength)
                bodyAsText = string.Concat(bodyAsText.AsSpan(0, maxLength - 10), "…");

            var data = new BlueskyPostData()
            {
                PluggableLikeCountForScoring = pluggableLikeCountForScoring,
                PluggableLikeCount = pluggableLikeCountForScoring // TODO temporary for debugging
            };
            if (url == null) hasFullContent = true;

            var mediaThumb = GetAttribute((mediaGroup ?? item)?.Element(NsMedia + "thumbnail"), "url");

            if (!hasFullContent)
            {

                if (summaryText != null)
                {
                    var firstAppearedOn = Regex.Match(summaryText, "(first appeared on|appeared first on)", RegexOptions.IgnoreCase);
                    if (firstAppearedOn != null)
                    {
                        summaryText = summaryText.Substring(0, firstAppearedOn.Index);
                        var endOfPreviousSentence = summaryText.AsSpan().LastIndexOfAny('\n', '.', '…');
                        if (endOfPreviousSentence >= 1)
                        {
                            summaryText = summaryText.Substring(0, endOfPreviousSentence + 1).TrimEnd();
                        }
                    }
                }

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
                data.ExternalUrl = url!.AbsoluteUri;

                if (feedUrl.HasHostSuffix("reddit.com") && data.Text == null)
                {
                    data.Text = title;
                }
                else
                {
                    data.ExternalDescription = bodyAsText;
                    data.ExternalTitle = title;
                }


                if (mediaThumb != null)
                {
                    data.ExternalThumbCid = UrlToCid(mediaThumb);
                }
                else
                {

                    var img = bodyDom?.QuerySelector("img");
                    if (img != null)
                    {
                        data.ExternalThumbCid = UrlToCid(TryGetImageUrl(img, url)?.AbsoluteUri);
                    }
                }
            }
            else
            {
                data.Facets = bodyAsText != null ? bodyFacets?.Where(x => x.InlineImageUrl == null).ToArray() : null;
                if (title != null && bodyAsText != null)
                {
                    var prefix = title + "\n";
                    if (data.Facets != null)
                    {
                        var offset = Encoding.UTF8.GetByteCount(prefix);
                        foreach (var facet in data.Facets)
                        {
                            facet.Start += offset;
                        }
                    }
                    data.Facets = [new FacetData { Start = 0, Length = Encoding.UTF8.GetByteCount(title), Bold = true }, .. (data.Facets ?? [])];
                    data.Text = prefix + bodyAsText;
                }
                else
                {
                    data.Text = bodyAsText ?? title;
                }

                data.Media = bodyDom?.QuerySelectorAll("img").Select(x =>
                {
                    var imageUrl = TryGetImageUrl(x, url ?? feedUrl);
                    if (imageUrl != null && imageUrl.HasHostSuffix("preview.redd.it") && imageUrl.AbsolutePath.Split('.').Last() is "jpg" or "jpeg" or "png" or "webp" or "gif")
                    {
                        imageUrl = new Uri("https://i.redd.it/" + imageUrl.GetSegments()[0]);
                    };
                    return new BlueskyMediaData
                    {
                        Cid = UrlToCid(imageUrl?.AbsoluteUri)!,
                        AltText = feedUrl.HasHostSuffix("reddit.com") ? null : x.GetAttribute("title") ?? x.GetAttribute("alt"),
                    };
                }).Where(x => x.Cid != null).ToArray();
                if ((data.Media == null || data.Media.Length == 0) && data.ExternalUrl == null && mediaThumb != null)
                {
                    data.Media = [new BlueskyMediaData 
                    { 
                        Cid = UrlToCid(mediaThumb)!
                    }];
                }
            }
            OnPostDiscovered(new QualifiedPluggablePostId(did, postId), null, null, data, ctx: ctx);
            return (dateParsed, url);
        }

        private static IText? IsTumblrAttribution(IReadOnlyList<INode> content)
        {
            if (content.Count == 1 && content[0] is IText { } shortenedThread && Regex.IsMatch(shortenedThread.Text, @"^[\w\-]+\:\w"))
                return shortenedThread;
            return null;
        }

        private static BlueskyMediaData[] GetMediaFromDom(INode[] content)
        {
            return content.OfType<IElement>().SelectMany(x => x.TagName is "IMG" or "VIDEO" ? [x] : x.QuerySelectorAll("img, video").AsEnumerable()).Select(x =>
            {
                var isVideo = x.TagName == "VIDEO";
                if (isVideo) { }
                return new BlueskyMediaData
                {
                    AltText = x.GetAttribute("title") ?? x.GetAttribute("alt"),
                    Cid = isVideo ? UrlToCid(x.GetAttribute("poster"), x.GetAttribute("src") ?? x.QuerySelector("source")!.GetAttribute("src"))! : UrlToCid(x.GetAttribute("src"))!,
                    IsVideo = isVideo,
                };
            }).ToArray();
        }

        private static string NormalizeTumblrTextForComparison(string title)
        {
            return Regex.Replace(title, @"[\s….]", string.Empty);
        }

        internal static byte[]? UrlToCid(string? imageUrl, string? videoUrl = null)
        {
            if (imageUrl == null) return null;
            if (videoUrl != null) imageUrl = videoUrl + "\n" + imageUrl;
            return BlueskyRelationships.CompressBpe(imageUrl);
        }


        private static TumblrPost UnfoldTumblrPosts(IElement bodyDom, TumblrPostId postId, string? externalLinkTitle, Uri? externalLinkUrl)
        {
            var blockquote = bodyDom.Children.FirstOrDefault(x => x.TagName == "BLOCKQUOTE");
            IElement? attributionLink = null;
            var attribution = blockquote?.PreviousNonWhiteSpaceSibling() is IElement { TagName: "P", FirstChild: IElement { TagName: "A", ClassName: "tumblr_blog" } } attributionParagraph ? attributionParagraph : null;

            if (blockquote != null && attribution == null)
            {
                blockquote = null;
            }

            if (blockquote != null)
            {
                attributionLink = attribution?.FirstElementChild;
                var attributionUrl = attributionLink?.GetAttribute("href");
                var originalPostId = attributionUrl != null ? GetTumblrPostId(new Uri(attributionUrl), CreateSyntheticTid(postId.SuggestedTid.Date.AddSeconds(-10), attributionUrl)) : default;
                var nextSiblings = new List<INode>();

                // For image only posts, content can be before the attribution
                var prelude = new List<INode>();
                foreach (var prev in bodyDom.ChildNodes)
                {
                    if (prev == attribution || prev == blockquote) break;
                    prelude.Add(prev);
                }


                var preludeBelongsToQuotee = false;
                string? quotedExternalLinkTitle = null;
                Uri? quotedExternalLinkUrl = null;
                if (IsTumblrAttribution(prelude) == null)
                {
                    if (
                        prelude.Count == 2 &&
                        prelude[0] is Element { TagName: "A", ClassName: null, TextContent: var linkText } &&
                        prelude[1] is { NodeType: NodeType.Text, TextContent: var colon } &&
                        !string.IsNullOrWhiteSpace(linkText) &&
                        colon.Trim() == ":")
                    {
                        quotedExternalLinkUrl = StringUtils.TryParseUri(((Element)prelude[0]).GetAttribute("href"));
                        if (quotedExternalLinkUrl != null)
                            quotedExternalLinkTitle = linkText.Trim();
                        preludeBelongsToQuotee = true;
                    }
                    if (!preludeBelongsToQuotee)
                        nextSiblings.AddRange(prelude);
                }

                var sibling = blockquote.NextSibling;
                while (sibling != null)
                {
                    nextSiblings.Add(sibling);
                    sibling = sibling.NextSibling;
                }
                var quotedPost = new TumblrPost(postId, UnfoldTumblrPosts(blockquote, originalPostId, quotedExternalLinkTitle, quotedExternalLinkUrl), nextSiblings.ToArray(), externalLinkTitle, externalLinkUrl);
                return quotedPost;
            }
            else
            {
                return new TumblrPost(postId, null, bodyDom.ChildNodes.ToArray(), externalLinkTitle, externalLinkUrl);
            }
        }

        public record struct TumblrPostId(string BlogId, long PostId, Tid SuggestedTid)
        {
            public override string ToString()
            {
                return BlogId + "/" + PostId;
            }

            public QualifiedPluggablePostId AsQualifiedPostId
            {
                get
                {
                    if (BlogId == null) return default;
                    var host = BlogId.Contains('.') ? BlogId : BlogId + ".tumblr.com";
                    return new QualifiedPluggablePostId($"did:rss:{host}:rss", new NonQualifiedPluggablePostId(SuggestedTid, PostId));
                }
            }
        }

        private static TumblrPostId GetTumblrPostId(Uri url, Tid suggestedTid)
        {
            var customDomain = !url.HasHostSuffix("tumblr.com");
            var segments = url.GetSegments();

            string tumblrBlogId;
            long postId;
            if (segments[0] == "post")
            {
                tumblrBlogId = customDomain ? url.Host : url.Host.Split('.')[0];
                postId = long.Parse(segments[1]);
            }
            else if (segments[0] == "blog" && segments[1] == "view")
            {
                tumblrBlogId = customDomain ? url.Host : segments[2];
                postId = long.Parse(segments[3]);
            }
            else
            {
                if (customDomain) AssertionLiteException.Throw("GetTumblrPostId: custom domain but URL is " + url);
                tumblrBlogId = segments[0];
                postId = long.Parse(segments[1]);
            }
            if (postId == 0)
            {
                // Happens for deleted blogs? /post/000
                postId = suggestedTid.TidValue;
            }
            return new(tumblrBlogId, postId, suggestedTid);
        }

        public record TumblrPost(TumblrPostId PostId, TumblrPost? QuotedPost, INode[] Content, string? ExternalLinkTitle, Uri? ExternalLinkUrl)
        {
            public override string ToString()
            {
                return PostId + ": " + (QuotedPost != null ? "(quoting post) " : null) + string.Join(" ", Content.Select(x => x.TextContent));
            }
        }

        private static Uri? TryGetImageUrl(IElement img, Uri baseUrl)
        {
            try
            {
                var size = img.GetAttribute("height") ?? img.GetAttribute("width");
                if (size == null || int.Parse(size) > 60)
                {
                    return new Uri(baseUrl, img.GetAttribute("src"));
                }
            }
            catch
            {
            }
            return null;
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
            var url = DidToUrl(did);
            if (url.HasHostSuffix("youtube.com")) return "/assets/default-youtube-avatar.png";
            if (url.HasHostSuffix("reddit.com")) return "/assets/default-reddit-avatar.svg";
            if (url.HasHostSuffix("tumblr.com")) return "/assets/default-tumblr-avatar.svg";
            if (url.HasHostSuffix("github.com")) return "/assets/default-github-avatar.svg";
            return "/assets/default-rss-avatar.svg";
        }

        private static Uri? GetAlternateLink(XElement? item)
        {
            var links = GetChildren(item, "link");
            var link = links.FirstOrDefault(x => GetAttribute(x, "rel") == "alternate") ??
                links.FirstOrDefault(x => GetAttribute(x, "rel") == null);
            var url = Normalize(GetAttribute(link, "href")) ?? Normalize(link);
            return url != null ? new Uri(url) : null;
        }

        private static string? Normalize(XElement? element) => Normalize(element?.Value);
        private static string? Normalize(string? value)
        {
            var v = value?.Trim();
            return !string.IsNullOrEmpty(v) ? v : null;
        }

        private readonly static XNamespace NsContent = "http://purl.org/rss/1.0/modules/content/";
        private readonly static XNamespace NsAtom = "http://www.w3.org/2005/Atom";
        private readonly static XNamespace NsRdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
        private readonly static XNamespace NsRss = "http://purl.org/rss/1.0/";
        private readonly static XNamespace NsDc = "http://purl.org/dc/elements/1.1/";
        private readonly static XNamespace NsMedia = "http://search.yahoo.com/mrss/";
        private static IEnumerable<XName> GetNames(string name) => [name, NsAtom + name, NsRss + name, NsRdf + name, NsDc + name];
        protected internal override void EnsureValidDid(string did)
        {
            DidToUrl(did);
        }


        public override string? TryGetOriginalProfileUrl(BlueskyProfile profile)
        {
            if (profile.Did.StartsWith("did:rss:www.reddit.com:", StringComparison.Ordinal))
                return DidToUrl(profile.Did).AbsoluteUri;
            return profile.BasicData?.CustomFields?.FirstOrDefault(x => x.Name == "web")?.Value;
        }

        public static Uri DidToUrl(string did)
        {
            var parts = did.Substring(DidPrefix.Length).Split(':');
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
                throw new ArgumentException("Only HTTP and HTTPS schemes are supported for RSS.");
            if (!url.IsDefaultPort || !string.IsNullOrEmpty(url.UserInfo))
                throw new NotSupportedException("RSS feeds on custom ports or user login info are not supported.");
            return
               DidPrefix + (url.Scheme == Uri.UriSchemeHttp ? "http:" : null) + Uri.EscapeDataString(url.AbsoluteUri.AsSpan(url.Scheme.Length + 3)).Replace("_", "%5F").Replace("%2F", ":").Replace("%", "_");
        }


        public async override Task<BlobResult> GetBlobAsync(string did, byte[] cid, ThumbnailSize preferredSize, CancellationToken ct)
        {
            var url = BlueskyRelationships.DecompressBpe(cid)!;
            //bool isFavicon = false;
            if (url.StartsWith('!')) { url = url.Substring(1); /*isFavicon = true;*/ }

            var parts = url.Split("\n");
            if (parts.Length != 1)
            {
                if (preferredSize == ThumbnailSize.video_thumbnail) url = parts[1];
                else url = parts[0];
            }

            var result = await BlueskyEnrichedApis.GetBlobFromUrl(new Uri(url), preferredSize: preferredSize, ct: ct);
            //result.IsFavIcon = isFavicon;
            return result;
        }

        public override string? GetDisplayNameFromDid(string did)
        {
            var url = DidToUrl(did);
            if (url.HasHostSuffix("tumblr.com"))
            {
                return url.Host.Split('.')[0];
            }
            if (url.HasHostSuffix("reddit.com"))
            {
                var path = url.GetSegments();
                if (path[0] == "user") return "/u/" + path[1];
                else return path[1];
            }
            var host = url.GetDomainTrimWww();
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
            if (!postId.HasExternalIdentifier) return null;
            var feedUrl = DidToUrl(postId.Did);
            if (feedUrl.HasHostSuffix("reddit.com"))
            {
                var segments = feedUrl.GetSegments();
                if(postId.PostId.String!.StartsWith("/r/", StringComparison.Ordinal))
                    return $"https://www.reddit.com" + postId.PostId.String + "/";
                return $"https://www.reddit.com/{segments[0]}/{segments[1]}/comments/{postId.PostId.String}/";
            }
            else if (feedUrl.HasHostSuffix("tumblr.com"))
            {
                return $"https://www.tumblr.com/{feedUrl.Host.Split('.')[0]}/{postId.PostId.Int64}";
            }
            return post.Data?.ExternalUrl;
        }

        public override TimeSpan GetProfilePageMaxPostAge()
        {
            return TimeSpan.FromDays(365 * 50);
        }

        private readonly TaskDictionary<string, RequestContext, RssRefreshInfo> RefreshFeed;

        public async Task<RssRefreshInfo> MaybeRefreshFeedAsync(string did, RequestContext ctx)
        {
            var refreshData = Apis.WithRelationshipsLockForDid(did, (plc, rels) => rels.GetRssRefreshInfo(plc), ctx);

            var now = DateTime.UtcNow;
            if (refreshData == null || (now - refreshData.LastRefreshAttempt).TotalHours > 6)
            {
                return await RefreshFeed.GetValueAsync(did, ctx);
            }
            return refreshData;
        }

        public static async Task<Uri?> TryGetFeedUrlFromPageAsync(string responseText, Uri url)
        {
            var dom = StringUtils.ParseHtml(responseText);

            if (url.HasHostSuffix("youtube.com"))
            {
                var channelId = dom.QuerySelector("meta[itemprop='identifier']")?.GetAttribute("content");
                if (channelId != null)
                    return new Uri("https://www.youtube.com/feeds/videos.xml?channel_id=" + Uri.EscapeDataString(channelId));
            }


            var feedUrl = dom.QuerySelectorAll("link[type='application/atom+xml'],link[type='application/rss+xml']")
                .Select(x => Uri.TryCreate(url, x.GetAttribute("href"), out var u) ? u : null)
                .WhereNonNull()
                .MinBy(x => x!.AbsoluteUri.Length);
            if (feedUrl == null)
            {
                var links = dom.QuerySelectorAll("a")
                    .Select(x => Uri.TryCreate(url, x.GetAttribute("href"), out var u) ? u : null)
                    .WhereNonNull()
                    .Where(x => x.Host == url.Host)
                    .Where(x => Regex.IsMatch(x.PathAndQuery, @"\b(?:feed|rss|\.xml|atom)\b"))
                    .DistinctBy(x => x.GetLeftPart(UriPartial.Query))
                    .OrderBy(x => x.AbsoluteUri.Length);
                foreach (var link in links.Take(2))
                {
                    try
                    {
                        var xml = await BlueskyEnrichedApis.DefaultHttpClient.GetStringAsync(link);
                        if (IsFeedXml(xml))
                            return link;
                    }
                    catch
                    {
                    }
                }

            }
            return feedUrl;
        }

        public static bool IsFeedXml(string responseText)
        {
            var initialText = responseText.Substring(0, Math.Min(responseText.Length, 1024));
            return Regex.IsMatch(initialText, @"<(?:rss|feed|rdf)\b");
        }

        public async override Task<string?> TryGetDidOrLocalPathFromUrlAsync(Uri url, bool preferDid)
        {
            if (url.Host.EndsWith(".tumblr.com", StringComparison.Ordinal) && url.Host != "www.tumblr.com")
            {
                return $"did:rss:{url.Host}:rss";
            }
            if (url.HasHostSuffix("tumblr.com"))
            {
                var segment = url.GetSegments().FirstOrDefault();
                if (segment != null)
                    return $"did:rss:{segment}.tumblr.com:rss";
            }
            if (url.HasHostSuffix("youtube.com"))
            {
                var segments = url.GetSegments();
                var first = segments.FirstOrDefault();
                if (first != null)
                {
                    if (first is "user" or "c" || first.StartsWith('@'))
                    {
                        using var request = new HttpRequestMessage(HttpMethod.Get, url.GetLeftPart(UriPartial.Path));
                        // If we DON'T submit any User-Agent, we don't get the cookie consent interstitial.
                        using var ytresponse = await BlueskyEnrichedApis.DefaultHttpClientNoDefaultHeaders.SendAsync(request);
                        ytresponse.EnsureSuccessStatusCode();
                        if (ytresponse.RequestMessage?.RequestUri?.Host.StartsWith("consent.", StringComparison.Ordinal) == true)
                            throw AssertionLiteException.Throw("Could not resolve channel ID (cookie consent interstitial couldn't be bypassed)");
                        var dom = StringUtils.ParseHtml(await ytresponse.Content.ReadAsStringAsync());
                        var canonical = dom.QuerySelector("link[rel='canonical']")?.GetAttribute("href");
                        if (canonical == null) throw AssertionLiteException.Throw("Could not resolve channel ID.");
                        var canonicalUrlSegments = new Uri(canonical).GetSegments();
                        if (canonicalUrlSegments.FirstOrDefault() != "channel") throw AssertionLiteException.Throw("Could not resolve channel ID (the returned rel=canonical wasn't a channel ID).");
                        var channelId = canonicalUrlSegments[1];
                        return UrlToDid(new Uri("https://www.youtube.com/feeds/videos.xml?channel_id=" + channelId));
                    }
                    else if (first == "channel")
                    {
                        var channelId = segments[1];
                        return UrlToDid(new Uri("https://www.youtube.com/feeds/videos.xml?channel_id=" + channelId));
                    }

                }
            }

            if (url.HasHostSuffix("reddit.com"))
            {
                var segments = url.GetSegments();
                if (segments.Length != 0)
                {
                    var sortSegment = segments.ElementAtOrDefault(2);
                    if (sortSegment is "comments" or "wiki") throw new Exception("RSS feeds to reddit comments or wikis are not supported.");
                    var q = url.GetQueryDictionary();

                    if (segments[0] == "r") return UrlToDid(new Uri($"https://www.reddit.com/r/{segments[1].ToLowerInvariant()}"));
                    if (segments[0] is "u" or "user") return UrlToDid(new Uri($"https://www.reddit.com/user/{segments[1].ToLowerInvariant()}"));
                }
            }

            var did = UrlToDid(url);
            var (virtualFeedRedirect, virtualFeed) = TryGetVirtualRssDelegate(url, did);
            if (virtualFeedRedirect != null) return "/" + virtualFeedRedirect;
            if (virtualFeed != null) return did;


            using var response = await BlueskyEnrichedApis.DefaultHttpClientNoAutoRedirect.GetAsync(url);

            if (response.GetRedirectLocationUrl() is { } newUrl)
            {
                return "/" + newUrl.AbsoluteUri;
            }

            response.EnsureSuccessStatusCode();
            var responseText = (await response.Content.ReadAsStringAsync()).Trim();
            if (IsFeedXml(responseText))
            {
                return did;
            }

            var rssFeed = await TryGetFeedUrlFromPageAsync(responseText, url);
            if (rssFeed != null)
            {
                return UrlToDid(rssFeed);
            }

            return null;
        }

        public override bool ShouldIncludeFullReplyChain(BlueskyPost post) => DidToUrl(post.Did).HasHostSuffix("tumblr.com");

        public override bool ShouldShowRepliesTab(BlueskyProfile profile)
        {
            return DidToUrl(profile.Did).HasHostSuffix("tumblr.com");
        }

        public override bool SupportsProfileMetadataLookup(string did)
        {
            return true;
        }

        public override async Task TryFetchProfileMetadataAsync(string did, RequestContext ctx)
        {
            await MaybeRefreshFeedAsync(did, ctx);
        }

        public override bool RequiresLateOpenGraphData(BlueskyPost post)
        {
            var data = post.Data;
            if (data == null) return false;
            if (data.ExternalUrl == null) return false;

            if (post.Did.StartsWith("did:rss:www.reddit.com:", StringComparison.Ordinal))
            {
                // Reddit RSS doesn't provide description. However, keep RSS thumbnail as a fallback.
                return true;
            }
            if (new Uri(data.ExternalUrl).HasHostSuffix("tumblr.com"))
            {
                if (DidToUrl(post.Did).HasHostSuffix("tumblr.com"))
                {
                    return true;
                }
            }
            return base.RequiresLateOpenGraphData(post);
        }

        public override bool ShouldUseCompactMediaThumbnails(BlueskyPost post)
        {
            if (post.Data?.Media != null && post.Author.Did.StartsWith("did:rss:www.reddit.com:", StringComparison.Ordinal))
            {
                return post.Data.Media.Any(x =>
                {
                    var mediaUrl = new Uri(EfficientTextCompressor.Decompress(x.Cid!)!.Split('\n')[0]);
                    if (mediaUrl.HasHostSuffix("thumbs.redditmedia.com")) return true;
                    return false;
                });
            }
            return false;
        }

        public override string? GetDisplayHandle(BlueskyProfile profile)
        {
            var url = DidToUrl(profile.Did);
            if (url.HasHostSuffix("reddit.com"))
            {
                var segments = url.GetSegments();

                if (segments.Length >= 2)
                {
                    if (segments[0] == "r" && segments[1].Equals(profile.DisplayName, StringComparison.OrdinalIgnoreCase))
                        return "/r/" + profile.DisplayName;
                    if (segments[0] == "user") return "/u/" + segments[1];
                }
            }
            if (url.HasHostSuffix("tumblr.com"))
            {
                return url.Host;
            }
            if (url.HasHostSuffix("github.com"))
            {
                var segments = url.GetSegments().Take(2).ToArray();
                return "github.com/" + string.Join("/", segments);
            }
            if (url.HasHostSuffix("youtube.com"))
            {
                return "youtube.com/channel/" + url.GetQueryDictionary()["channel_id"];
            }
            if (url.HasHostSuffix("t.me"))
            {
                return "t.me/" + url.GetSegments().FirstOrDefault();
            }
            return StringUtils.GetDisplayHost(url);
        }

        public static (Uri? RedirectTo, VirtualRssDelegate? Delegate) TryGetVirtualRssDelegate(Uri feedUrl, string did)
        {
            // RSS works, while .json is often blocked
            if (false && feedUrl.HasHostSuffix("reddit.com") && feedUrl.AbsolutePath.StartsWith("/r/", StringComparison.Ordinal))
            {
                return (null, RedditApi.GetRedditVirtualRss(feedUrl, did));
            }

            if (feedUrl.HasHostSuffix("t.me"))
            {
                var segments = feedUrl.GetSegments();
                var channel = segments.FirstOrDefault() == "s" ? segments.ElementAtOrDefault(1) : segments.FirstOrDefault();
                if (channel != null)
                {
                    var normalized = "https://t.me/" + channel;
                    if (normalized != feedUrl.AbsoluteUri) return (new(normalized), null);
                    return (null, Telegram.GetFeedAsync(did, channel));
                }
            }

            if (feedUrl.HasHostSuffix("github.com"))
            {
                var segments = feedUrl.GetSegments();

                if (segments.Length == 2)
                    segments = segments.Append("commits").ToArray();
                if (segments.Length >= 3)
                {
                    var r = ("https://github.com/" + string.Join("/", segments.Take(3))).ToLowerInvariant();

                    if(r == feedUrl.AbsoluteUri)
                    {
                        if (segments[2] == "commits")
                        {
                            return (null, () => GitHub.GetCommitsAsync(did, segments[0], segments[1]));
                        }
                        if (segments[2] == "releases")
                        {
                            return (null, () => GitHub.GetReleasesAsync(did, segments[0], segments[1]));
                        }
                        if (segments[2] == "issues")
                        {
                            return (null, () => GitHub.GetIssuesAsync(did, segments[0], segments[1], pulls: false));
                        }
                        if (segments[2] == "pulls")
                        {
                            return (null, () => GitHub.GetIssuesAsync(did, segments[0], segments[1], pulls: true));
                        }
                        throw new UnexpectedFirehoseDataException("Unrecognized GitHub URL. Only /commits, /releases, /issues and /pulls are supported.");
                    }
                    
                    return (new Uri(r), null);

                }

            }
            return default;
        }

        public override bool ProvidesLikeCount(string did) => did.StartsWith("did:rss:www.reddit.com:", StringComparison.Ordinal) || did.StartsWith("did:rss:github.com:", StringComparison.Ordinal);


        private static int EstimateLikesFromRank(int rank, double alpha)
        {
            rank++; // 0-based to 1-based
            return (int)(500 * (1.0 / Math.Pow(rank, alpha))); // power law
        }
    }

    public delegate Task<VirtualRssResult> VirtualRssDelegate();
    public record VirtualRssPost(QualifiedPluggablePostId PostId, BlueskyPostData Data)
    {
        public DateTime Date => PostId.Tid.Date;
    }
    public record VirtualRssResult(BlueskyProfileBasicInfo? Profile, VirtualRssPost[] Posts);




}

