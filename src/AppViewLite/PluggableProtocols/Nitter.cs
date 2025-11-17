using AngleSharp.Dom;
using AngleSharp.Html.Dom;
using AppViewLite.Models;
using AppViewLite.Numerics;
using System;
using System.Globalization;
using System.Linq;
using System.Net.Http;

namespace AppViewLite.PluggableProtocols.Rss
{
    internal static class Nitter
    {
        internal static VirtualRssDelegate GetTwitterVirtualRss(string username)
        {
            var nitterInstance = AppViewLiteConfiguration.GetString(AppViewLiteParameter.APPVIEWLITE_NITTER_INSTANCE);
            if (nitterInstance == null) throw new NotSupportedException("A Nitter instance has not been configured on this AppViewLite instance.");
            if (!nitterInstance.Contains("://"))
                nitterInstance = "https://" + nitterInstance;
            return async () =>
            {
                var profileUrl = new Uri(nitterInstance.TrimEnd('/') + "/" + username);

                string html;
                bool isSuccessfulStatusCode;
                int responseStatusCode;
                using var request = new HttpRequestMessage(HttpMethod.Get, profileUrl);
                request.Headers.Add("Cookie", "hlsPlayback=on");

#if true
                using var response = await BlueskyEnrichedApis.DefaultHttpClientForRss.SendAsync(request);
                responseStatusCode = (int)response.StatusCode;
                isSuccessfulStatusCode = response.IsSuccessStatusCode;
                html = await response.Content.ReadAsStringAsync();

#else
                // dev only
                var cacheFile = "C:\\temp\\nitter2-" + username + ".html";
                if (!System.IO.File.Exists(cacheFile))
                {

                    using var response = await BlueskyEnrichedApis.DefaultHttpClientForRss.SendAsync(request);
                    html = await response.Content.ReadAsStringAsync();
                    System.IO.File.WriteAllText(cacheFile, html);
                }
                else 
                {
                    html = System.IO.File.ReadAllText(cacheFile);
                }
                isSuccessfulStatusCode = true;
                responseStatusCode = 200;
#endif

                var profileDom = StringUtils.ParseHtml(html);
                var pageTitle = profileDom.QuerySelector("title")?.TextContent;

                var profileDisplayNameElement = profileDom.QuerySelector(".profile-card-fullname");
                if (StringUtils.IsCaptchaOrProofOfWorkPageTitle(pageTitle) || profileDisplayNameElement == null || !isSuccessfulStatusCode) throw new UnexpectedFirehoseDataException($"Nitter instance returned HTML that could not be parsed (with HTTP {responseStatusCode}), possibly due to scraping protection. If this is an instance you control, add an exception for AppViewLite. Otherwise, consider running your own private Nitter instance.");

                var profileText = StringUtils.HtmlToFacets(profileDom.QuerySelector(".profile-bio"), x => ElementToFacet(x, profileUrl), pre: true);
                var profile = new BlueskyProfileBasicInfo()
                {

                    DisplayName = profileDisplayNameElement.TextContent?.Trim(),
                    Description = profileText.Text,
                    DescriptionFacets = profileText.Facets,
                    AvatarCidBytes = ImageToCid(profileDom.QuerySelector(".profile-card-avatar img"), profileUrl),
                    BannerCidBytes = ImageToCid(profileDom.QuerySelector(".profile-banner img"), profileUrl),
                    PluggableProtocolFollowerCount = ParseInt64(profileDom.QuerySelector(".profile-statlist .followers .profile-stat-num")),
                    PluggableProtocolFollowingCount = ParseInt64(profileDom.QuerySelector(".profile-statlist .following .profile-stat-num")),
                    ExternalWebsite = profileDom.QuerySelector(".profile-website a")?.TryGetHref(profileUrl)?.AbsoluteUri,
                    Location = profileDom.QuerySelector(".profile-location")?.Text(),
                };
                var posts = profileDom.QuerySelectorAll(".timeline-item").Select(x =>
                {
                    var quote = x.QuerySelector(".quote");
                    VirtualRssPost? quoted = null;
                    if (quote != null)
                    {
                        quote.Remove();
                        quoted = ParseNitterTweet(quote, profileUrl, null);
                    }
                    return ParseNitterTweet(x, profileUrl, quoted);
                }).ToArray();
                return new VirtualRssResult(profile, posts);
            };
        }

        private static VirtualRssPost ParseNitterTweet(IElement x, Uri profileUrl, VirtualRssPost? quotedPost)
        {
            var tweetSegments = x.QuerySelector(".tweet-link, .quote-link")!.GetHref(profileUrl).GetSegments();
            if (tweetSegments[1] != "status") throw new UnexpectedFirehoseDataException("Unparseable HTML");
            var originalPoster = tweetSegments[0].ToLowerInvariant();
            var tweetId = long.Parse(tweetSegments[2]);
            var date = TryGetDateFromTweetId(tweetId);
            if (date == default)
            {
                var dateToParse = x.QuerySelector(".tweet-date a")!.GetAttribute("title")!.Replace('·', ' ').Replace("UTC", " +00:00");
                date = DateTime.Parse(dateToParse, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal);
            }
            var did = RssProtocol.UrlToDid(new Uri("https://x.com/" + tweetSegments[0].ToLowerInvariant()));
            var tid = Tid.FromDateTime(date);
            var tweetStats = x.QuerySelector(".tweet-stats");
            var postText = StringUtils.HtmlToFacets(x.QuerySelector(".tweet-content, .quote-text"), x => ElementToFacet(x, profileUrl), pre: true);

            var postData = new BlueskyPostData
            {
                PluggableReplyCount = ParseInt32Saturated(tweetStats?.QuerySelector(".icon-comment")?.NextSibling),
                PluggableLikeCount = ParseInt32Saturated(tweetStats?.QuerySelector(".icon-heart")?.NextSibling),
                PluggableRepostCount = ParseInt32Saturated(tweetStats?.QuerySelector(".icon-retweet")?.NextSibling),
                PluggableQuoteCount = ParseInt32Saturated(tweetStats?.QuerySelector(".icon-quote")?.NextSibling),
                Text = postText.Text,
                Facets = postText.Facets,
                ExternalUrl = x.QuerySelector(".card-container")?.TryGetHref(profileUrl)?.AbsoluteUri,
                ExternalThumbCid = ImageToCid(x.QuerySelector(".card-image img"), profileUrl),
                ExternalTitle = x.QuerySelector(".card-title")?.TextContent,
                ExternalDescription = x.QuerySelector(".card-description")?.TextContent,
                Media = x.QuerySelectorAll(".attachment").Select(x =>
                {
                    if (x.ClassList.Contains("image"))
                    {
                        return new BlueskyMediaData
                        {
                            AltText = x.QuerySelector("img")?.GetAttribute("alt"),
                            Cid = ImageToCid(x.QuerySelector(".still-image")!.GetHref(profileUrl))!,
                        };
                    }
                    else if (x.ClassList.Contains("video-container"))
                    {
                        var img = x.QuerySelector("img");
                        if (img != null)
                        {
                            // hlsPlayback=on didn't work.
                            var poster = ToOriginalImageUrl(StringUtils.GetSrcSetLargestImageUrl(img, profileUrl));

                            return new BlueskyMediaData
                            {
                                Cid = RssProtocol.UrlToCid(poster)!,
                                IsVideo = true
                            };
                        }
                        else
                        {
                            var video = x.QuerySelector("video");
                            var poster = ToOriginalImageUrl(video!.TryGetUrlAttribute("poster", profileUrl));
                            var videoUrl = ToOriginalImageUrl(video!.TryGetUrlAttribute("data-url", profileUrl));

                            return new BlueskyMediaData
                            {
                                Cid = RssProtocol.UrlToCid(poster, videoUrl)!,
                                IsVideo = true
                            };
                        }
                    }
                    else
                    {
                        return null;
                    }
                }).WhereNonNull().ToArray(),
            };
            return new VirtualRssPost(new QualifiedPluggablePostId(GetDidForUsername(originalPoster), new NonQualifiedPluggablePostId(tid, tweetId)), postData, QuotedPost: quotedPost);
        }

        public static string GetDidForUsername(string username)
        {
            if (string.IsNullOrEmpty(username)) throw new ArgumentException();
            return RssProtocol.UrlToDid(new Uri("https://x.com/" + username.ToLowerInvariant()));
        }

        private static FacetData? ElementToFacet(IElement element, Uri pageUrl)
        {
            if (element is IHtmlAnchorElement a)
            {
                var url = a.TryGetHref(pageUrl);
                if (url?.Host == pageUrl.Host)
                {
                    if (url.PathAndQuery.StartsWith("/search?q=%23", StringComparison.OrdinalIgnoreCase))
                        return null;
                    else
                    {
                        var segments = url.GetSegments();
                        if (segments.Length == 1)
                        {
                            return new FacetData
                            {
                                Did = GetDidForUsername(segments[0]),
                            };
                        }
                        else
                        {
                            return new FacetData
                            {
                                Link = "https://x.com" + url.PathAndQuery + url.Fragment
                            };
                        }

                    }
                }
            }
            return StringUtils.DefaultElementToFacet(element, pageUrl);
        }

        public static DateTime TryGetDateFromTweetId(long tweetId)
        {
            // https://github.com/oduwsdl/tweetedat/blob/master/script/TimestampEstimator.py#L175

            if (tweetId < 29700859247) return default;
            return DateTime.UnixEpoch.AddMilliseconds((tweetId >> 22) + 1288834974657);
        }

        private static long? ParseInt64(INode? element)
        {
            if (element == null) return null;
            var text = element?.TextContent.Trim();
            if (string.IsNullOrEmpty(text)) return null;
            return long.Parse(text, NumberStyles.AllowThousands, CultureInfo.InvariantCulture);
        }

        private static int? ParseInt32Saturated(INode? element)
        {
            var r = ParseInt64(element);
            if (r == null) return null;
            return (int)Math.Min(int.MaxValue, r.Value);
        }

        private static byte[]? ImageToCid(IElement? img, Uri pageUrl)
        {
            var src = img != null ? StringUtils.GetSrcSetLargestImageUrl(img, pageUrl) : null;
            return ImageToCid(src);
        }
        private static byte[]? ImageToCid(Uri? src)
        {
            return RssProtocol.UrlToCid(ToOriginalImageUrl(src));
        }

        private static string? ToOriginalImageUrl(Uri? src)
        {
            if (src == null) return null;
            if (src.AbsolutePath.StartsWith("/pic/", StringComparison.Ordinal))
            {
                var url = Uri.UnescapeDataString(src.AbsolutePath.AsSpan(5));
                if (url.StartsWith("card_img/", StringComparison.Ordinal))
                {
                    return "https://pbs.twimg.com/" + url;
                }
                else if (url.StartsWith("orig/", StringComparison.Ordinal))
                {
                    return string.Concat("https://pbs.twimg.com/", url.AsSpan(5));
                }
                else if (url.StartsWith("amplify_video_thumb/", StringComparison.Ordinal))
                {
                    return string.Concat("https://pbs.twimg.com/" + url);
                }
                else if (!url.StartsWith("http://", StringComparison.Ordinal) && !url.StartsWith("https://", StringComparison.Ordinal))
                {
                    return "https://" + url;
                }
                else
                {
                    return new Uri(url).AbsoluteUri;
                }
            }
            else if (src.AbsolutePath.StartsWith("/video/", StringComparison.Ordinal))
            {
                var segments = src.GetSegments();
                return new Uri(Uri.UnescapeDataString(segments[2])).AbsoluteUri;
            }
            else
            {
                return src.AbsoluteUri;
            }
        }
    }
}
