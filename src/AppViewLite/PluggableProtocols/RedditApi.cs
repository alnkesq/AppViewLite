using AppViewLite.Models;
using AppViewLite.PluggableProtocols.Reddit;
using AppViewLite.PluggableProtocols.Rss;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http.Json;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.PluggableProtocols.Reddit
{
    internal class RedditApi
    {
        public static VirtualRssDelegate? GetRedditVirtualRss(Uri feedUrl, string did)
        {
            return async () =>
            {
                var baseUrl = new Uri("https://www.reddit.com/");
                var subreddit = feedUrl.GetSegments()[1];
                var response = (await BlueskyEnrichedApis.DefaultHttpClientForRss.GetFromJsonAsync<RedditApiResponse>($"https://www.reddit.com/r/{subreddit}/top/.json?sort=top&t=week"))!;
                var postsJson = response.data.children.Select(x => x.data).ToArray();
                var profile = new BlueskyProfileBasicInfo
                {
                    DisplayName = postsJson.Select(x => x.subreddit).FirstOrDefault(x => x.Equals(subreddit, StringComparison.OrdinalIgnoreCase)) ?? subreddit,
                };

                var posts = postsJson.Select(x =>
                {
                    var date = DateTime.UnixEpoch.AddSeconds(x.created_utc);
                    var tid = RssProtocol.CreateSyntheticTid(date, x.id);
                    var title = x.title;
                    var html = StringUtils.HtmlDecode(x.selftext_html);
                    if (!string.IsNullOrEmpty(html) && !string.IsNullOrEmpty(title))
                    {
                        html = "<b>" + title + "</b>" + (title.EndsWith(':') || title.EndsWith('!') || title.EndsWith('?') || title.EndsWith('.') ? " " : ": ") + html;
                    }
                    else if (!string.IsNullOrEmpty(title))
                    {
                        html = title;
                    }

                    var (text, facets) = StringUtils.ParseHtmlToText(html, out var body, x => StringUtils.DefaultElementToFacet(x, baseUrl));
                    var externalLinkFromBody = x.is_self ? body?.QuerySelectorAll("a").Select(x => StringUtils.TryParseUri(baseUrl, x.GetAttribute("href"))).FirstOrDefault(x => x != null && !x.HasHostSuffix("reddit.com") && !x.HasHostSuffix("redd.it")) : null;

                    Uri? externalLink = null;

                    if (!x.is_self || externalLinkFromBody != null)
                    {
                        var url = externalLinkFromBody ?? new Uri(x.url);
                        if (!url.HasHostSuffix("redd.it") && !(url.HasHostSuffix("reddit.com") && url.AbsolutePath.StartsWith("/gallery/", StringComparison.Ordinal)))
                        {
                            externalLink = url;
                        }
                    }

                    BlueskyMediaData[] media;


                    if (x.gallery_data != null && x.gallery_data.items.Length != 0)
                    {
                        media = x.gallery_data.items.Select(y =>
                        {
                            var m = x.media_metadata[y.media_id];
                            var altText = !string.IsNullOrEmpty(y.caption) ? y.caption : null;
                            if (m.e == "Image")
                            {
                                return new BlueskyMediaData
                                {
                                    AltText = altText,
                                    Cid = RssProtocol.UrlToCid(StringUtils.HtmlDecode(m.s.u))!
                                };
                            }
                            else if (m.e == "AnimatedImage")
                            {
                                var thumbUrl = StringUtils.HtmlDecode(m.p.MaxBy(x => x.x * x.y)!.u);
                                return new BlueskyMediaData
                                {
                                    AltText = altText,
                                    Cid = RssProtocol.UrlToCid(thumbUrl, StringUtils.HtmlDecode(m.s.mp4))!
                                };
                            }
                            else
                            {
                                return null;
                            }

                        }).WhereNonNull().ToArray();
                    }
                    else
                    {
                        media = x.preview?.images?.Select(y =>
                        {
                            var videoUrl = StringUtils.HtmlDecode(y.variants?.mp4?.source?.url); // ?? x.preview.reddit_video_preview?.hls_url /*mirrored video from external domain*/;
                            var imageUrl = StringUtils.HtmlDecode(y.source?.url);
                            return new BlueskyMediaData()
                            {
                                Cid = RssProtocol.UrlToCid(imageUrl, videoUrl)!,
                                IsVideo = videoUrl != null,
                            };
                        })
                        .Where(x => x.Cid != null)
                        .ToArray() ?? [];
                    }


                    var postData = new BlueskyPostData
                    {
                        Text = text,
                        Facets = facets,
                        Media = media,
                        PluggableLikeCount = x.ups,
                        PluggableReplyCount = x.num_comments,
                    };

                    if (externalLink != null)
                    {
                        postData.ExternalUrl = externalLink.AbsoluteUri;
                        postData.ExternalThumbCid = media?.FirstOrDefault()?.Cid ?? (x.thumbnail != "default" && !string.IsNullOrEmpty(x.thumbnail) ? RssProtocol.UrlToCid(x.thumbnail) : null);
                        postData.Media = null;
                    }

                    return new VirtualRssPost(new QualifiedPluggablePostId(did, tid, x.id), postData);
                }).ToArray();
                return new VirtualRssResult(profile, posts);
            };
        }

    }
}

