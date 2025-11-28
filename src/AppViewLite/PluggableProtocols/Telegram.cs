using AppViewLite.Models;
using System;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace AppViewLite.PluggableProtocols.Rss
{
    internal static class Telegram
    {
        internal static VirtualRssDelegate GetFeedAsync(string did, string channel)
        {

            return async () =>
            {
                var feedUrl = new Uri("https://t.me/s/" + channel);
                var html = await BlueskyEnrichedApis.DefaultHttpClientForRss.GetStringAsync(feedUrl);
                var page = StringUtils.ParseHtml(html);

                var profile = new BlueskyProfileBasicInfo()
                {
                    DisplayName = page.QuerySelector("meta[property='twitter:title']")?.GetAttribute("content"),
                    Description = page.QuerySelector("meta[property='og:description']")?.GetAttribute("content"),
                    AvatarCidBytes = RssProtocol.UrlToCid(page.QuerySelector("meta[property='twitter:image']")?.GetAttribute("content"))
                };
                var posts = page.QuerySelectorAll(".tgme_widget_message_wrap").Select(x => 
                {
                    var time = x.QuerySelectorAll("time");
                    var date = DateTime.Parse(time[time.Count - 1].GetAttribute("datetime")!, CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.AssumeUniversal | System.Globalization.DateTimeStyles.AdjustToUniversal);
                    var postId = x.Children.First(x => x.ClassList.Contains("tgme_widget_message")).GetAttribute("data-post");
                    var body = x.QuerySelector(".tgme_widget_message_text");
                    body?.Children.LastOrDefault(x => x.TagName == "A" && StringUtils.TryParseUri(feedUrl, x.GetAttribute("href"))?.HasHostSuffix("t.me") == true && x.GetAttribute("rel") != "noopener")?.Remove();
                    var (text, facets) = body != null ? StringUtils.HtmlToFacets(body, x => StringUtils.DefaultElementToFacet(x, feedUrl)) : default;
                    var data = new BlueskyPostData
                    {
                        Text = text,
                        Facets = facets,
                    };
                    data.Media = x.QuerySelectorAll(".tgme_widget_message_photo_wrap").Select(x => 
                    {
                        var imageUrl = Regex.Match(x.GetAttribute("style")!, @"url\((.*?)\)").Groups[1].Value.Replace("'", null).Trim();
                        return new BlueskyMediaData
                        {
                            Cid = RssProtocol.UrlToCid(imageUrl)!
                        };
                    }).Concat(x.QuerySelectorAll(".tgme_widget_message_video_player").Select(x => 
                    {
                        var thumb = Regex.Matches(x.QuerySelector(".tgme_widget_message_video_thumb")!.GetAttribute("style")!, @"url\('(.+?)'").First().Groups[1].Value;
                        return new BlueskyMediaData
                        {
                            IsVideo = true,
                            Cid = RssProtocol.UrlToCid(thumb, x.QuerySelector("video")!.GetAttribute("src"))!,
                        };
                    })).ToArray();
                    if (data.Media.Length == 0 && text == null && x.QuerySelector(".message_media_not_supported_label") != null) return null;
                    var post = new VirtualRssPost(new QualifiedPluggablePostId(did, new NonQualifiedPluggablePostId(PluggableProtocol.CreateSyntheticTid(date, postId), "/" + postId)), data);
                    return post;
                }).WhereNonNull().ToArray();
                return new VirtualRssResult(profile, posts);
            };
        }
    }
}
