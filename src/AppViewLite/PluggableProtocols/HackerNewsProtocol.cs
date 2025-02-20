using AppViewLite.Models;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace AppViewLite.PluggableProtocols.HackerNews
{
    public class HackerNewsProtocol : PluggableProtocol
    {
        public HackerNewsProtocol()
            : base("did:hackernews:")
        { 
        }
        public async override Task DiscoverAsync(CancellationToken ct)
        {
            await PluggableProtocol.RetryInfiniteLoopAsync(async ct =>
            {
                OnProfileDiscovered(HackerNewsMainDid, new BlueskyProfileBasicInfo { DisplayName = "Hacker News" });
                while (true)
                {
                    // Official API would require polling scores for every individual thread: https://hacker-news.firebaseio.com/v0/newstories.json

                    var hackerNewsHomeUrl = new Uri("https://news.ycombinator.com/");
                    var dom = StringUtils.ParseHtml(await BlueskyEnrichedApis.DefaultHttpClient.GetStringAsync(hackerNewsHomeUrl, ct));


                    

                    foreach (var post in dom.QuerySelectorAll(".submission"))
                    {
                        var postMeta = post.NextElementSibling!;
                        var username = postMeta.QuerySelector(".hnuser")?.TextContent;
                        if (username == null) continue;
                        var date = DateTime.UnixEpoch.AddSeconds(long.Parse(postMeta.QuerySelector(".age")!.GetAttribute("title")!.Split(' ')[1])); // format: title="2025-02-19T12:40:58 1739968858"
                        var id = long.Parse(post.Id!);
                        var threadLink = postMeta.QuerySelectorAll("a").Last();
                        var titleLink = post.QuerySelector(".titleline a");
                        var threadUrl = new Uri(hackerNewsHomeUrl, threadLink.GetAttribute("href"));
                        var titleUrl = new Uri(hackerNewsHomeUrl, titleLink!.GetAttribute("href"));
                        var hasExternalLink = threadUrl != titleUrl;

                        char[] whitespace = ['\u00A0',' '];

                        var data = new BlueskyPostData
                        {
                            PluggableLikeCount = int.Parse(postMeta.QuerySelector(".score")!.TextContent.Split(whitespace, StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)[0]),
                            PluggableReplyCount = threadLink!.TextContent != "discuss" ? int.Parse(threadLink!.TextContent.Split(whitespace, StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)[0]) : 0,
                            Text = titleLink!.TextContent,
                            ExternalUrl = hasExternalLink ? titleUrl.AbsoluteUri : null,
                        };

                        var postId = OnPostDiscovered(new QualifiedPluggablePostId(DidPrefix + username, new NonQualifiedPluggablePostId(CreateSyntheticTid(date, id.ToString()), id)), null, null, data);

                        if (postId != null)
                            OnRepostDiscovered(HackerNewsMainDid, postId.Value, date);
                    }

                    await Task.Delay(TimeSpan.FromMinutes(10), ct);
                    return;
                }
            }, ct);
        }

        public override bool RepostsAreCategories => true;

        public override string? GetIndexableDidText(string did)
        {
            return GetUserName(did);
        }

        public override string? TryGetOriginalPostUrl(QualifiedPluggablePostId postId, BlueskyPost post)
        {
            return "https://news.ycombinator.com/item?id=" + postId.PostId.Int64;
        }

        public override string? TryGetOriginalProfileUrl(BlueskyProfile profile)
        {
            var username = GetUserName(profile.Did);
            if (username == null)
                return "https://news.ycombinator.com/";
            return "https://news.ycombinator.com/user?id=" + Uri.EscapeDataString(username);
        }

        public string HackerNewsMainDid => DidPrefix;
        private string? GetUserName(string did)
        {
            var username = did.Substring(DidPrefixLength);
            if (username.Length == 0) return null;
            return username;
        }

        protected internal override void EnsureValidDid(string did)
        {
            var username = GetUserName(did);
            if (username != null)
            {
                if (username.AsSpan().ContainsAnyExcept(ValidUserNameChars))
                    throw new UnexpectedFirehoseDataException("Invalid HackerNews username.");
            }
        }

        public override string? GetDisplayNameFromDid(string did)
        {
            if (did == HackerNewsMainDid) return "Hacker News";
            return GetUserName(did);
        }

        // case sensitive usernames
        private readonly static SearchValues<char> ValidUserNameChars = SearchValues.Create("-_0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ");

        public override string? GetDefaultAvatar(string did)
        {
            return "/assets/default-hackernews-avatar.svg";
        }

        public override string? GetDefaultBannerColor(string did)
        {
            return "#FF6600";
        }
    }
}

