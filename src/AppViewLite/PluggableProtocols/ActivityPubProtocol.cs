using AngleSharp.Dom;
using AngleSharp.Html.Dom;
using AngleSharp.Html.Parser;
using AppViewLite.Models;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace AppViewLite.PluggableProtocols.ActivityPub
{
    public class ActivityPubProtocol : PluggableProtocol
    {
        new const string DidPrefix = "did:fedi:";
        public ActivityPubProtocol() : base(DidPrefix)
        {
        }

        public override Task DiscoverAsync(CancellationToken ct)
        {
            foreach (var relay in AppViewLiteConfiguration.GetStringList(AppViewLiteParameter.APPVIEWLITE_LISTEN_ACTIVITYPUB_RELAYS) ?? [])
            {
                RetryInfiniteLoopAsync(ct => ListenActivityPubRelay(relay, ct), ct).FireAndForget();
            }
            return Task.CompletedTask;
        }


        private readonly static JsonSerializerOptions JsonOptions = new JsonSerializerOptions { IncludeFields = true };
        private async Task ListenActivityPubRelay(string host, CancellationToken ct)
        {
            using var reader = new HttpEventStreamReader(await BlueskyEnrichedApis.DefaultHttpClient.GetStreamAsync($"https://{host}/api/v1/streaming/public", ct));
            await reader.BaseReader.ReadLineAsync(ct); // ":)"


            while (await reader.ReadAsync(ct) is { } evt)
            {

                try
                {
                    var post = System.Text.Json.JsonSerializer.Deserialize<ActivityPubPostJson>(evt.Data, JsonOptions)!;
                    OnPostReceived(post);
                }
                catch (Exception ex)
                {
                    Console.Error.WriteLine(ex);
                }
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        private void OnPostReceived(ActivityPubPostJson post)
        {
            if (post.reblog != null) return;

            var author = ParseActivityPubUserId(post.account, post.url).Normalize();
            if (author == default) return;
            if (author.Instance == "bsky.brid.gy") return;

            var url = new Uri(post.url);

            var did = GetDid(author);
            var postIdStr = GetNonQualifiedPostId(author, url);
            var tid = CreateSyntheticTid(post.created_at, postIdStr);

            var nonQualifiedPostId = NonQualifiedPluggablePostId.CreatePreferInt64(tid, postIdStr);
            var postId = new QualifiedPluggablePostId(did, nonQualifiedPostId);

            var card = post.card;
            var dom = StringUtils.ParseHtml(post.content);
            var (text, facets) = StringUtils.HtmlToFacets(dom, url, x => ElementToFacet(x, url));

            if (card != null) { }

            var data = new BlueskyPostData
            {
                Text = text,
                Facets = facets,
                ExternalDescription = card?.description,
                ExternalTitle = card?.title,
                ExternalUrl = card?.url,
                ExternalThumbCid = BlueskyRelationships.CompressBpe(card?.image),
                Language = BlueskyRelationships.ParseLanguage(post.language),
                Media = post.media_attachments != null && post.media_attachments.Length != 0 ? post.media_attachments.Select(x => ConvertMediaAttachment(x)).ToArray() : null
            };
            OnPostDiscovered(postId, null, null, data, shouldIndex: true);

        }

        private static BlueskyMediaData ConvertMediaAttachment(ActivityPubMediaAttachmentJson x)
        {

            return new BlueskyMediaData
            {
                 AltText = x.description,
                 Cid = BlueskyRelationships.CompressBpe(x.remote_url + "\n" + x.preview_url)!,
                 IsVideo = x.type?.Contains("video", StringComparison.OrdinalIgnoreCase) == true
            };
        }
        public async override Task<byte[]> GetBlobAsync(string did, byte[] bytes, ThumbnailSize preferredSize)
        {
            var urls = BlueskyRelationships.DecompressBpe(bytes)!.Split('\n');
            var url = (preferredSize == ThumbnailSize.feed_fullsize || preferredSize == ThumbnailSize.avatar) ? urls[1] : urls[0];
            if(string.IsNullOrEmpty(url))
                 url = urls.First(x => !string.IsNullOrEmpty(x));
            return await BlueskyEnrichedApis.DefaultHttpClient.GetByteArrayAsync(url);
        }

        private static FacetData? ElementToFacet(IElement element, Uri baseUrl)
        {
            
            var href = element.TagName == "A" ? element.GetAttribute("href") : null;

            if (Uri.TryCreate(baseUrl, href, out var url))
            {
                var segments = url.AbsolutePath.Split('/', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

                if (element.ClassList.Contains("hashtag"))
                {
                    // don't care
                }
                else if (segments.Length >= 2 && segments[0] is
                    "tag" or
                    "tags" or
                    "hashtag" or
                    "hashtags" or
                    "topic" or
                    "topics" && element.Text().Equals(("#" + segments[1]), StringComparison.InvariantCultureIgnoreCase))
                {
                    // don't care
                }
                else
                {
                    var link = url.AbsoluteUri;
                    if (link == element.Text())
                        return new FacetData { SameLinkAsText = true };
                    else
                        return new FacetData { Link = link };
                }
            }
            return null;
        }

        public static string GetDid(ActivityPubUserId author)
        {
            author = author.Normalize();
            BlueskyEnrichedApis.EnsureValidDomain(author.Instance);
            EnsureValidUserName(author.UserName);
            return DidPrefix + author.Instance + ":" + author.UserName;

        }

        private static void EnsureValidUserName(string userName)
        {
            if (string.IsNullOrEmpty(userName)) throw new Exception();
            if (userName.AsSpan().ContainsAnyExcept(ValidUserNameChars)) throw new Exception();
        }

        public readonly static SearchValues<char> ValidUserNameChars = SearchValues.Create("0123456789abcdefghijklmnopqrstuvwxyz.-_");

        public static string GetNonQualifiedPostId(ActivityPubUserId userId, Uri url)
        {
            if (url.Host == userId.Instance || (url.Host == "www." + userId.Instance && userId.Instance == "threads.net"))
            {
                if (string.IsNullOrEmpty(url.Query))
                {
                    var segments = url.AbsolutePath.Split('/', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
                    if (segments.Length == 2 && segments[0].Equals("@" + userId.UserName, StringComparison.OrdinalIgnoreCase))
                    {
                        var postIdSegment = segments[1];
                        return postIdSegment;
                    }
                }
                return url.PathAndQuery;
            }

            return url.AbsoluteUri;
        }

        private static ActivityPubUserId ParseActivityPubUserId(ActivityPubAccountJson author, string postUrl)
        {
            var hostFromAuthorUrl = new Uri(author.url!).Host;
            if (!Uri.TryCreate(postUrl, UriKind.Absolute, out var postUrlParsed)) return default;
            var hostFromPostUrl = postUrlParsed.Host;
            if (hostFromPostUrl.StartsWith("www.", StringComparison.Ordinal))
                hostFromPostUrl = hostFromPostUrl.Substring(4);
            if (hostFromPostUrl == "mkkey.net") hostFromAuthorUrl = hostFromPostUrl;
            if (hostFromAuthorUrl != hostFromPostUrl)
            {
                if (hostFromAuthorUrl == "sportsbots.xyz") return default;
            }
            if (hostFromAuthorUrl == "gleasonator.dev") hostFromAuthorUrl = null;

            var id = author.fqn ?? author.acct ?? author.username;

            if (string.IsNullOrEmpty(id))
            {
                return default;
            }

            if (id.EndsWith("@lor.sh/", StringComparison.Ordinal))
            {
                id = id.Substring(0, id.Length - 1);
            }

            if (author.fqn != null && !author.fqn.Contains('@'))
            {
                if (id.StartsWith("npub", StringComparison.Ordinal)) return default;
                if (author.nostr != null) return default;
                throw new Exception();
            }
            return ActivityPubUserId.Parse(id, hostFromAuthorUrl);
        }

        private static DateTime? TryParseActivityPubDate(string created_at)
        {
            if (DateTime.TryParse(created_at, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal, out var d))
            {
                return d;
            }
            return null;
        }


        public override string? GetOriginalUrl(string did, BlueskyPostData postData)
        {
            throw new NotImplementedException();
        }

        protected internal override void EnsureValidDid(string did)
        {
            var parts = did.Substring(DidPrefix.Length).Split(':');
            if (parts.Length != 2) throw new UnexpectedFirehoseDataException("Invalid did.");
            BlueskyEnrichedApis.EnsureValidDomain(parts[0]);
            EnsureValidUserName(parts[1]);
        }

        public override string? TryHandleToDid(string handle)
        {
            if (!handle.Contains('@')) return null;
            try
            {
                var userId = ActivityPubUserId.Parse(handle).Normalize();
                if (userId != default)
                    return GetDid(userId);
            }
            catch
            {
            }
            return null;
        }

        public override string? TryGetHandleFromDid(string did)
        {
            return GetUserFromDid(did).ToString().Substring(1);
        }

        public static ActivityPubUserId GetUserFromDid(string did)
        {
            var parts = did.Substring(DidPrefix.Length).Split(':');
            return new ActivityPubUserId(parts[0], parts[1]);
        }

        public override string? TryGetOriginalPostUrl(QualifiedPluggablePostId postId)
        {
            var user = GetUserFromDid(postId.Did);
            var postIdStr = postId.PostId.AsString;
            if (postIdStr.StartsWith('/'))
                return "https://" + user.Instance + postIdStr;
            else if (postIdStr.StartsWith("http://", StringComparison.Ordinal) || postIdStr.StartsWith("https://", StringComparison.Ordinal))
                return postIdStr;
            else
                return "https://" + user.Instance + "/@" + user.UserName + "/" + postIdStr;
        }

    }



    public record struct ActivityPubUserId(string Instance, string UserName)
    {
        public ActivityPubUserId Normalize()
        {
            if (this == default) return default;
            return new ActivityPubUserId(Instance.ToLowerInvariant(), UserName.ToLowerInvariant());
        }
        public static ActivityPubUserId Parse(string userAtHost, string? defaultDomain = null)
        {
            var parts = userAtHost.Split('@', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);

            if (parts.Length == 1)
            {
                if (defaultDomain != null) return new ActivityPubUserId(defaultDomain, parts[0]);
                throw new ArgumentException();
            }
            return new ActivityPubUserId(parts[1], parts[0]);
        }

        public override string ToString()
        {
            return "@" + UserName + "@" + Instance;
        }
    }

}
