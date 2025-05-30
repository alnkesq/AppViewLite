using AngleSharp.Dom;
using AppViewLite.Models;
using AppViewLite;
using DuckDbSharp.Types;
using System;
using System.Buffers;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace AppViewLite.PluggableProtocols.ActivityPub
{
    public class ActivityPubProtocol : PluggableProtocol
    {
        public new const string DidPrefix = "did:fedi:";

        private Stopwatch? RecentlyStoredProfilesLastReset;
        private HashSet<UInt128> RecentlyStoredProfiles = new();
        public static ActivityPubProtocol? Instance;
        public ConcurrentFullEvictionSetCache<QualifiedPluggablePostId> RecentlyStoredPosts = new(50_000);

        public ActivityPubProtocol() : base(DidPrefix)
        {

            Instance = this;
        }

        public override Task DiscoverAsync(CancellationToken ct)
        {
            foreach (var relay in AppViewLiteConfiguration.GetStringList(AppViewLiteParameter.APPVIEWLITE_LISTEN_ACTIVITYPUB_RELAYS) ?? [])
            {
                if (relay == "-") continue;
                RetryInfiniteLoopAsync(relay, ct => ListenActivityPubRelay(relay, ct), ct).FireAndForget();
            }
            return Task.CompletedTask;
        }

        public override string? GetDisplayHandle(BlueskyProfile profile)
        {
            return ParseDid(profile.Did).ToString();
        }

        private readonly static JsonSerializerOptions JsonOptions = new JsonSerializerOptions { IncludeFields = true };
        private async Task ListenActivityPubRelay(string host, CancellationToken ct)
        {
            using var reader = new HttpEventStreamReader(await BlueskyEnrichedApis.DefaultHttpClient.GetStreamAsync($"https://{host}/api/v1/streaming/public", ct));
            await reader.BaseReader.ReadLineAsync(ct); // ":)"


            while (await reader.ReadAsync(ct) is { } evt)
            {
                using var _ = BlueskyRelationshipsClientBase.CreateIngestionThreadPriorityScope();
                try
                {

                    var post = System.Text.Json.JsonSerializer.Deserialize<ActivityPubPostJson>(evt.Data!, JsonOptions)!;
                    OnPostReceived(post, host);
                }
                catch (Exception ex)
                {
                    LogLowImportanceException(ex);
                }
            }
        }


        public PostId? OnPostReceived(ActivityPubPostJson post, string relay)
        {
            if (post.reblog != null) return null;

            var author = ParseActivityPubUserId(post.account, post.url).Normalize();
            if (author == default) return null;
            if (Apis.AdministrativeBlocklist.ShouldBlockIngestion(author.Instance)) return null;

            var url = new Uri(post.url);

            var did = GetDid(author);
            var postIdStr = GetNonQualifiedPostId(author, url);
            var tid = CreateSyntheticTid(post.created_at, postIdStr);

            var nonQualifiedPostId = NonQualifiedPluggablePostId.CreatePreferInt64(tid, postIdStr);
            var postId = new QualifiedPluggablePostId(did, nonQualifiedPostId);

            if (!RecentlyStoredPosts.Add(postId)) return null;

            var card = post.card;
            var dom = StringUtils.ParseHtml(post.content).Body!;
            var (text, facets) = StringUtils.HtmlToFacets(dom, x => ElementToFacet(x, url));

            var shouldIndex = !(post.account.noindex == true);

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

            if (post.in_reply_to_account_id != null)
                data.IsReplyToUnspecifiedPost = true;

            if (post.conversation_id != null && post.mentions != null && post.mentions.Length != 0)
                data.IsReplyToUnspecifiedPost = true;

            var customEmojis = (post.emojis ?? []).Concat(post.account?.emojis ?? []).Select(x => new CustomEmoji(x.shortcode, x.static_url ?? x.url)).ToArray();

            var ctx = RequestContext.CreateForFirehose("ActivityPub:" + relay, allowStale: true);
            Apis.MaybeAddCustomEmojis(customEmojis, ctx);

            StringUtils.GuessCustomEmojiFacetsNoAdjacent(data.Text, ref data.Facets, customEmojis);

            var corePostId = OnPostDiscovered(postId, null, null, data, ctx, shouldIndex: shouldIndex);

            OnProfileReceived(did, author, post.account!, url, ctx, shouldIndex, customEmojis);
            return corePostId;
        }


        private void OnProfileReceived(string did, ActivityPubUserId author, ActivityPubAccountJson account, Uri baseUrl, RequestContext ctx, bool shouldIndex, CustomEmoji[] customEmojis)
        {


            lock (RecentlyStoredProfiles)
            {
                RecentlyStoredProfilesLastReset ??= Stopwatch.StartNew();
                if (RecentlyStoredProfilesLastReset.Elapsed.TotalHours >= 6 || RecentlyStoredProfiles.Count >= 500_000)
                {
                    RecentlyStoredProfiles.Clear();
                    RecentlyStoredProfilesLastReset.Restart();
                }

                if (!RecentlyStoredProfiles.Add(System.IO.Hashing.XxHash128.HashToUInt128(MemoryMarshal.AsBytes<char>(did))))
                    return;
            }

            var descriptionDom = StringUtils.ParseHtml(account.note).Body!;
            var description = StringUtils.HtmlToFacets(descriptionDom, x => ElementToFacet(x, baseUrl));
            var proto = new BlueskyProfileBasicInfo
            {
                DisplayName = account.display_name,
                HasExplicitFacets = true,
                DescriptionFacets = description.Facets,
                Description = description.Text,
                PluggableProtocolFollowerCount = account.followers_count,
                PluggableProtocolFollowingCount = account.following_count,
                AvatarCidBytes = MediaUrlsToBytes(account.avatar_static ?? account.avatar, null, null),
                BannerCidBytes = MediaUrlsToBytes(account.header_static ?? account.header, null, null),
                CustomFields = account.fields?.Select(x => ConvertFieldToProto(x, baseUrl)).WhereNonNull().ToArray()
            };
            if (proto.CustomFields != null && proto.CustomFields.Length == 0)
                proto.CustomFields = null;

            StringUtils.GuessCustomEmojiFacetsNoAdjacent(proto.Description, ref proto.DescriptionFacets, customEmojis);
            StringUtils.GuessCustomEmojiFacetsNoAdjacent(proto.DisplayName, ref proto.DisplayNameFacets, customEmojis);

            OnProfileDiscovered(did, proto, ctx, shouldIndex: shouldIndex, extraIndexableWords: ["%activitypub-instance:" + author.Instance]);

        }

        private static CustomFieldProto? ConvertFieldToProto(ActivityPubAccountFieldJson x, Uri baseUrl)
        {
            var dom = StringUtils.ParseHtml(x.value).Body!;
            while (ShouldTrimEmptyNode(dom.FirstChild))
                dom.FirstChild!.RemoveFromParent();
            while (ShouldTrimEmptyNode(dom.LastChild))
                dom.LastChild!.RemoveFromParent();

            string? value;
            if (dom.ChildNodes.Length == 1 && dom.FirstElementChild != null && TryGetAnchorUrl(dom.FirstElementChild, baseUrl) is { } url)
            {
                value = url.AbsoluteUri;
            }
            else
            {
                (value, _) = StringUtils.HtmlToFacets(dom, x => StringUtils.DefaultElementToFacet(x, baseUrl));
            }

            if (value == null) return null;
            return new CustomFieldProto(x.name, x.value)
            {
                VerifiedAt = x.verified_at,
            };
        }

        private static Uri? TryGetAnchorUrl(IElement a, Uri baseUrl)
        {
            if (a.TagName != "A") return null;
            return Uri.TryCreate(baseUrl, a.GetAttribute("href"), out var result) ? result : null;
        }

        private static bool ShouldTrimEmptyNode(INode? node)
        {
            return node != null && node.NodeType == NodeType.Text && string.IsNullOrWhiteSpace(node.TextContent);
        }

        private static BlueskyMediaData ConvertMediaAttachment(ActivityPubMediaAttachmentJson x)
        {
            return new BlueskyMediaData
            {
                AltText = x.description,
                Cid = MediaUrlsToBytes(x.remote_url, x.preview_url, x.url)!,
                IsVideo = x.type == "gifv" || x.type?.Contains("video", StringComparison.OrdinalIgnoreCase) == true
            };
        }

        private static byte[]? MediaUrlsToBytes(string? a, string? b, string? c)
        {
            if (a == null && b == null && c == null) return null;
            return BlueskyRelationships.CompressBpe(a + "\n" + b + "\n" + c);
        }

        public async override Task<BlobResult> GetBlobAsync(string did, byte[] bytes, ThumbnailSize preferredSize, CancellationToken ct)
        {
            var urls = BlueskyRelationships.DecompressBpe(bytes)!.Split('\n').Select(x => x.Length != 0 ? x : null).ToArray();

            var a = urls.ElementAtOrDefault(0);
            var b = urls.ElementAtOrDefault(1);
            var c = urls.ElementAtOrDefault(2);

            var fullSize =
                preferredSize == ThumbnailSize.feed_fullsize ||
                preferredSize == ThumbnailSize.avatar ||
                preferredSize == ThumbnailSize.feed_video_blob ||
                preferredSize == ThumbnailSize.feed_video_playlist;

            string url;

            if (fullSize)
            {
                url = c ?? a ?? b!;
            }
            else
            {
                url = b ?? c ?? a!;
            }

            return await BlueskyEnrichedApis.GetBlobFromUrl(new Uri(url), preferredSize: preferredSize, ct: ct);
        }

        private static FacetData? ElementToFacet(IElement element, Uri baseUrl)
        {


            if (TryGetAnchorUrl(element, baseUrl) is { } url)
            {
                var segments = url.AbsolutePath.Split('/', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

                if (element.ClassList.Contains("hashtag") || element.GetAttribute("rel") == "tag")
                {
                    // don't care
                }
                else if (segments.Length >= 2 && segments[0] is
                    "tag" or
                    "tags" or
                    "hashtag" or
                    "hashtags" or
                    "topic" or
#pragma warning disable CA1309 // Use ordinal string comparison
                    "topics" && element.Text().Equals(("#" + Uri.UnescapeDataString(segments[1])), StringComparison.InvariantCultureIgnoreCase))
#pragma warning restore CA1309 // Use ordinal string comparison
                {
                    // don't care
                }
                else if (element.ClassList.Contains("mention") && TryGetDidFromUrl(url) is { } did)
                {
                    return new FacetData { Did = did };
                }
                else if (url.Host == "bsky.brid.gy" && url.AbsolutePath.StartsWith("/ap/did:", StringComparison.Ordinal) && segments.Length == 2 && BlueskyEnrichedApis.IsValidDid(segments[1]))
                {
                    return new FacetData { Did = segments[1] };
                }
                else
                {
                    return StringUtils.DefaultElementToFacet(element, baseUrl);
                }
            }
            return null;
        }

        public override Task<string?> TryGetDidOrLocalPathFromUrlAsync(Uri url, bool preferDid)
        {
            var u = TryGetUserIdFromUrl(url);
            if (preferDid) return Task.FromResult((string?)GetDid(u));
            return Task.FromResult(u != default ? "/" + u : null);
        }

        public static string? TryGetDidFromUrl(Uri url)
        {
            var u = TryGetUserIdFromUrl(url);
            return u != default ? GetDid(u) : null;
        }


        private static ActivityPubUserId TryGetUserIdFromUrl(Uri url)
        {
            if (IsFalsePositiveDomain(url.GetDomainTrimWww()))
                return default;
            try
            {
                var pathSegments = url.AbsolutePath.Split('/', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

                // Mastodon profile
                if (pathSegments.Length == 1 && pathSegments[0].StartsWith('@'))
                {
                    return ActivityPubUserId.Parse(pathSegments[0], url.Host).Normalize();
                }

                // Other fediverse profile
                if (pathSegments.Length == 2 && pathSegments[0] == "users")
                {
                    return ActivityPubUserId.Parse(pathSegments[1], url.Host).Normalize();
                }
            }
            catch
            {
            }
            return default;
        }

        private static bool IsFalsePositiveDomain(string host)
        {
            return host is "medium.com" or "youtube.com" or "primal.net" or "nostrcheck.me";
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
            if (string.IsNullOrEmpty(userName)) throw new Exception("Null or empty ActivityPub username.");
            if (userName.AsSpan().ContainsAnyExcept(ValidUserNameChars)) throw new Exception("Not a valid ActivityPub username: " + userName);
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
                        return segments[1];
                    }
                    if (segments.Length == 4 && segments[0] == "users" && segments[1].Equals(userId.UserName, StringComparison.OrdinalIgnoreCase) && segments[2] == "statuses")
                    {
                        return segments[3];
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
            var hostFromPostUrl = postUrlParsed.GetDomainTrimWww();
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
                throw new ArgumentException("Cannot parse ActivityPub fqn username " + author.fqn);
            }
            var userId = ActivityPubUserId.Parse(id, hostFromAuthorUrl);
            if (hostFromPostUrl == "gleasonator.dev" && (userId.Instance != "gleasonator.dev" || userId.UserName.StartsWith("npub", StringComparison.Ordinal))) return default;
            return userId;
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
            return ParseDid(did).ToString().Substring(1);
        }

        public static ActivityPubUserId ParseDid(string did)
        {
            var parts = did.Substring(DidPrefix.Length).Split(':');
            return new ActivityPubUserId(parts[0], parts[1]);
        }

        public override string? TryGetOriginalPostUrl(QualifiedPluggablePostId postId, BlueskyPost post)
        {
            if (!postId.HasExternalIdentifier) return null;
            var user = ParseDid(postId.Did);
            var postIdStr = postId.PostId.AsString!;
            if (postIdStr.StartsWith('/'))
                return "https://" + user.Instance + postIdStr;
            else if (postIdStr.StartsWith("http://", StringComparison.Ordinal) || postIdStr.StartsWith("https://", StringComparison.Ordinal))
                return postIdStr;
            else
                return "https://" + user.Instance + "/@" + user.UserName + "/" + postIdStr;
        }


        public override string TryGetOriginalProfileUrl(BlueskyProfile profile)
        {
            var user = ParseDid(profile.Did);
            return "https://" + user.Instance + "/@" + user.UserName;
        }

        public override string GetIndexableDidText(string did)
        {
            var user = ParseDid(did);
            return user.Instance + " " + user.UserName;
        }


        public override string? TryGetDomainForDid(string did)
        {
            return ParseDid(did).Instance;
        }

        public override string? GetFollowingUrl(string did)
        {
            var userId = ParseDid(did);
            return $"https://{userId.Instance}/@{userId.UserName}/following";
        }
        public override string? GetFollowersUrl(string did)
        {
            var userId = ParseDid(did);
            return $"https://{userId.Instance}/@{userId.UserName}/followers";
        }

        public override bool RequiresLateOpenGraphData(BlueskyPost post)
        {
            return DefaultRequiresLateOpenGraphData(post, alsoConsiderLinkFacets: true);
        }


    }



    public record struct ActivityPubUserId(string Instance, string UserName)
    {
        public ActivityPubUserId Normalize()
        {
            if (this == default) return default;
            string instance = StringUtils.TrimWww(this.Instance.ToLowerInvariant());
            if (!BlueskyEnrichedApis.IsValidDomain(instance) && !instance.AsSpan().ContainsAny("/@:% "))
            {
                if (Uri.TryCreate("https://" + instance + "/", UriKind.Absolute, out var url) && url.PathAndQuery == "/")
                {
                    instance = url.IdnHost;
                }
            }
            return new ActivityPubUserId(instance.ToLowerInvariant(), UserName.ToLowerInvariant());
        }
        public static ActivityPubUserId Parse(string userAtHost, string? defaultDomain = null)
        {
            var parts = userAtHost.Split('@', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);

            if (parts.Length == 1)
            {
                if (defaultDomain != null) return new ActivityPubUserId(defaultDomain, parts[0]);
                throw new ArgumentException("Cannot parse ActivityPub user when defaultDomain is not specified: " + userAtHost);
            }
            return new ActivityPubUserId(parts[1], parts[0]);
        }

        public override string ToString()
        {
            return "@" + UserName + "@" + Instance;
        }

    }


}
