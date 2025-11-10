using Microsoft.AspNetCore.Mvc;
using AppViewLite.Models;
using AppViewLite.Numerics;
using AppViewLite.Web.Components;
using System.Text.Json;
using System.Text;
using System.Runtime.InteropServices;

namespace AppViewLite.Web
{
    [Route("/api")]
    [ApiController]
    public class AppViewLiteController : ControllerBase
    {
        private readonly RequestContext ctx;
        private readonly BlueskyEnrichedApis apis;

        public AppViewLiteController(RequestContext requestContext, BlueskyEnrichedApis apis)
        {
            this.ctx = requestContext;
            this.apis = apis;
        }

        [HttpPost(nameof(CreatePostLike))]
        public async Task<object> CreatePostLike([FromBody] DidAndRKeyWithVia postId)
        {
            return new
            {
                rkey = (await apis.CreatePostLikeAsync(postId.Did, Tid.Parse(postId.Rkey), postId.GetVia(), ctx)).ToString()
            };
        }

        [HttpPost(nameof(DeletePostLike))]
        public async Task DeletePostLike([FromBody] RKeyOnly rkey)
        {
            await apis.DeletePostLikeAsync(Tid.Parse(rkey.Rkey), ctx);
        }



        [HttpPost(nameof(CreatePostBookmark))]
        public object CreatePostBookmark([FromBody] DidAndRKey postId)
        {
            return new
            {
                rkey = apis.CreatePostBookmark(postId.Did, Tid.Parse(postId.Rkey), ctx).ToString()
            };
        }
        [HttpPost(nameof(DeletePostBookmark))]
        public void DeletePostBookmark([FromBody] DeleteBookmarkArgs args)
        {
            apis.DeletePostBookmark(args.PostDid, Tid.Parse(args.PostRkey), Tid.Parse(args.BookmarkRKey), ctx);
        }

        [HttpPost(nameof(CreateRepost))]
        public async Task<object> CreateRepost([FromBody] DidAndRKeyWithVia postId)
        {
            return new
            {
                rkey = (await apis.CreateRepostAsync(postId.Did, Tid.Parse(postId.Rkey), postId.GetVia(), ctx)).ToString()
            };
        }
        [HttpPost(nameof(CreateFollow))]
        public async Task<object> CreateFollow([FromBody] ToggleFollowArgs args)
        {
            if (args.Private)
            {
                apis.TogglePrivateFollowFlag(args.Did, PrivateFollowFlags.PrivateFollow, true, ctx);
                return new { rkey = "x" };
            }
            else
            {
                return new
                {
                    rkey = (await apis.CreateFollowAsync(args.Did, ctx)).ToString()
                };
            }

        }
        [HttpPost(nameof(CreateBlock))]
        public async Task<object> CreateBlock([FromBody] DidOnly args)
        {

            return new
            {
                rkey = (await apis.CreateBlockAsync(args.Did, ctx)).ToString()
            };
        }
        [HttpPost(nameof(DeleteRepost))]
        public async Task DeleteRepost([FromBody] RKeyOnly rkey)
        {
            await apis.DeleteRepostAsync(Tid.Parse(rkey.Rkey), ctx);
        }
        [HttpPost(nameof(DeletePost))]
        public async Task DeletePost([FromBody] RKeyOnly rkey)
        {
            await apis.DeletePostAsync(Tid.Parse(rkey.Rkey), ctx);
        }
        [HttpPost(nameof(HideThreadReply))]
        public async Task HideThreadReply([FromBody] DidAndRKey postId)
        {
            await apis.HideThreadReply(postId.Did, Tid.Parse(postId.Rkey), ctx);
        }
        [HttpPost(nameof(DeleteFollow))]
        public async Task DeleteFollow([FromBody] DidAndRKey args)
        {
            if (args.Rkey == "x")
            {
                apis.TogglePrivateFollowFlag(args.Did, PrivateFollowFlags.PrivateFollow, false, ctx);
            }
            else
            {
                await apis.DeleteFollowAsync(Tid.Parse(args.Rkey), ctx);
            }
        }

        [HttpPost(nameof(DeleteBlock))]
        public async Task DeleteBlock([FromBody] RKeyOnly args)
        {
            await apis.DeleteBlockAsync(Tid.Parse(args.Rkey), ctx);
        }

        [HttpPost(nameof(TogglePrivateFollow))]
        public void TogglePrivateFollow([FromBody] TogglePrivateFollowArgs args)
        {
            var flag = Enum.Parse<PrivateFollowFlags>(args.Flag);
            apis.TogglePrivateFollowFlag(args.Did, flag, args.NewValue, ctx);
        }
        [HttpPost(nameof(ToggleDomainMute))]
        public void ToggleDomainMute([FromBody] ToggleDomainMuteArgs args)
        {

            apis.ToggleDomainMute(args.Domain, args.Mute, ctx);
        }

        [HttpPost(nameof(MuteThread))]
        public void MuteThread([FromBody] DidAndRKey threadId)
        {
            var plc = apis.SerializeSingleDid(threadId.Did, ctx);
            ctx.PrivateProfile.MutedThreads = ctx.PrivateProfile.MutedThreads.Append(new PostIdProto(plc.PlcValue, Tid.Parse(threadId.Rkey).TidValue)).ToHashSet();
            apis.SaveAppViewLiteProfile(ctx);
        }

        [HttpPost(nameof(MarkLastSeenNotification))]
        public void MarkLastSeenNotification([FromBody] NotificationIdArgs notificationId)
        {
            var notification = Notification.Deserialize(notificationId.NotificationId);
            if (notification != default)
            {
                apis.MarkLastSeenNotification(notification, ctx);
            }
        }

        [HttpGet(nameof(AppViewTakeout))]
        public object AppViewTakeout()
        {
            string[] EnumFlagToArray<T>(T obj) where T : Enum => EqualityComparer<T>.Default.Equals(obj, default(T)) ? [] : obj.ToString().Split(',', StringSplitOptions.TrimEntries);
            var profile = ctx.UserContext.PrivateProfile!;
            var now = DateTime.UtcNow;
            var bookmarks = apis.GetBookmarks(16 * 1024 * 1024, null, ctx).ToArray();
            var obj = apis.WithRelationshipsLock(rels =>
            {
                var seenPosts = BlueskyRelationships.CompactPostEngagements(rels.SeenPosts.GetValuesSorted(ctx.LoggedInUser)).ToDictionary(x => x.PostId, x => (Flags: x.Kind, DateFirstSeen: default(DateTime)));
                foreach (var item in rels.SeenPostsByDate.GetValuesUnsorted(ctx.LoggedInUser))
                {
                    ref var p = ref CollectionsMarshal.GetValueRefOrAddDefault(seenPosts, item.PostId, out var _);
                    if (p.DateFirstSeen == default || item.Date < p.DateFirstSeen) p.DateFirstSeen = item.Date;
                }
                return new
                {
                    did = ctx.UserContext.Did,
                    firstLogin = profile.FirstLogin,
                    sessions = profile.Sessions.Select(x => new { loginDate = x.LogInDate }).ToArray(),
                    alwaysPreferBookmarkButton = profile.AlwaysPreferBookmarkButton,
                    theme = profile.Theme.ToString(),
                    accentColor = profile.AccentColor.ToString(),
                    moderationSettings = profile.LabelerSubscriptions.Select(x => new
                    {
                        labelerDid = rels.GetDid(new Plc(x.LabelerPlc)),
                        listRkey = x.ListRKey != 0 ? new Tid(x.ListRKey).ToString() : null,
                        labelName = x.ListRKey == 0 ? rels.GetLabel(new LabelId(new Plc(x.LabelerPlc), x.LabelerNameHash), ctx).Name : null,
                        behavior = x.Behavior.ToString(),
                        privateNickname = x.OverrideDisplayName,
                    }).ToArray(),
                    bookmarks = bookmarks.Select(x => new { dateBookmarked = x.Bookmark.BookmarkRKey.Date, did = x.Did, rkey = x.Bookmark.PostId.PostRKey.ToString(), originalPostUrl = rels.GetOriginalPostUrl(x.Bookmark.PostId, x.Did) }).ToArray(),
                    perUserSettings = profile.PrivateFollows.Where(x => x.Plc != 0).Select(x => new
                    {
                        did = rels.GetDid(new Plc(x.Plc)),
                        datePrivateFollowed = x.DatePrivateFollowed != default ? x.DatePrivateFollowed : (DateTime?)null,
                        flags = EnumFlagToArray(x.Flags),
                    }).ToArray(),
                    postEngagements = seenPosts.Select(x => new { did = rels.GetDid(x.Key.Author), rkey = x.Key.PostRKey.ToString(), flags = EnumFlagToArray(x.Value.Flags), dateFirstSeen = x.Value.DateFirstSeen }).ToArray(),
                    mutedWords = profile.MuteRules.Select(x => new { word = x.Word, did = x.AppliesToPlc != null ? rels.GetDid(new Plc(x.AppliesToPlc.Value)) : null }).ToArray(),
                };
            }, ctx);
            return TypedResults.Stream(async stream =>
            {
                await JsonSerializer.SerializeAsync(stream, obj, TakeoutJsonOptions);
            }, fileDownloadName: $"AppViewLite-{ctx.UserContext.Did!.Replace(":", "_")}-{now.ToString("yyyy-MM-dd-HHmmss")}.json");
        }

        private readonly static JsonSerializerOptions TakeoutJsonOptions = new JsonSerializerOptions { DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull };

        [HttpGet(nameof(SearchAutoComplete))]
        public async Task<object> SearchAutoComplete(string? q, string? forceResults)
        {

            var responseAlreadySent = false;
            ProfilesAndContinuation profiles;
            var ctx = this.ctx;
            void NotifySearchAutocompleteUpdates()
            {
                if (responseAlreadySent)
                {
                    ctx.SendSignalrAsync("SearchAutoCompleteProfileDetails");
                }
            }

            var hasUri = q != null &&
                (
                    q.StartsWith("https://", StringComparison.Ordinal) ||
                    q.StartsWith("http://", StringComparison.Ordinal) ||
                    q.StartsWith("at://", StringComparison.Ordinal) ||
                    (q.StartsWith("did:", StringComparison.Ordinal) && BlueskyEnrichedApis.IsValidDid(q))
                );

            if (hasUri)
            {
                profiles = new ProfilesAndContinuation();
            }
            if (forceResults != null)
            {
                profiles = (await apis.GetProfilesAsync(forceResults.Split(','), ctx, profile => NotifySearchAutocompleteUpdates()), null);
            }
            else
            {
                profiles = await apis.SearchProfilesAsync(q ?? string.Empty, allowPrefixForLastWord: true, null, 5, ctx, onLateDataAvailable: profile => NotifySearchAutocompleteUpdates());
            }
            responseAlreadySent = true;
            return new
            {
                Html = await Program.RenderComponentAsync<ProfileSearchAutocomplete>(new Dictionary<string, object?>
                {
                    { "Profiles", profiles.Profiles },
                    { "SearchQuery", q },
                    { "GoToUri", hasUri },
                })
            };
        }

        [HttpPost(nameof(PinFeed))]
        public void PinFeed(DidAndRKey args)
        {
            var plc = apis.SerializeSingleDid(args.Did, ctx);
            lock (ctx.PrivateProfile)
            {
                if (ctx.PrivateProfile.FeedSubscriptions.Any(x => new Plc(x.FeedPlc) == plc && x.FeedRKey == args.Rkey))
                    return;
                ctx.PrivateProfile.FeedSubscriptions = ctx.PrivateProfile.FeedSubscriptions.Append(new FeedSubscription { FeedRKey = args.Rkey, FeedPlc = plc.PlcValue }).ToArray();
            }
            apis.SaveAppViewLiteProfile(ctx);
        }

        [HttpPost(nameof(UnpinFeed))]
        public void UnpinFeed(DidAndRKey args)
        {
            var plc = apis.SerializeSingleDid(args.Did, ctx);
            lock (ctx.PrivateProfile)
            {
                ctx.PrivateProfile.FeedSubscriptions = ctx.PrivateProfile.FeedSubscriptions.Where(x => !(new Plc(x.FeedPlc) == plc && x.FeedRKey == args.Rkey)).ToArray();
            }
            apis.SaveAppViewLiteProfile(ctx);
        }
        [HttpPost(nameof(SetLabelerMode))]
        public void SetLabelerMode(SetLabelerModeArgs args)
        {
            ModerationBehavior? mode = args.Mode != null ? Enum.Parse<ModerationBehavior>(args.Mode) : null;
            var plc = apis.SerializeSingleDid(args.Did, ctx);
            var subscription = new LabelerSubscription { LabelerPlc = plc.PlcValue, Behavior = ModerationBehavior.None };
            if (args.ListRkey != null)
                subscription.ListRKey = Tid.Parse(args.ListRkey).TidValue;
            else
                subscription.LabelerNameHash = BlueskyRelationships.HashLabelName(args.LabelName!);
            lock (ctx.PrivateProfile)
            {
                subscription = ctx.PrivateProfile.LabelerSubscriptions.FirstOrDefault(x => x.LabelerPlc == subscription.LabelerPlc && x.ListRKey == subscription.ListRKey && x.LabelerNameHash == subscription.LabelerNameHash) ?? subscription;

                if (mode != null) subscription.Behavior = mode.Value;
                if (args.Nickname != null) subscription.OverrideDisplayName = args.Nickname.Length == 0 ? null : args.Nickname;
                if (subscription.Behavior == ModerationBehavior.None && args.Nickname == null)
                {
                    ctx.PrivateProfile.LabelerSubscriptions = ctx.PrivateProfile.LabelerSubscriptions.Where(x => x != subscription).ToArray();
                }
                else if (!ctx.PrivateProfile.LabelerSubscriptions.Contains(subscription))
                {
                    ctx.PrivateProfile.LabelerSubscriptions = ctx.PrivateProfile.LabelerSubscriptions.Append(subscription).ToArray();
                }
            }
            apis.SaveAppViewLiteProfile(ctx);
        }

        public record DidAndRKey(string Did, string Rkey);
        public record DidAndRKeyWithVia(string Did, string Rkey, string? ViaDid, string? ViaRkey)
        {
            public RelationshipStr GetVia() => ViaDid != null && ViaRkey != null && Did != ViaDid ? new RelationshipStr(ViaDid, ViaRkey) : default;
        }
        public record RKeyOnly(string Rkey);
        public record DidOnly(string Did);
        public record ToggleFollowArgs(string Did, bool Private);
        public record NotificationIdArgs(string NotificationId);
        public record TogglePrivateFollowArgs(string Did, string Flag, bool NewValue);
        public record DeleteBookmarkArgs(string PostDid, string PostRkey, string BookmarkRKey);
        public record ToggleDomainMuteArgs(string Domain, bool Mute);
        public record SetLabelerModeArgs(string Did, string? ListRkey, string? LabelName, string? Mode, string? Nickname /*null=dont change, empty:use default*/);
    }
}

