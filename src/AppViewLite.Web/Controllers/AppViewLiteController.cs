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
        public async Task<object> CreatePostLike([FromBody] DidAndRKey postId)
        {
            return new
            {
                rkey = (await apis.CreatePostLikeAsync(postId.Did, Tid.Parse(postId.Rkey), ctx)).ToString()
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
        public async Task<object> CreateRepost([FromBody] DidAndRKey postId)
        {
            return new
            {
                rkey = (await apis.CreateRepostAsync(postId.Did, Tid.Parse(postId.Rkey), ctx)).ToString()
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

        [HttpPost(nameof(MarkLastSeenNotification))]
        public void MarkLastSeenNotification([FromBody] NotificationIdArgs notificationId)
        {
            var notification = Notification.Deserialize(notificationId.NotificationId);
            if (notification != default)
            {
                apis.WithRelationshipsWriteLock(rels => rels.LastSeenNotifications.Add(ctx.LoggedInUser, notification), ctx);
                apis.DangerousUnlockedRelationships.UserNotificationSubscribersThreadSafe.MaybeNotifyOutsideLock(ctx.LoggedInUser, handler => handler(0));
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
                var seenPosts =  rels.CompactPostEngagements(rels.SeenPosts.GetValuesSorted(ctx.LoggedInUser)).ToDictionary(x => x.PostId, x => (Flags: x.Kind, DateFirstSeen: default(DateTime)));
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
            var json = JsonSerializer.Serialize(obj, IndentedOptions);
            return TypedResults.File(Encoding.UTF8.GetBytes(json), fileDownloadName: $"AppViewLite-{ctx.UserContext.Did!.Replace(":", "_")}-{now.ToString("yyyy-MM-dd-HHmmss")}.json");
        }

        private readonly static JsonSerializerOptions IndentedOptions = new JsonSerializerOptions { WriteIndented = true };

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
                    q.StartsWith("at://", StringComparison.Ordinal)
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


        public record DidAndRKey(string Did, string Rkey);
        public record RKeyOnly(string Rkey);
        public record ToggleFollowArgs(string Did, bool Private);
        public record NotificationIdArgs(string NotificationId);
        public record TogglePrivateFollowArgs(string Did, string Flag, bool NewValue);
        public record DeleteBookmarkArgs(string PostDid, string PostRkey, string BookmarkRKey);
        public record ToggleDomainMuteArgs(string Domain, bool Mute);
    }
}

