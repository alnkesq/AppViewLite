using FishyFlip.Lexicon.App.Bsky.Actor;
using FishyFlip.Lexicon.App.Bsky.Feed;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.SignalR;
using AppViewLite.Models;
using AppViewLite.Numerics;
using AppViewLite.Web.Components;

namespace AppViewLite.Web
{
    [Route("/api")]
    [ApiController]
    public class AppViewLiteController : ControllerBase
    {
        private readonly RequestContext ctx;

        public AppViewLiteController(RequestContext requestContext)
        {
            this.ctx = requestContext;
        }

        [HttpPost(nameof(CreatePostLike))]
        public async Task<object> CreatePostLike([FromBody] DidAndRKey postId)
        {
            return new
            {
                rkey = (await BlueskyEnrichedApis.Instance.CreatePostLikeAsync(postId.Did, Tid.Parse(postId.Rkey), ctx)).ToString()
            };
        }
        [HttpPost(nameof(DeletePostLike))]
        public async Task DeletePostLike([FromBody] RKeyOnly rkey)
        {
            await BlueskyEnrichedApis.Instance.DeletePostLikeAsync(Tid.Parse(rkey.Rkey), ctx);
        }

        [HttpPost(nameof(CreateRepost))]
        public async Task<object> CreateRepost([FromBody] DidAndRKey postId)
        {
            return new
            {
                rkey = (await BlueskyEnrichedApis.Instance.CreateRepostAsync(postId.Did, Tid.Parse(postId.Rkey), ctx)).ToString()
            };
        }
        [HttpPost(nameof(CreateFollow))]
        public async Task<object> CreateFollow([FromBody] DidOnly did)
        {
            return new
            {
                rkey = (await BlueskyEnrichedApis.Instance.CreateFollowAsync(did.Did, ctx)).ToString()
            };
        }
        [HttpPost(nameof(DeleteRepost))]
        public async Task DeleteRepost([FromBody] RKeyOnly rkey)
        {
            await BlueskyEnrichedApis.Instance.DeleteRepostAsync(Tid.Parse(rkey.Rkey), ctx);
        }
        [HttpPost(nameof(DeletePost))]
        public async Task DeletePost([FromBody] RKeyOnly rkey)
        {
            await BlueskyEnrichedApis.Instance.DeletePostAsync(Tid.Parse(rkey.Rkey), ctx);
        }
        [HttpPost(nameof(DeleteFollow))]
        public async Task DeleteFollow([FromBody] RKeyOnly rkey)
        {
            await BlueskyEnrichedApis.Instance.DeleteFollowAsync(Tid.Parse(rkey.Rkey), ctx);
        }

        [HttpPost(nameof(MarkLastSeenNotification))]
        public async Task MarkLastSeenNotification([FromBody] NotificationIdArgs notificationId)
        {
            var notification = Notification.Deserialize(notificationId.NotificationId);
            if (notification != default)
            {
                BlueskyEnrichedApis.Instance.WithRelationshipsLock(rels => rels.LastSeenNotifications.Add(ctx.LoggedInUser, notification));
                BlueskyEnrichedApis.Instance.DangerousUnlockedRelationships.UserNotificationSubscribersThreadSafe.MaybeNotify(ctx.LoggedInUser, handler => handler(0));
            }
        }


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

            if (forceResults != null)
            {
                profiles = (await BlueskyEnrichedApis.Instance.GetProfilesAsync(forceResults.Split(','), ctx, profile => NotifySearchAutocompleteUpdates()), null);
            }
            else
            {
                profiles = await BlueskyEnrichedApis.Instance.SearchProfilesAsync(q, allowPrefixForLastWord: true, null, 5, ctx, onProfileDataAvailable: profile => NotifySearchAutocompleteUpdates());
            }
            responseAlreadySent = true;
            return new
            {
                Html = await Program.RenderComponentAsync<ProfileSearchAutocomplete>(new Dictionary<string, object?> { { "Profiles", profiles.Profiles }, { "SearchQuery", q } })
            };
        }


        public record DidAndRKey(string Did, string Rkey);
        public record RKeyOnly(string Rkey);
        public record DidOnly(string Did);
        public record NotificationIdArgs(string NotificationId);
    }
}

