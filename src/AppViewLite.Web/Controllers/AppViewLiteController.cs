using FishyFlip.Lexicon.App.Bsky.Actor;
using FishyFlip.Lexicon.App.Bsky.Feed;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using AppViewLite.Models;
using AppViewLite.Numerics;

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



        public record DidAndRKey(string Did, string Rkey);
        public record RKeyOnly(string Rkey);
        public record DidOnly(string Did);
        public record NotificationIdArgs(string NotificationId);
    }
}

