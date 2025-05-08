using AppViewLite.Numerics;
using FishyFlip.Lexicon;
using FishyFlip.Lexicon.App.Bsky.Feed;
using FishyFlip.Lexicon.App.Bsky.Graph;
using FishyFlip.Lexicon.App.Bsky.Notification;
using FishyFlip.Lexicon.App.Bsky.Unspecced;
using FishyFlip.Lexicon.Chat.Bsky.Convo;
using FishyFlip.Models;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Mvc;
using System.IO.Hashing;

namespace AppViewLite.Web.ApiCompat
{
    [Route("/xrpc")]
    [ApiController]
    [EnableCors("BskyClient")]
    public class AppBskyNotification : Controller
    {
        private readonly BlueskyEnrichedApis apis;
        private readonly RequestContext ctx;
        public AppBskyNotification(BlueskyEnrichedApis apis, RequestContext ctx)
        {
            this.apis = apis;
            this.ctx = ctx;
        }

        [HttpGet("app.bsky.notification.listNotifications")]
        public async Task<IResult> ListNotifications(int limit)
        {
            var notifications = await apis.GetNotificationsAsync(ctx, dark: false);
            var allNotifications = notifications.NewNotifications.Select(x => (IsNew: true, Notification: x)).Concat(notifications.OldNotifications.Select(x => (IsNew: false, Notification: x)))
                .ToArray();
            return new ListNotificationsOutput
            {
                Notifications = allNotifications.Select(x => 
                {

                    ATUri uri;
                    ATObject record;
                    if (x.Notification.Kind == Models.NotificationKind.LikedYourPost)
                    {
                        record = new Like
                        {
                            CreatedAt = x.Notification.EventDate,
                            Subject = ApiCompatUtils.GetPostStrongRef(x.Notification.Post!.Did, x.Notification.Post.RKey),
                        };
                        uri = new ATUri("at://" + x.Notification.Profile!.Did + "/app.bsky.feed.like/" + x.Notification.NotificationCore.RKey.ToString());
                    }
                    else if (x.Notification.Kind == Models.NotificationKind.LikedYourFeed)
                    {
                        record = new Like
                        {
                            CreatedAt = x.Notification.EventDate,
                            Subject = ApiCompatUtils.GetStrongRef(x.Notification.Feed!.AtUri),
                        };
                        uri = new ATUri("at://" + x.Notification.Profile!.Did + "/app.bsky.feed.like/" + x.Notification.NotificationCore.RKey.ToString());
                    }
                    else if (x.Notification.Kind == Models.NotificationKind.RepostedYourPost)
                    {
                        record = new Repost
                        {
                            CreatedAt = x.Notification.EventDate,
                            Subject = ApiCompatUtils.GetPostStrongRef(x.Notification.Post!.Did, x.Notification.Post.RKey),
                        };
                        uri = new ATUri("at://" + x.Notification.Profile!.Did + "/app.bsky.feed.repost/" + x.Notification.NotificationCore.RKey.ToString());
                    }
                    else if (x.Notification.Kind is Models.NotificationKind.FollowedYou or Models.NotificationKind.FollowedYouBack)
                    {
                        record = new Follow
                        {
                            CreatedAt = x.Notification.EventDate,
                            Subject = new ATDid(ctx.UserContext.Profile!.Did),
                        };
                        uri = new ATUri("at://" + x.Notification.Profile!.Did + "/app.bsky.graph.follow/zz" + x.Notification.Profile.PlcId);
                    }
                    else if (x.Notification.Post != null)
                    {
                        record = ApiCompatUtils.ToApiCompatPost(x.Notification.Post);
                        uri = x.Notification.Post.AtUri;
                    }
                    else return null;

                    return new Notification
                    {
                        Author = ApiCompatUtils.ToApiCompatProfileView(x.Notification.Profile!),
                        IsRead = !x.IsNew,
                        Reason = x.Notification.Kind switch
                        {
                            Models.NotificationKind.FollowedYou => "follow",
                            Models.NotificationKind.LikedYourPost => "like",
                            Models.NotificationKind.RepostedYourPost => "repost",
                            Models.NotificationKind.QuotedYourPost => "quote",
                            Models.NotificationKind.MentionedYou => "mention",
                            Models.NotificationKind.RepliedToYourPost => "reply",
                            Models.NotificationKind.RepliedToYourThread => "reply",
                            Models.NotificationKind.FollowedYouBack => "follow",
                            Models.NotificationKind.LikedYourFeed => "like",
                            Models.NotificationKind.RepliedToADescendant => "reply",
                            _ => string.Empty,
                        },
                        IndexedAt = x.Notification.EventDate,
                        Record = record,
                        Uri = uri,
                        Cid = ApiCompatUtils.GetSyntheticCid(uri),
                    };
                })
                .Where(x => x != null && !string.IsNullOrEmpty(x.Reason))
                .ToList()!,
            }.ToJsonResponse();
        }
        [HttpPost("app.bsky.notification.updateSeen")]
        public object UpdateSeen(UpdateSeenInput input)
        {
            apis.MarkLastSeenNotification(new Models.Notification(((ApproximateDateTime32)input.SeenAt!.Value).AddTicks(1), default, default, Models.NotificationKind.None), ctx);
            return Ok();
        }
    }
}

