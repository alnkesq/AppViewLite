using Microsoft.AspNetCore.SignalR;
using AppViewLite.Models;
using AppViewLite.Numerics;
using System.Collections.Concurrent;

namespace AppViewLite.Web
{
    internal class AppViewLiteHub : Hub
    {
        public AppViewLiteHub()
        {
            
        }

        public Task SubscribeUnsubscribePosts(string[] toSubscribe, string[] toUnsubscribe)
        {
            var ctx = connectionIdToCallback[Context.ConnectionId];
            
            var dangerousRels = BlueskyEnrichedApis.Instance.DangerousUnlockedRelationships;
            var (postIdsToSubscribe, postIdsToUnsubscribe) = BlueskyEnrichedApis.Instance.WithRelationshipsLock(rels =>
            {
                return (
                    toSubscribe.Select(x => PostIdString.Deserialize(x)).Select(x => new PostId(rels.SerializeDid(x.Did), Tid.Parse(x.RKey))).ToArray(),
                    toUnsubscribe.Select(x => PostIdString.Deserialize(x)).Select(x => new PostId(rels.SerializeDid(x.Did), Tid.Parse(x.RKey))).ToArray());
            });
            lock (ctx)
            {
                foreach (var postId in postIdsToSubscribe)
                {
                    dangerousRels.PostLiveSubscribersThreadSafe.Subscribe(postId, ctx.LiveUpdatesCallback);
                    ctx.PostIds.Add(postId);
                }
                foreach (var postId in postIdsToUnsubscribe)
                {
                    dangerousRels.PostLiveSubscribersThreadSafe.Unsubscribe(postId, ctx.LiveUpdatesCallback);
                    ctx.PostIds.Remove(postId);
                }
            }
            return Task.CompletedTask;
        }

        public override Task OnConnectedAsync()
        {
            var httpContext = this.Context.GetHttpContext();
            var ctx = Program.TryGetSession(httpContext);
            var userPlc = ctx != null && ctx.IsLoggedIn ? ctx.LoggedInUser : default;
            var connectionId = Context.ConnectionId;

            var context = new ConnectionContext
            {
                UserPlc = userPlc,
                LiveUpdatesCallback = (notification, commitPlc) =>
                {
                    var client = Program.AppViewLiteHubContext.Clients.Client(connectionId);

                    object? ownRelationshipChange = null;
                    if (commitPlc == userPlc)
                    {
                        // In this callback, we're already inside the lock.
                        var rels = BlueskyEnrichedApis.Instance.DangerousUnlockedRelationships;
                        rels.Likes.HasActor(notification.PostId, commitPlc, out var userLike);
                        rels.Reposts.HasActor(notification.PostId, commitPlc, out var userRepost);
                        ownRelationshipChange = new
                        {
                            likeRkey = userLike.RelationshipRKey.ToString() ?? "-",
                            repostRkey = userRepost.RelationshipRKey.ToString() ?? "-",
                        };
                    }
                    _ = client.SendAsync("PostEngagementChanged", new { notification.Did, notification.RKey, notification.LikeCount, notification.RepostCount, notification.QuoteCount, notification.ReplyCount }, ownRelationshipChange);
                },
                UserNotificationCallback = (notificationCount) =>
                {
                    var client = Program.AppViewLiteHubContext.Clients.Client(connectionId);

                    _ = client.SendAsync("NotificationCount", notificationCount);
                }
            };
            if (!connectionIdToCallback.TryAdd(connectionId, context)) throw new Exception();


            if (userPlc != null)
                BlueskyEnrichedApis.Instance.DangerousUnlockedRelationships.UserNotificationSubscribersThreadSafe.Subscribe(userPlc.Value, context.UserNotificationCallback);


            return Task.CompletedTask;
        }

        public override Task OnDisconnectedAsync(Exception? exception)
        {
            var dangerousRels = BlueskyEnrichedApis.Instance.DangerousUnlockedRelationships;
            if (connectionIdToCallback.TryRemove(this.Context.ConnectionId, out var context))
            {
                lock (context)
                {
                    foreach (var postId in context.PostIds)
                    {
                        dangerousRels.PostLiveSubscribersThreadSafe.Unsubscribe(postId, context.LiveUpdatesCallback);
                    }
                }
                if(context.UserPlc != null)
                    dangerousRels.UserNotificationSubscribersThreadSafe.Unsubscribe(context.UserPlc.Value, context.UserNotificationCallback);
            }

            return Task.CompletedTask;
        }


        private readonly static ConcurrentDictionary<string, ConnectionContext> connectionIdToCallback = new();
    }

    class ConnectionContext
    {
        public HashSet<PostId> PostIds = new();
        public LiveNotificationDelegate LiveUpdatesCallback;
        public Action<long>? UserNotificationCallback;
        public Plc? UserPlc;
    }
}
