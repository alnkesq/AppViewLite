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
                    dangerousRels.RegisterForPostStatsNotificationThreadSafe(postId, ctx.Callback);
                    ctx.PostIds.Add(postId);
                }
                foreach (var postId in postIdsToUnsubscribe)
                {
                    dangerousRels.UnregisterForPostStatsNotificationThreadSafe(postId, ctx.Callback);
                    ctx.PostIds.Remove(postId);
                }
            }
            return Task.CompletedTask;
        }

        public override Task OnConnectedAsync()
        {
            var httpContext = this.Context.GetHttpContext();
            var ctx = Program.TryGetSession(httpContext);
            var userPlc = ctx.IsLoggedIn ? ctx.LoggedInUser : default;
            var connectionId = Context.ConnectionId;
            if (!connectionIdToCallback.TryAdd(connectionId, new ConnectionContext 
            { 
                Callback = (notification, commitPlc) => 
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
                    _ = client.SendAsync("PostEngagementChanged", notification, ownRelationshipChange);
                }
            })) throw new Exception();
            return Task.CompletedTask;
        }

        public override Task OnDisconnectedAsync(Exception? exception)
        {

            if (connectionIdToCallback.TryRemove(this.Context.ConnectionId, out var context))
            {
                foreach (var postId in context.PostIds)
                {
                    BlueskyEnrichedApis.Instance.DangerousUnlockedRelationships.UnregisterForPostStatsNotificationThreadSafe(postId, context.Callback);
                }
            }
            return Task.CompletedTask;
        }


        private readonly static ConcurrentDictionary<string, ConnectionContext> connectionIdToCallback = new();
    }

    class ConnectionContext
    {
        public HashSet<PostId> PostIds = new();
        public LiveNotificationDelegate Callback;
    }
}
