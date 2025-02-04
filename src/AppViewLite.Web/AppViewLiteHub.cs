using Microsoft.AspNetCore.SignalR;
using AppViewLite.Web.Components;
using AppViewLite.Models;
using AppViewLite.Numerics;
using System.Collections.Concurrent;

namespace AppViewLite.Web
{
    internal class AppViewLiteHub : Hub
    {
        private RequestContext _ctxWithoutConnectionId;

        public AppViewLiteHub(RequestContext ctx)
        {
            this._ctxWithoutConnectionId = ctx;
        }

        public RequestContext RequestContext
        {
            get
            {
                _ctxWithoutConnectionId.SignalrConnectionId = this.Context.ConnectionId;
                _ctxWithoutConnectionId.Session = Program.TryGetSessionFromCookie(this.HubContext.SessionCookie);
                return _ctxWithoutConnectionId;
            }
        }
        
        public async Task LoadPendingProfiles(ProfileRenderRequest[] requests)
        {
            var hub = HubContext;

            var connectionId = Context.ConnectionId;
            var profiles = BlueskyEnrichedApis.Instance.WithRelationshipsLock(rels => requests.Select(x => rels.GetProfile(rels.SerializeDid(x.did))).ToArray());
            var newctx = new RequestContext(RequestContext.Session, null, null, connectionId);
            BlueskyEnrichedApis.Instance.EnrichAsync(profiles, newctx, async p =>
            {
                var html = await Program.RenderComponentAsync<ProfileRow>(new Dictionary<string, object>() { { nameof(ProfileRow.Profile), p } });
                var index = Array.IndexOf(profiles, p);
                Program.AppViewLiteHubContext.Clients.Client(connectionId).SendAsync("ProfileRendered", requests[index].nodeId, html);
            }).FireAndForget();

        }

        public void VerifyUncertainHandlesForDids(string[] dids)
        {
            var ctx = RequestContext;
            var pairs = BlueskyEnrichedApis.Instance.WithRelationshipsLock(rels =>
            {
                return dids.Select(did => (Did: did, PossibleHandle: rels.TryGetLatestDidDoc(rels.SerializeDid(did))?.Handle)).ToArray();
            });
            foreach (var pair in pairs)
            {
                if (pair.PossibleHandle != null)
                {
                    BlueskyEnrichedApis.Instance.VerifyHandleAndNotifyAsync(pair.Did, pair.PossibleHandle, ctx);
                }

            }
        }

        public async Task LoadPendingPosts(PostRenderRequest[] requests, bool sideWithQuotee, string? focalDid)
        {
            var hub = HubContext;

            var connectionId = Context.ConnectionId;
            BlueskyPost[]? posts = null;
            Plc? focalPlc = null;
            BlueskyEnrichedApis.Instance.WithRelationshipsLock(rels =>
            {
                posts = requests.Select(x => rels.GetPost(rels.GetPostId(x.did, x.rkey))).ToArray();
                focalPlc = focalDid != null ? rels.SerializeDid(focalDid) : null;
            });
            var newctx = new RequestContext(RequestContext.Session, null, null, connectionId);
            BlueskyEnrichedApis.Instance.EnrichAsync(posts, newctx, async p =>
            {
                var index = Array.IndexOf(posts, p);
                if (index == -1) return; // quoted post, will be handled separately
                var req = requests[index];
                var html = await Program.RenderComponentAsync<PostRow>(PostRow.CreateParametersForRenderFlags(p, req.renderFlags));
                Program.AppViewLiteHubContext.Clients.Client(connectionId).SendAsync("PostRendered", req.nodeId, html);
            }, sideWithQuotee: sideWithQuotee, focalPostAuthor: focalPlc).FireAndForget();

        }

        public Task SubscribeUnsubscribePosts(string[] toSubscribe, string[] toUnsubscribe)
        {
            var ctx = HubContext;

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

        private ConnectionContext HubContext => connectionIdToCallback[Context.ConnectionId];
        
        public override Task OnConnectedAsync()
        {
            var httpContext = this.Context.GetHttpContext();
            var ctx = Program.TryGetSession(httpContext);
            var userPlc = ctx != null && ctx.IsLoggedIn ? ctx.LoggedInUser : default;
            var connectionId = Context.ConnectionId;

            void SubmitLivePostEngagement(PostStatsNotification notification, Plc commitPlc)
            {
                var client = Program.AppViewLiteHubContext.Clients.Client(connectionId);

                object? ownRelationshipChange = null;
                if (commitPlc != default && commitPlc == userPlc)
                {
                    BlueskyEnrichedApis.Instance.WithRelationshipsLock(rels => 
                    {
                        rels.Likes.HasActor(notification.PostId, commitPlc, out var userLike);
                        rels.Reposts.HasActor(notification.PostId, commitPlc, out var userRepost);
                        ownRelationshipChange = new
                        {
                            likeRkey = userLike.RelationshipRKey.ToString() ?? "-",
                            repostRkey = userRepost.RelationshipRKey.ToString() ?? "-",
                        };
                    });

                }
                client.SendAsync("PostEngagementChanged", new { notification.Did, notification.RKey, notification.LikeCount, notification.RepostCount, notification.QuoteCount, notification.ReplyCount }, ownRelationshipChange).FireAndForget();
            }

            var throttler = new Throttler<PostStatsNotification>(TimeSpan.FromSeconds(4), (notif) => SubmitLivePostEngagement(notif, default));
            var context = new ConnectionContext
            {
                UserPlc = userPlc,
                LiveUpdatesCallbackThrottler = throttler,
                LiveUpdatesCallback = (notification, commitPlc) => 
                {
                    if (commitPlc == userPlc) SubmitLivePostEngagement(notification, commitPlc); // send immediately, we don't want to lose the ownRelationshipChange
                    else
                        throttler.Notify(notification);
                },
                UserNotificationCallback = (notificationCount) =>
                {
                    var client = Program.AppViewLiteHubContext.Clients.Client(connectionId);

                    client.SendAsync("NotificationCount", notificationCount).FireAndForget();
                },
                SessionCookie = Program.TryGetSessionCookie(httpContext)
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
                using (context)
                {
                    lock (context)
                    {
                        foreach (var postId in context.PostIds)
                        {
                            dangerousRels.PostLiveSubscribersThreadSafe.Unsubscribe(postId, context.LiveUpdatesCallback);
                        }
                    }
                    if (context.UserPlc != null)
                        dangerousRels.UserNotificationSubscribersThreadSafe.Unsubscribe(context.UserPlc.Value, context.UserNotificationCallback);

                }


            }

            return Task.CompletedTask;
        }


        private readonly static ConcurrentDictionary<string, ConnectionContext> connectionIdToCallback = new();

        public record struct ProfileRenderRequest(string nodeId, string did);
        public record struct PostRenderRequest(string nodeId, string did, string rkey, string renderFlags);
    }

    class ConnectionContext : IDisposable
    {
        public HashSet<PostId> PostIds = new();
        public LiveNotificationDelegate LiveUpdatesCallback;
        public Throttler<PostStatsNotification> LiveUpdatesCallbackThrottler;
        public Action<long>? UserNotificationCallback;
        public Plc? UserPlc;
        public string? SessionCookie;

        public void Dispose()
        {
            LiveUpdatesCallbackThrottler.Dispose();
        }
    }
}
