using Microsoft.AspNetCore.SignalR;
using AppViewLite.Web.Components;
using AppViewLite.Models;
using AppViewLite.Numerics;
using System.Collections.Concurrent;

namespace AppViewLite.Web
{
    internal class AppViewLiteHub : Hub
    {
        private readonly RequestContext _ctxWithoutConnectionId;
        private readonly BlueskyEnrichedApis apis;
        public AppViewLiteHub(RequestContext ctx, BlueskyEnrichedApis apis)
        {
            this._ctxWithoutConnectionId = ctx;
            this.apis = apis;
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
        
        public void LoadPendingProfiles(ProfileRenderRequest[] requests)
        {
            if (apis.IsReadOnly) return;
            var hub = HubContext;

            var connectionId = Context.ConnectionId;
            var profiles = apis.WithRelationshipsLock(rels => requests.Select(x => rels.GetProfile(rels.SerializeDid(x.did))).ToArray());
            var newctx = new RequestContext(RequestContext.Session, null, null, connectionId);
            apis.EnrichAsync(profiles, newctx, async p =>
            {
                var html = await Program.RenderComponentAsync<ProfileRow>(new Dictionary<string, object>() { { nameof(ProfileRow.Profile), p } });
                var index = Array.IndexOf(profiles, p);
                Program.AppViewLiteHubContext.Clients.Client(connectionId).SendAsync("ProfileRendered", requests[index].nodeId, html);
            }).FireAndForget();

        }

        public void VerifyUncertainHandlesForDids(string[] dids)
        {
            if (apis.IsReadOnly) return;
            var ctx = RequestContext;
            
            var pairs = apis.WithRelationshipsLockForDids(dids, (plcs, rels) =>
            {
                return plcs.Select((plc, i) => (Did: dids[i], PossibleHandle: rels.TryGetLatestDidDoc(plc)?.Handle)).ToArray();
            });
            foreach (var pair in pairs)
            {
                apis.VerifyHandleAndNotifyAsync(pair.Did, pair.PossibleHandle, ctx).FireAndForget();
            }
        }

        public void LoadPendingPosts(PostRenderRequest[] requests, bool sideWithQuotee, string? focalDid)
        {
            var hub = HubContext;
            if (apis.IsReadOnly) return;

            var connectionId = Context.ConnectionId;
            BlueskyPost[]? posts = null;
            Plc? focalPlc = null;
            apis.WithRelationshipsLock(rels =>
            {
                posts = requests.Select(x =>
                {
                    var post = rels.GetPost(rels.GetPostId(x.did, x.rkey));
                    post.RepostedBy = x.repostedBy != null ? rels.GetProfile(rels.SerializeDid(x.repostedBy)) : null;
                    return post;
                }).ToArray();
                focalPlc = focalDid != null ? rels.SerializeDid(focalDid) : null;
            });
            var newctx = new RequestContext(RequestContext.Session, null, null, connectionId);
            apis.EnrichAsync(posts, newctx, async p =>
            {
                var index = Array.IndexOf(posts, p);
                if (index == -1) return; // quoted post, will be handled separately
                apis.PopulateViewerFlags(new[] { p.Author, p.RepostedBy }.Where(x => x != null).ToArray(), newctx);
                var req = requests[index];
                var html = await Program.RenderComponentAsync<PostRow>(PostRow.CreateParametersForRenderFlags(p, req.renderFlags));
                Program.AppViewLiteHubContext.Clients.Client(connectionId).SendAsync("PostRendered", req.nodeId, html);
            }, sideWithQuotee: sideWithQuotee, focalPostAuthor: focalPlc).FireAndForget();

        }



        public void MarkAsRead(string did, string rkey)
        {
            var ctx = HubContext;
            if (ctx.MarkAsReadThrottler == null) return;
            lock (ctx.MarkAsReadPending)
            {
                ctx.MarkAsReadPending.Add(new PostIdString(did, rkey));
            }
            ctx.MarkAsReadThrottler.Notify();

        }

        public Task SubscribeUnsubscribePosts(string[] toSubscribe, string[] toUnsubscribe)
        {
            var ctx = HubContext;

            var dangerousRels = apis.DangerousUnlockedRelationships;
            var (postIdsToSubscribe, postIdsToUnsubscribe) = apis.WithRelationshipsUpgradableLock(rels =>
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
                    apis.WithRelationshipsLock(rels => 
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
            { 
                apis.DangerousUnlockedRelationships.UserNotificationSubscribersThreadSafe.Subscribe(userPlc.Value, context.UserNotificationCallback);

                context.MarkAsReadThrottler = new Throttler(TimeSpan.FromSeconds(1), () =>
                {
                    PostIdString[] markAsReadPendingArray;
                    lock (context.MarkAsReadPending)
                    {
                        markAsReadPendingArray = context.MarkAsReadPending.ToArray();
                        context.MarkAsReadPending.Clear();
                    }
                    apis.MarkAsRead(markAsReadPendingArray, userPlc.Value);
                });
            }
            return Task.CompletedTask;
        }

        public override Task OnDisconnectedAsync(Exception? exception)
        {
            var dangerousRels = apis.DangerousUnlockedRelationships;
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
        public record struct PostRenderRequest(string nodeId, string did, string rkey, string renderFlags, string repostedBy);
    }

    class ConnectionContext : IDisposable
    {
        public HashSet<PostId> PostIds = new();
        public LiveNotificationDelegate LiveUpdatesCallback;
        public Throttler<PostStatsNotification> LiveUpdatesCallbackThrottler;
        public Action<long>? UserNotificationCallback;
        public Plc? UserPlc;
        public string? SessionCookie;
        public Throttler? MarkAsReadThrottler;
        public List<PostIdString> MarkAsReadPending = new();
        public void Dispose()
        {
            LiveUpdatesCallbackThrottler.Dispose();
            MarkAsReadThrottler?.Dispose();
        }
    }
}
