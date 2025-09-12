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
                if (_ctxWithoutConnectionId.SignalrConnectionId == null)
                {
                    _ctxWithoutConnectionId.SignalrConnectionId = this.Context.ConnectionId;
                }
                if (_ctxWithoutConnectionId.Session == null)
                {
                    var httpContext = this.Context.GetHttpContext();
                    var ctx = Program.TryGetSession(httpContext);
                    _ctxWithoutConnectionId.Session = ctx ?? AppViewLiteSession.CreateAnonymous();
                }

                return _ctxWithoutConnectionId;
            }
        }

        public void LoadPendingProfiles(ProfileRenderRequest[] requests)
        {
            if (apis.IsReadOnly) return;
            var hub = HubContext;

            var connectionId = Context.ConnectionId;
            var profiles = apis.WithRelationshipsLock(rels => requests.Select(x => rels.GetProfile(rels.SerializeDid(x.did, RequestContext))).ToArray(), RequestContext);
            apis.EnrichAsync(profiles, RequestContext, async p =>
            {
                var html = await Program.RenderComponentAsync<ProfileRow>(new Dictionary<string, object?>()
                {
                        { nameof(ProfileRow.Profile), p },
                        { nameof(ProfileRow.RequestContextOverride), RequestContext }
                });
                var index = Array.IndexOf(profiles, p);
                Program.AppViewLiteHubContext.Clients.Client(connectionId).SendAsync("ProfileRendered", requests[index].nodeId, html).FireAndForget();
            }).FireAndForget();

        }

        public void VerifyUncertainHandlesForDids(string[] dids)
        {
            if (apis.IsReadOnly) return;
            var ctx = RequestContext;

            var pairs = apis.WithRelationshipsLockForDids(dids, (plcs, rels) =>
            {
                return plcs.Select((plc, i) => (Did: dids[i], PossibleHandle: rels.TryGetLatestDidDoc(plc)?.Handle)).ToArray();
            }, ctx);
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
            BlueskyPost[] posts = null!;
            Plc? focalPlc = null;
            var ctx = this.RequestContext;
            apis.WithRelationshipsLockForDids(requests.Select(x => x.did).Concat(requests.Select(x => x.fromFeedDid).WhereNonNull()).Concat(requests.Select(x => x.repostedBy).WhereNonNull()).ToArray(), (_, rels) =>
            {
                posts = requests.Select(x =>
                {
                    var post = rels.GetPost(rels.GetPostId(x.did, x.rkey, ctx), ctx);
                    post.RepostedBy = x.repostedBy != null ? rels.GetProfile(rels.SerializeDid(x.repostedBy, ctx), ctx) : null;
                    post.RepostedByOrLikeRKey = x.repostedByRkey != null ? Tid.Parse(x.repostedByRkey) : default;
                    post.FromFeed = x.fromFeedDid != null ? rels.GetFeedGenerator(rels.SerializeDid(x.fromFeedDid, ctx), x.fromFeedRkey!, ctx) : null;
                    return post;
                }).ToArray();
                focalPlc = focalDid != null ? rels.SerializeDid(focalDid, ctx) : null;
            }, RequestContext);
            apis.EnrichAsync(posts, RequestContext, async p =>
            {
                await Task.Yield();
                var index = Array.IndexOf(posts, p);
                if (index == -1) return; // quoted post, will be handled separately
                apis.PopulateViewerFlags(new[] { p.Author, p.RepostedBy, p.QuotedPost?.Author }.WhereNonNull().ToArray(), RequestContext);
                var req = requests[index];
                p.ReplyChainLength = req.replyChainLength;
                p.IsKnownContinuationOfPreviousPost = req.isKnownContinuationOfPreviousPost;
                var html = await Program.RenderComponentAsync<PostRow>(PostRow.CreateParametersForRenderFlags(p, req.renderFlags, RequestContext));
                Program.AppViewLiteHubContext.Clients.Client(connectionId).SendAsync("PostRendered", req.nodeId, html).FireAndForget();
            }, sideWithQuotee: sideWithQuotee, focalPostAuthor: focalPlc).FireAndForget();

        }



        public void MarkAsRead(string did, string rkey, string? fromFeedDid, string? fromFeedRkey, string kind, float weight)
        {
            var ctx = HubContext;
            if (ctx.MarkAsReadThrottler == null) return;
            lock (ctx.PostEngagementPending)
            {
                ctx.PostEngagementPending.Add(new PostEngagementStr(new PostIdString(did, rkey), Enum.Parse<PostEngagementKind>(kind), fromFeedDid != null ? new RelationshipStr(fromFeedDid, fromFeedRkey!) : default,  weight));
            }
            ctx.MarkAsReadThrottler.Notify();

        }

        public Task SubscribeUnsubscribePosts(string[] toSubscribe, string[] toUnsubscribe)
        {
            var ctx = HubContext;

            var dangerousRels = apis.DangerousUnlockedRelationships;
            var toSubscribeParsed = toSubscribe.Select(x => PostIdString.Deserialize(x)).ToArray();
            var toUnsubscribeParsed = toUnsubscribe.Select(x => PostIdString.Deserialize(x)).ToArray();
            var (postIdsToSubscribe, postIdsToUnsubscribe) = apis.WithRelationshipsLockForDids(toSubscribeParsed.Concat(toUnsubscribeParsed).Select(x => x.Did).ToArray(), (_, rels) =>
            {
                return (
                    toSubscribeParsed.Select(x => new PostId(rels.SerializeDid(x.Did, RequestContext), Tid.Parse(x.RKey))).ToArray(),
                    toUnsubscribeParsed.Select(x => new PostId(rels.SerializeDid(x.Did, RequestContext), Tid.Parse(x.RKey))).ToArray());
            }, this.RequestContext);
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
            var ctx = RequestContext;
            Plc? userPlc = ctx.IsLoggedIn ? ctx.LoggedInUser : null;
            var connectionId = Context.ConnectionId;

            void SubmitLivePostEngagement(Versioned<PostStatsNotification> versionedNotification, Plc commitPlc)
            {
                var notification = versionedNotification.Value;
                var client = Program.AppViewLiteHubContext.Clients.Client(connectionId);

                object? ownRelationshipChange = null;
                if (commitPlc != default && commitPlc == userPlc)
                {
                    ctx.BumpMinimumVersion(versionedNotification.MinVersion);
                    apis.WithRelationshipsLock(rels =>
                    {
                        rels.Likes.HasActor(notification.PostId, commitPlc, out var userLike);
                        rels.Reposts.HasActor(notification.PostId, commitPlc, out var userRepost);
                        var bookmark = rels.TryGetLatestBookmarkForPost(notification.PostId, this.RequestContext.LoggedInUser);
                        ownRelationshipChange = new
                        {
                            likeRkey = userLike.RelationshipRKey.ToString() ?? "-",
                            repostRkey = userRepost.RelationshipRKey.ToString() ?? "-",
                            bookmarkRkey = bookmark?.ToString() ?? "-"
                        };
                    }, RequestContext.ToNonUrgent(ctx));

                }
                client.SendAsync("PostEngagementChanged", new { notification.Did, notification.RKey, notification.LikeCount, notification.RepostCount, notification.QuoteCount, notification.ReplyCount }, ownRelationshipChange).FireAndForget();
            }

            var throttler = new Throttler<Versioned<PostStatsNotification>>(TimeSpan.FromSeconds(4), (notif) => SubmitLivePostEngagement(notif, default));
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
            };
            if (!connectionIdToCallback.TryAdd(connectionId, context)) AssertionLiteException.Throw("connectionIdToCallback already contain an item for this key.");


            if (userPlc != null)
            {
                apis.DangerousUnlockedRelationships.UserNotificationSubscribersThreadSafe.Subscribe(userPlc.Value, context.UserNotificationCallback);

                context.MarkAsReadThrottler = new Throttler(TimeSpan.FromSeconds(1), () =>
                {
                    PostEngagementStr[] postEngagementPendingArray;
                    lock (context.PostEngagementPending)
                    {
                        postEngagementPendingArray = context.PostEngagementPending.ToArray();
                        context.PostEngagementPending.Clear();
                    }
                    apis.MarkAsRead(postEngagementPendingArray, userPlc.Value, RequestContext);
                });

                // In case notifications arrived while the websocket was broken or the browser tab was throttled or suspended
                var client = Program.AppViewLiteHubContext.Clients.Client(connectionId);
                var notificationCount = apis.GetNotificationCount(ctx.Session, ctx, false);
                client.SendAsync("NotificationCount", notificationCount).FireAndForget();
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
        public record struct PostRenderRequest(string nodeId, string did, string rkey, string renderFlags, string repostedBy, string repostedByRkey, int replyChainLength, string? fromFeedDid, string? fromFeedRkey, bool isKnownContinuationOfPreviousPost);
    }

    class ConnectionContext : IDisposable
    {
        public HashSet<PostId> PostIds = new();
        public required LiveNotificationDelegate LiveUpdatesCallback;
        public required Throttler<Versioned<PostStatsNotification>> LiveUpdatesCallbackThrottler;
        public Action<long>? UserNotificationCallback;
        public Plc? UserPlc;
        public Throttler? MarkAsReadThrottler;
        public List<PostEngagementStr> PostEngagementPending = new();
        public void Dispose()
        {
            LiveUpdatesCallbackThrottler.Dispose();
            MarkAsReadThrottler?.Dispose();
        }
    }
}
