using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Rendering;
using Microsoft.AspNetCore.Components.RenderTree;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.AspNetCore.SignalR;
using AppViewLite.Web.Components;
using AppViewLite.Models;
using AppViewLite.Numerics;
using System.Collections.Concurrent;

namespace AppViewLite.Web
{
    internal class AppViewLiteHub : Hub
    {
        private RequestContext ctx;
        private ILoggerFactory loggerFactory;

        public AppViewLiteHub(RequestContext ctx, ILoggerFactory loggerFactory)
        {
            this.ctx = ctx;
            this.loggerFactory = loggerFactory;
        }

        public async Task LoadPendingProfiles(ProfileRenderRequest[] requests)
        {
            var hub = HubContext;

            var connectionId = Context.ConnectionId;
            var profiles = BlueskyEnrichedApis.Instance.WithRelationshipsLock(rels => requests.Select(x => rels.GetProfile(rels.SerializeDid(x.did))).ToArray());
            var newctx = new RequestContext(ctx.Session, null, null);
            _ = BlueskyEnrichedApis.Instance.EnrichAsync(profiles, newctx, async p =>
            {
                using var scope = Program.StaticServiceProvider.CreateScope();
                using var renderer = new HtmlRenderer(scope.ServiceProvider, loggerFactory);
                var html = await renderer.Dispatcher.InvokeAsync(async () => (await renderer.RenderComponentAsync<ProfileRow>(ParameterView.FromDictionary(new Dictionary<string, object>() { { nameof(ProfileRow.Profile), p } }))).ToHtmlString());
                var index = Array.IndexOf(profiles, p);
                Program.AppViewLiteHubContext.Clients.Client(connectionId).SendAsync("ProfileRendered", requests[index].nodeId, html);
            });

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
            var newctx = new RequestContext(ctx.Session, null, null);
            _ = BlueskyEnrichedApis.Instance.EnrichAsync(posts, newctx, async p =>
            {
                var index = Array.IndexOf(posts, p);
                if (index == -1) return; // quoted post, will be handled separately
                using var scope = Program.StaticServiceProvider.CreateScope();
                using var renderer = new HtmlRenderer(scope.ServiceProvider, loggerFactory);
                var req = requests[index];
                var html = await renderer.Dispatcher.InvokeAsync(async () => (await renderer.RenderComponentAsync<PostRow>(PostRow.CreateParametersForRenderFlags(p, req.renderFlags))).ToHtmlString());
                Program.AppViewLiteHubContext.Clients.Client(connectionId).SendAsync("PostRendered", req.nodeId, html);
            }, sideWithQuotee: sideWithQuotee, focalPostAuthor: focalPlc);

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

        public record struct ProfileRenderRequest(string nodeId, string did);
        public record struct PostRenderRequest(string nodeId, string did, string rkey, string renderFlags);
    }

    class ConnectionContext
    {
        public HashSet<PostId> PostIds = new();
        public LiveNotificationDelegate LiveUpdatesCallback;
        public Action<long>? UserNotificationCallback;
        public Plc? UserPlc;
    }
}
