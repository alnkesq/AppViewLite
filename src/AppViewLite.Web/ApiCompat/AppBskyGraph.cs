using AppViewLite.Numerics;
using FishyFlip.Lexicon.App.Bsky.Feed;
using FishyFlip.Models;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using AppViewLite;

#pragma warning disable CS1998

namespace AppViewLite.Web
{
    [Route("/xrpc")]
    [ApiController]
    [EnableCors("BskyClient")]
    public class AppBskyGraph : ControllerBase
    {
        private readonly BlueskyEnrichedApis apis;
        private readonly RequestContext ctx;
        public AppBskyGraph(BlueskyEnrichedApis apis, RequestContext ctx)
        {
            this.apis = apis;
            this.ctx = ctx;
        }

        [HttpGet("app.bsky.graph.getFollowers")]
        public async Task<IResult> GetFollowers(string actor, string? cursor, int limit)
        {
            var subject = await apis.GetProfileAsync(actor, ctx);
            var (followers, nextContinuation) = await apis.GetFollowersAsync(actor, cursor, limit, ctx);

            return new FishyFlip.Lexicon.App.Bsky.Graph.GetFollowersOutput
            {
                Followers = followers.Select(x => x.ToApiCompatProfile()).ToList(),
                Cursor = nextContinuation,
                Subject = subject.ToApiCompatProfile()
            }.ToJsonResponse();
        }
        [HttpGet("app.bsky.graph.getFollows")]
        public async Task<IResult> GetFollows(string actor, string? cursor, int limit)
        {
            var subject = await apis.GetProfileAsync(actor, ctx);
            var (follows, nextContinuation) = await apis.GetFollowingAsync(actor, cursor, limit, ctx);

            return new FishyFlip.Lexicon.App.Bsky.Graph.GetFollowsOutput
            {
                Follows = follows.Select(x => x.ToApiCompatProfile()).ToList(),
                Cursor = nextContinuation,
                Subject = subject.ToApiCompatProfile()
            }.ToJsonResponse();
        }
        [HttpGet("app.bsky.graph.getActorStarterPacks")]
        public async Task<IResult> GetActorStarterPacks(string actor, string? cursor, int limit)
        {
            return new FishyFlip.Lexicon.App.Bsky.Graph.GetActorStarterPacksOutput
            {
                StarterPacks = []
            }.ToJsonResponse();
        }
        [HttpGet("app.bsky.graph.getLists")]
        public async Task<IResult> GetLists(string actor, string? cursor, int limit)
        {
            var lists = await apis.GetProfileListsAsync(actor, cursor, limit, ctx);

            return new FishyFlip.Lexicon.App.Bsky.Graph.GetListsOutput
            {
                Lists = lists.Lists.Select(x => ApiCompatUtils.ToApiCompactListView(x)).ToList(),
                Cursor = lists.NextContinuation,
            }.ToJsonResponse();
        }
        [HttpGet("app.bsky.graph.getList")]
        public async Task<IResult> GetList(string list, string? cursor, int limit)
        {
            var aturi = new ATUri(list);
            var info = await apis.GetListMetadataAsync(aturi.Did!.Handler, aturi.Rkey, ctx);
            var members = await apis.GetListMembersAsync(info.ModeratorDid!, info.RKey, cursor, limit, ctx);

            return new FishyFlip.Lexicon.App.Bsky.Graph.GetListOutput
            {
                List = ApiCompatUtils.ToApiCompactListView(info),
                Items = members.Page.Select(x => ApiCompatUtils.ToApiCompatToListItemView(x, aturi.Did.Handler)).ToList(),
                Cursor = members.NextContinuation,
            }.ToJsonResponse();
        }
        [HttpGet("app.bsky.graph.getSuggestedFollowsByActor")]
        public async Task<IResult> GetSuggestedFollowsByActor(string actor)
        {
            return new FishyFlip.Lexicon.App.Bsky.Graph.GetSuggestedFollowsByActorOutput
            {
                Suggestions = []
            }.ToJsonResponse();
        }
    }
}

