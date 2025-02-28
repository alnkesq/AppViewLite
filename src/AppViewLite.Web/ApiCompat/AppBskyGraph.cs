using FishyFlip.Lexicon.App.Bsky.Feed;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

#pragma warning disable CS1998

namespace AppViewLite.Web
{
    [Route("/xrpc")]
    [ApiController]
    [EnableCors("BskyClient")]
    public class AppBskyGraph : ControllerBase
    {
        private readonly BlueskyEnrichedApis apis;
        public AppBskyGraph(BlueskyEnrichedApis apis)
        {
            this.apis = apis;
        }

        [HttpGet("app.bsky.graph.getFollowers")]
        public async Task<FishyFlip.Lexicon.App.Bsky.Graph.GetFollowersOutput> GetFollowers(string actor, string? cursor, int limit)
        {
            var ctx = RequestContext.Create();
            var subject = await apis.GetProfileAsync(actor, ctx);
            var (followers, nextContinuation) = await apis.GetFollowersAsync(actor, cursor, limit, ctx);

            return new FishyFlip.Lexicon.App.Bsky.Graph.GetFollowersOutput
            {
                Followers = followers.Select(x => x.ToApiCompatProfile()).ToList(),
                Cursor = nextContinuation,
                Subject = subject.ToApiCompatProfile()
            };
        }
        [HttpGet("app.bsky.graph.getFollows")]
        public async Task<FishyFlip.Lexicon.App.Bsky.Graph.GetFollowsOutput> GetFollows(string actor, string? cursor, int limit)
        {
            var ctx = RequestContext.Create();
            var subject = await apis.GetProfileAsync(actor, ctx);
            var (follows, nextContinuation) = await apis.GetFollowingAsync(actor, cursor, limit, ctx);

            return new FishyFlip.Lexicon.App.Bsky.Graph.GetFollowsOutput
            {
                Follows = follows.Select(x => x.ToApiCompatProfile()).ToList(),
                Cursor = nextContinuation,
                Subject = subject.ToApiCompatProfile()
            };
        }
        [HttpGet("app.bsky.graph.getActorStarterPacks")]
        public async Task<FishyFlip.Lexicon.App.Bsky.Graph.GetActorStarterPacksOutput> GetActorStarterPacks(string actor, string? cursor, int limit)
        {
            return new FishyFlip.Lexicon.App.Bsky.Graph.GetActorStarterPacksOutput
            {
                StarterPacks = []
            };
        }
    }
}

