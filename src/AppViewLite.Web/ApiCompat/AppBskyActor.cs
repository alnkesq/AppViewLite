using FishyFlip.Lexicon.App.Bsky.Actor;
using FishyFlip.Lexicon.App.Bsky.Feed;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace AppViewLite.Web
{
    [Route("/xrpc")]
    [ApiController]
    [EnableCors("BskyClient")]
    public class AppBskyActor : ControllerBase
    {
        private readonly BlueskyEnrichedApis apis;
        private readonly RequestContext ctx;
        public AppBskyActor(BlueskyEnrichedApis apis, RequestContext ctx)
        {
            this.apis = apis;
            this.ctx = ctx;
        }

        [HttpGet("app.bsky.actor.getProfile")]
        public async Task<IResult> GetProfile(string actor)
        {
            var profile = await apis.GetFullProfileAsync(actor, ctx, 0);

            return profile.ToApiCompatProfileDetailed().ToJsonResponse();
        }

        [HttpGet("app.bsky.actor.getProfiles")]
        public async Task<IResult> GetProfiles([FromQuery]string[] actors)
        {
            var profiles = await Task.WhenAll(actors.Select(did => apis.GetFullProfileAsync(did, ctx, 0)));
            return new GetProfilesOutput(profiles.Select(p => p.ToApiCompatProfileDetailed()).ToList()).ToJsonResponse();
        }

        [HttpGet("app.bsky.actor.searchActorsTypeahead")]
        public Task<IResult> SearchActorsTypeahead(string q, int limit)
        {
            return Task.FromResult(new SearchActorsTypeaheadOutput
            {
                Actors = []
            }.ToJsonResponse());
        }

        [HttpGet("app.bsky.actor.searchActors")]
        public Task<IResult> SearchActors(string q, int limit, string? cursor)
        {
            return Task.FromResult(new SearchActorsOutput
            {
                Actors = []
            }.ToJsonResponse());
        }

        [HttpGet("app.bsky.actor.getPreferences")]
        public Task<IResult> GetPreferences()
        {
            return Task.FromResult(new GetPreferencesOutput
            {
                Preferences = [
                    new SavedFeedsPrefV2 { Items = [new SavedFeed("3lemacgq3ne2v", "timeline", "following", pinned: true)] },
                    new AdultContentPref { Enabled = true }
                ]
            }.ToJsonResponse());
        }

        [HttpPost("app.bsky.actor.putPreferences")]
        public object PutPreferences(PutPreferencesInput preferences)
        {
            return new object();
        }
    }
}

