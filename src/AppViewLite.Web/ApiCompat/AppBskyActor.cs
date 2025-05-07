using AppViewLite.Models;
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
        public async Task<IResult> GetProfiles([FromQuery] string[] actors)
        {
            if (actors.Length == 0) return new GetProfilesOutput { Profiles = [] }.ToJsonResponse();

            // TODO: where is getProfiles used?

            if (actors.Length == 1) return new GetProfilesOutput { Profiles = [ApiCompatUtils.ToApiCompatProfileDetailed(await apis.GetFullProfileAsync(actors[0], ctx, 0))] }.ToJsonResponse();
            var profiles = apis.WithRelationshipsLockForDids(actors, (plcs, rels) =>
            {
                return plcs.Select(x =>
                {
                    var p = rels.GetProfile(x, ctx);
                    return new BlueskyFullProfile
                    {
                        Profile = p,
                    };
                }).ToArray();
            }, ctx);
            await apis.EnrichAsync(profiles.Select(x => x.Profile).ToArray(), ctx);
            return new GetProfilesOutput
            {

                Profiles = profiles.Select(x => ApiCompatUtils.ToApiCompatProfileDetailed(x)).ToList(),
            }.ToJsonResponse();
        }

        [HttpGet("app.bsky.actor.searchActorsTypeahead")]
        public async Task<IResult> SearchActorsTypeahead(string q, int limit)
        {
            var results = await apis.SearchProfilesAsync(q, allowPrefixForLastWord: true, null, limit, ctx);
            return new SearchActorsTypeaheadOutput
            {
                Actors = results.Profiles.Select(x => ApiCompatUtils.ToApiCompatProfileViewBasic(x)).ToList(),
            }.ToJsonResponse();
        }

        [HttpGet("app.bsky.actor.searchActors")]
        public async Task<IResult> SearchActors(string q, int limit, string? cursor)
        {
            var results = await apis.SearchProfilesAsync(q, allowPrefixForLastWord: false, cursor, limit, ctx);
            return new SearchActorsOutput
            {
                Cursor = results.NextContinuation,
                Actors = results.Profiles.Select(x => ApiCompatUtils.ToApiCompatProfileView(x)).ToList(),
            }.ToJsonResponse();
        }

        [HttpGet("app.bsky.actor.getSuggestions")]
        public Task<IResult> GetSuggestions(int limit, string? cursor)
        {
            return Task.FromResult(new GetSuggestionsOutput
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

