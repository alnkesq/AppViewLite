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
        [HttpGet("app.bsky.actor.getProfile")]
        public async Task<FishyFlip.Lexicon.App.Bsky.Actor.ProfileViewDetailed> GetProfile(string actor)
        {
            var profile = await BlueskyEnrichedApis.Instance.GetFullProfileAsync(actor, RequestContext.Create(), 0);

            return profile.ToApiCompatDetailed();
        }

        [HttpGet("app.bsky.actor.searchActorsTypeahead")]
        public Task<SearchActorsTypeaheadOutput> SearchActorsTypeahead(string q, int limit)
        {
            return Task.FromResult(new SearchActorsTypeaheadOutput 
            { 
                Actors = []
            });
        }
    }
}

