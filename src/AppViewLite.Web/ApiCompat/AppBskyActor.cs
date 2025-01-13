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
            var profile = await BlueskyEnrichedApis.Instance.GetFullProfileAsync(actor, EnrichDeadlineToken.Create());

            return profile.ToApiCompatDetailed();
        }
    }
}

