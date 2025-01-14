using FishyFlip.Lexicon.App.Bsky.Feed;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace AppViewLite.Web
{
    [Route("/xrpc")]
    [ApiController]
    [EnableCors("BskyClient")]
    public class AppBskyGraph : ControllerBase
    {
        [HttpGet("app.bsky.graph.getFollowers")]
        public async Task<FishyFlip.Lexicon.App.Bsky.Graph.GetFollowersOutput> GetFollowers(string actor, string? cursor, int limit)
        {
            var deadline = EnrichDeadlineToken.Create();
            var subject = await BlueskyEnrichedApis.Instance.GetProfileAsync(actor, deadline);
            var (followers, nextContinuation) = await BlueskyEnrichedApis.Instance.GetFollowersAsync(actor, cursor, limit, deadline);
            
            return new FishyFlip.Lexicon.App.Bsky.Graph.GetFollowersOutput
            {
                Followers = followers.Select(x => x.ToApiCompatProfile()).ToList(),
                Cursor = nextContinuation,
                Subject = subject.ToApiCompatProfile()
            };
        }
    }
}

