using FishyFlip.Lexicon.App.Bsky.Unspecced;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Mvc;

namespace AppViewLite.Web.ApiCompat
{
    [Route("/xrpc")]
    [ApiController]
    [EnableCors("BskyClient")]
    public class AppBskyUnspecced : Controller
    {
        [HttpGet("app.bsky.unspecced.getConfig")]
        public IResult GetConfig()
        {
            return new GetConfigOutput
            {
            }.ToJsonResponse();
        }
        [HttpGet("app.bsky.unspecced.getTrendingTopics")]
        public IResult GetTrendingTopics()
        {
            return new GetTrendingTopicsOutput
            {
                Suggested = [],
                Topics = [],
            }.ToJsonResponse();
        }
        [HttpGet("app.bsky.unspecced.getPopularFeedGenerators")]
        public IResult GetPopularFeedGenerators(string? query, int limit)
        {
            return new GetPopularFeedGeneratorsOutput
            {
                Feeds = []
            }.ToJsonResponse();
        }
    }
}

