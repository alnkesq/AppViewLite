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
        private readonly BlueskyEnrichedApis apis;
        private readonly RequestContext ctx;
        public AppBskyUnspecced(BlueskyEnrichedApis apis, RequestContext ctx)
        {
            this.apis = apis;
            this.ctx = ctx;
        }
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
        public async Task<IResult> GetPopularFeedGenerators(string? query, int limit, string? cursor)
        {
            var results = string.IsNullOrWhiteSpace(query) ? await apis.GetPopularFeedsAsync(cursor, limit, ctx) : await apis.SearchFeedsAsync(query, cursor, limit, ctx);
            return new GetPopularFeedGeneratorsOutput
            {
                Cursor = results.NextContinuation,
                Feeds = results.Feeds.Select(x => ApiCompatUtils.ToApiCompatGeneratorView(x)).ToList(),
            }.ToJsonResponse();
        }
    }
}

