using FishyFlip.Lexicon.App.Bsky.Unspecced;
using FishyFlip.Models;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Mvc;

namespace AppViewLite.Web.ApiCompat
{
    [ApiController]
    [EnableCors("BskyClient")]
    public class AppBskyUnspecced : FishyFlip.Xrpc.Lexicon.App.Bsky.Unspecced.UnspeccedController
    {
        private readonly BlueskyEnrichedApis apis;
        private readonly RequestContext ctx;
        public AppBskyUnspecced(BlueskyEnrichedApis apis, RequestContext ctx)
        {
            this.apis = apis;
            this.ctx = ctx;
        }

        public override Task<Results<Ok<GetConfigOutput>, ATErrorResult>> GetConfigAsync(CancellationToken cancellationToken = default)
        {
            return new GetConfigOutput
            {
            }.ToJsonResultOkTask();
        }

        public async override Task<Results<Ok<GetPopularFeedGeneratorsOutput>, ATErrorResult>> GetPopularFeedGeneratorsAsync([FromQuery] int? limit = 50, [FromQuery] string? cursor = null, [FromQuery] string? query = null, CancellationToken cancellationToken = default)
        {
            var results = string.IsNullOrWhiteSpace(query) ? await apis.GetPopularFeedsAsync(cursor, limit ?? default, ctx) : await apis.SearchFeedsAsync(query, cursor, limit ?? default, ctx);
            return new GetPopularFeedGeneratorsOutput
            {
                Cursor = results.NextContinuation,
                Feeds = results.Feeds.Select(x => ApiCompatUtils.ToApiCompatGeneratorView(x)).ToList(),
            }.ToJsonResultOk();
        }

        public override Task<Results<Ok<GetSuggestedFeedsOutput>, ATErrorResult>> GetSuggestedFeedsAsync([FromQuery] int? limit = 10, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok<GetSuggestedFeedsSkeletonOutput>, ATErrorResult>> GetSuggestedFeedsSkeletonAsync([FromQuery] ATDid? viewer = null, [FromQuery] int? limit = 10, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok<GetSuggestedStarterPacksOutput>, ATErrorResult>> GetSuggestedStarterPacksAsync([FromQuery] int? limit = 10, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok<GetSuggestedStarterPacksSkeletonOutput>, ATErrorResult>> GetSuggestedStarterPacksSkeletonAsync([FromQuery] ATDid? viewer = null, [FromQuery] int? limit = 10, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok<GetSuggestedUsersOutput>, ATErrorResult>> GetSuggestedUsersAsync([FromQuery] string? category = null, [FromQuery] int? limit = 25, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok<GetSuggestedUsersSkeletonOutput>, ATErrorResult>> GetSuggestedUsersSkeletonAsync([FromQuery] ATDid? viewer = null, [FromQuery] string? category = null, [FromQuery] int? limit = 25, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok<GetSuggestionsSkeletonOutput>, ATErrorResult>> GetSuggestionsSkeletonAsync([FromQuery] ATDid? viewer = null, [FromQuery] int? limit = 50, [FromQuery] string? cursor = null, [FromQuery] ATDid? relativeToDid = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok<GetTaggedSuggestionsOutput>, ATErrorResult>> GetTaggedSuggestionsAsync(CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok<GetTrendingTopicsOutput>, ATErrorResult>> GetTrendingTopicsAsync([FromQuery] ATDid? viewer = null, [FromQuery] int? limit = 10, CancellationToken cancellationToken = default)
        {
            return new GetTrendingTopicsOutput
            {
                Suggested = [],
                Topics = [],
            }.ToJsonResultOkTask();
        }

        public override Task<Results<Ok<GetTrendsOutput>, ATErrorResult>> GetTrendsAsync([FromQuery] int? limit = 10, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok<GetTrendsSkeletonOutput>, ATErrorResult>> GetTrendsSkeletonAsync([FromQuery] ATDid? viewer = null, [FromQuery] int? limit = 10, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok<SearchActorsSkeletonOutput>, ATErrorResult>> SearchActorsSkeletonAsync([FromQuery] string q, [FromQuery] ATDid? viewer = null, [FromQuery] bool? typeahead = null, [FromQuery] int? limit = 25, [FromQuery] string? cursor = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok<SearchPostsSkeletonOutput>, ATErrorResult>> SearchPostsSkeletonAsync([FromQuery] string q, [FromQuery] string? sort = null, [FromQuery] string? since = null, [FromQuery] string? until = null, [FromQuery] ATIdentifier? mentions = null, [FromQuery] ATIdentifier? author = null, [FromQuery] string? lang = null, [FromQuery] string? domain = null, [FromQuery] string? url = null, [FromQuery] List<string>? tag = null, [FromQuery] ATDid? viewer = null, [FromQuery] int? limit = 25, [FromQuery] string? cursor = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok<SearchStarterPacksSkeletonOutput>, ATErrorResult>> SearchStarterPacksSkeletonAsync([FromQuery] string q, [FromQuery] ATDid? viewer = null, [FromQuery] int? limit = 25, [FromQuery] string? cursor = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }
    }
}

