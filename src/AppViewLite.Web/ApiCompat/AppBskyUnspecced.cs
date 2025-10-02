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

        public override Task<Results<ATResult<AgeAssuranceState>, ATErrorResult>> GetAgeAssuranceStateAsync(CancellationToken cancellationToken = default)
        {
            return new AgeAssuranceState { Status = "assured", LastInitiatedAt = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc) }.ToJsonResultOkTask();
        }

        public override Task<Results<ATResult<GetConfigOutput>, ATErrorResult>> GetConfigAsync(CancellationToken cancellationToken = default)
        {
            return new GetConfigOutput
            {
            }.ToJsonResultOkTask();
        }

        public async override Task<Results<ATResult<GetPopularFeedGeneratorsOutput>, ATErrorResult>> GetPopularFeedGeneratorsAsync([FromQuery] int? limit = 50, [FromQuery] string? cursor = null, [FromQuery] string? query = null, CancellationToken cancellationToken = default)
        {
            var results = string.IsNullOrWhiteSpace(query) ? await apis.GetPopularFeedsAsync(cursor, limit ?? default, ctx) : await apis.SearchFeedsAsync(query, cursor, limit ?? default, ctx);
            return new GetPopularFeedGeneratorsOutput
            {
                Cursor = results.NextContinuation,
                Feeds = results.Feeds.Select(x => ApiCompatUtils.ToApiCompatGeneratorView(x)).ToList(),
            }.ToJsonResultOk();
        }

        public override Task<Results<ATResult<GetPostThreadOtherV2Output>, ATErrorResult>> GetPostThreadOtherV2Async([FromQuery] ATUri anchor, [FromQuery] bool? prioritizeFollowedUsers = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<ATResult<GetPostThreadV2Output>, ATErrorResult>> GetPostThreadV2Async([FromQuery] ATUri anchor, [FromQuery] bool? above = null, [FromQuery] int? below = 6, [FromQuery] int? branchingFactor = 10, [FromQuery] bool? prioritizeFollowedUsers = null, [FromQuery] string? sort = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<ATResult<GetSuggestedFeedsOutput>, ATErrorResult>> GetSuggestedFeedsAsync([FromQuery] int? limit = 10, CancellationToken cancellationToken = default)
        {
            return new GetSuggestedFeedsOutput
            {
                Feeds = []
            }.ToJsonResultOkTask();
        }

        public override Task<Results<ATResult<GetSuggestedFeedsSkeletonOutput>, ATErrorResult>> GetSuggestedFeedsSkeletonAsync([FromQuery] ATDid? viewer = null, [FromQuery] int? limit = 10, CancellationToken cancellationToken = default)
        {
            return new GetSuggestedFeedsSkeletonOutput
            {
                Feeds = []
            }.ToJsonResultOkTask();
        }

        public override Task<Results<ATResult<GetSuggestedStarterPacksOutput>, ATErrorResult>> GetSuggestedStarterPacksAsync([FromQuery] int? limit = 10, CancellationToken cancellationToken = default)
        {
            return new GetSuggestedStarterPacksOutput
            {
                StarterPacks = []
            }.ToJsonResultOkTask();
        }

        public override Task<Results<ATResult<GetSuggestedStarterPacksSkeletonOutput>, ATErrorResult>> GetSuggestedStarterPacksSkeletonAsync([FromQuery] ATDid? viewer = null, [FromQuery] int? limit = 10, CancellationToken cancellationToken = default)
        {
            return new GetSuggestedStarterPacksSkeletonOutput 
            {
                StarterPacks = []
            }
            .ToJsonResultOkTask();
        }

        public override Task<Results<ATResult<GetSuggestedUsersOutput>, ATErrorResult>> GetSuggestedUsersAsync([FromQuery] string? category = null, [FromQuery] int? limit = 25, CancellationToken cancellationToken = default)
        {
            return new GetSuggestedUsersOutput
            {
                Actors = []
            }.ToJsonResultOkTask();
        }

        public override Task<Results<ATResult<GetSuggestedUsersSkeletonOutput>, ATErrorResult>> GetSuggestedUsersSkeletonAsync([FromQuery] ATDid? viewer = null, [FromQuery] string? category = null, [FromQuery] int? limit = 25, CancellationToken cancellationToken = default)
        {
            return new GetSuggestedUsersSkeletonOutput
            {
                Dids = [],
            }.ToJsonResultOkTask();
        }

        public override Task<Results<ATResult<GetSuggestionsSkeletonOutput>, ATErrorResult>> GetSuggestionsSkeletonAsync([FromQuery] ATDid? viewer = null, [FromQuery] int? limit = 50, [FromQuery] string? cursor = null, [FromQuery] ATDid? relativeToDid = null, CancellationToken cancellationToken = default)
        {
            return new GetSuggestionsSkeletonOutput
            {
                Actors = [],
            }.ToJsonResultOkTask();
        }

        public override Task<Results<ATResult<GetTaggedSuggestionsOutput>, ATErrorResult>> GetTaggedSuggestionsAsync(CancellationToken cancellationToken = default)
        {
            return new GetTaggedSuggestionsOutput
            {
                Suggestions = [],
            }.ToJsonResultOkTask();
        }

        public override Task<Results<ATResult<GetTrendingTopicsOutput>, ATErrorResult>> GetTrendingTopicsAsync([FromQuery] ATDid? viewer = null, [FromQuery] int? limit = 10, CancellationToken cancellationToken = default)
        {
            return new GetTrendingTopicsOutput
            {
                Suggested = [],
                Topics = [],
            }.ToJsonResultOkTask();
        }

        public override Task<Results<ATResult<GetTrendsOutput>, ATErrorResult>> GetTrendsAsync([FromQuery] int? limit = 10, CancellationToken cancellationToken = default)
        {
            return new GetTrendsOutput
            {
                Trends = []
            }.ToJsonResultOkTask();
        }

        public override Task<Results<ATResult<GetTrendsSkeletonOutput>, ATErrorResult>> GetTrendsSkeletonAsync([FromQuery] ATDid? viewer = null, [FromQuery] int? limit = 10, CancellationToken cancellationToken = default)
        {
            return new GetTrendsSkeletonOutput
            {
                Trends = [],
            }.ToJsonResultOkTask();
        }

        public override Task<Results<ATResult<AgeAssuranceState>, ATErrorResult>> InitAgeAssuranceAsync([FromBody] InitAgeAssuranceInput input, CancellationToken cancellationToken)
        {
            return new AgeAssuranceState { LastInitiatedAt = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc), Status = "assured" }.ToJsonResultOkTask();
        }

        public override Task<Results<ATResult<SearchActorsSkeletonOutput>, ATErrorResult>> SearchActorsSkeletonAsync([FromQuery] string q, [FromQuery] ATDid? viewer = null, [FromQuery] bool? typeahead = null, [FromQuery] int? limit = 25, [FromQuery] string? cursor = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<ATResult<SearchPostsSkeletonOutput>, ATErrorResult>> SearchPostsSkeletonAsync([FromQuery] string q, [FromQuery] string? sort = null, [FromQuery] string? since = null, [FromQuery] string? until = null, [FromQuery] ATIdentifier? mentions = null, [FromQuery] ATIdentifier? author = null, [FromQuery] string? lang = null, [FromQuery] string? domain = null, [FromQuery] string? url = null, [FromQuery] List<string>? tag = null, [FromQuery] ATDid? viewer = null, [FromQuery] int? limit = 25, [FromQuery] string? cursor = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<ATResult<SearchStarterPacksSkeletonOutput>, ATErrorResult>> SearchStarterPacksSkeletonAsync([FromQuery] string q, [FromQuery] ATDid? viewer = null, [FromQuery] int? limit = 25, [FromQuery] string? cursor = null, CancellationToken cancellationToken = default)
        {
            return new SearchStarterPacksSkeletonOutput
            {
                StarterPacks = []
            }.ToJsonResultOkTask();
        }
    }
}

