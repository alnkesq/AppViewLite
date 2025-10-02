using AppViewLite.Numerics;
using FishyFlip.Lexicon.App.Bsky.Feed;
using FishyFlip.Lexicon.App.Bsky.Graph;
using FishyFlip.Models;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Mvc;
using AppViewLite;

namespace AppViewLite.Web
{
    [ApiController]
    [EnableCors("BskyClient")]
    public class AppBskyGraph : FishyFlip.Xrpc.Lexicon.App.Bsky.Graph.GraphController
    {
        private readonly BlueskyEnrichedApis apis;
        private readonly RequestContext ctx;
        public AppBskyGraph(BlueskyEnrichedApis apis, RequestContext ctx)
        {
            this.apis = apis;
            this.ctx = ctx;
        }

        public override Task<Results<ATResult<GetActorStarterPacksOutput>, ATErrorResult>> GetActorStarterPacksAsync([FromQuery] ATIdentifier actor, [FromQuery] int? limit = 50, [FromQuery] string? cursor = null, CancellationToken cancellationToken = default)
        {
            return new GetActorStarterPacksOutput
            {
                StarterPacks = []
            }.ToJsonResultOkTask();
        }

        public override Task<Results<ATResult<GetBlocksOutput>, ATErrorResult>> GetBlocksAsync([FromQuery] int? limit = 50, [FromQuery] string? cursor = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public async override Task<Results<ATResult<GetFollowersOutput>, ATErrorResult>> GetFollowersAsync([FromQuery] ATIdentifier actor, [FromQuery] int? limit = 50, [FromQuery] string? cursor = null, CancellationToken cancellationToken = default)
        {
            var actorDid = ((ATDid)actor).Handler;
            var subject = await apis.GetProfileAsync(actorDid, ctx);
            var (followers, nextContinuation) = await apis.GetFollowersAsync(actorDid, cursor, limit ?? default, ctx);

            return new GetFollowersOutput
            {
                Followers = followers.Select(x => x.ToApiCompatProfileView()).ToList(),
                Cursor = nextContinuation,
                Subject = subject.ToApiCompatProfileView()
            }.ToJsonResultOk();
        }

        public async override Task<Results<ATResult<GetFollowsOutput>, ATErrorResult>> GetFollowsAsync([FromQuery] ATIdentifier actor, [FromQuery] int? limit = 50, [FromQuery] string? cursor = null, CancellationToken cancellationToken = default)
        {
            var actorDid = ((ATDid)actor).Handler;
            var subject = await apis.GetProfileAsync(actorDid, ctx);
            var (follows, nextContinuation) = await apis.GetFollowingAsync(actorDid, cursor, limit ?? default, ctx);

            return new GetFollowsOutput
            {
                Follows = follows.Select(x => x.ToApiCompatProfileView()).ToList(),
                Cursor = nextContinuation,
                Subject = subject.ToApiCompatProfileView()
            }.ToJsonResultOk();
        }

        public override Task<Results<ATResult<GetKnownFollowersOutput>, ATErrorResult>> GetKnownFollowersAsync([FromQuery] ATIdentifier actor, [FromQuery] int? limit = 50, [FromQuery] string? cursor = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public async override Task<Results<ATResult<GetListOutput>, ATErrorResult>> GetListAsync([FromQuery] ATUri list, [FromQuery] int? limit = 50, [FromQuery] string? cursor = null, CancellationToken cancellationToken = default)
        {
            var info = await apis.GetListMetadataAsync(list.Did!.Handler, list.Rkey, ctx);
            var members = await apis.GetListMembersAsync(info.ModeratorDid!, info.RKey, cursor, limit ?? default, ctx);

            return new GetListOutput
            {
                List = ApiCompatUtils.ToApiCompatListView(info),
                Items = members.Page.Select(x => ApiCompatUtils.ToApiCompatToListItemView(x, list.Did.Handler)).ToList(),
                Cursor = members.NextContinuation,
            }.ToJsonResultOk();
        }

        public override Task<Results<ATResult<GetListBlocksOutput>, ATErrorResult>> GetListBlocksAsync([FromQuery] int? limit = 50, [FromQuery] string? cursor = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<ATResult<GetListMutesOutput>, ATErrorResult>> GetListMutesAsync([FromQuery] int? limit = 50, [FromQuery] string? cursor = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public async override Task<Results<ATResult<GetListsOutput>, ATErrorResult>> GetListsAsync([FromQuery] ATIdentifier actor, [FromQuery] int? limit = 50, [FromQuery] string? cursor = null, [FromQuery] List<string>? purposes = null, CancellationToken cancellationToken = default)
        {
            var lists = await apis.GetProfileListsAsync(((ATDid)actor).Handler, cursor, limit ?? default, ctx);

            return new FishyFlip.Lexicon.App.Bsky.Graph.GetListsOutput
            {
                Lists = lists.Lists.Select(x => ApiCompatUtils.ToApiCompatListView(x)).Where(x => purposes != null && purposes.Count != 0 ? purposes.Contains(x.Purpose) : true).ToList(),
                Cursor = lists.NextContinuation,
            }.ToJsonResultOk();
        }

        public override Task<Results<ATResult<GetListsWithMembershipOutput>, ATErrorResult>> GetListsWithMembershipAsync([FromQuery] ATIdentifier actor, [FromQuery] int? limit = 50, [FromQuery] string? cursor = null, [FromQuery] List<string>? purposes = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }


        public override Task<Results<ATResult<GetMutesOutput>, ATErrorResult>> GetMutesAsync([FromQuery] int? limit = 50, [FromQuery] string? cursor = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<ATResult<GetRelationshipsOutput>, ATErrorResult>> GetRelationshipsAsync([FromQuery] ATIdentifier actor, [FromQuery] List<ATIdentifier>? others = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<ATResult<GetStarterPackOutput>, ATErrorResult>> GetStarterPackAsync([FromQuery] ATUri starterPack, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<ATResult<GetStarterPacksOutput>, ATErrorResult>> GetStarterPacksAsync([FromQuery] List<ATUri> uris, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<ATResult<GetSuggestedFollowsByActorOutput>, ATErrorResult>> GetSuggestedFollowsByActorAsync([FromQuery] ATIdentifier actor, CancellationToken cancellationToken = default)
        {
            return new GetSuggestedFollowsByActorOutput
            {
                Suggestions = []
            }.ToJsonResultOkTask();
        }

        public override Task<Results<Ok, ATErrorResult>> MuteActorAsync([FromBody] MuteActorInput input, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok, ATErrorResult>> MuteActorListAsync([FromBody] MuteActorListInput input, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok, ATErrorResult>> MuteThreadAsync([FromBody] MuteThreadInput input, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<ATResult<SearchStarterPacksOutput>, ATErrorResult>> SearchStarterPacksAsync([FromQuery] string q, [FromQuery] int? limit = 25, [FromQuery] string? cursor = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok, ATErrorResult>> UnmuteActorAsync([FromBody] UnmuteActorInput input, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok, ATErrorResult>> UnmuteActorListAsync([FromBody] UnmuteActorListInput input, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok, ATErrorResult>> UnmuteThreadAsync([FromBody] UnmuteThreadInput input, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<ATResult<GetStarterPacksWithMembershipOutput>, ATErrorResult>> GetStarterPacksWithMembershipAsync([FromQuery] ATIdentifier actor, [FromQuery] int? limit = 50, [FromQuery] string? cursor = null, CancellationToken cancellationToken = default)
        {
            return new GetStarterPacksWithMembershipOutput() { StarterPacksWithMembership = [] }.ToJsonResultOkTask();
        }
    }
}

