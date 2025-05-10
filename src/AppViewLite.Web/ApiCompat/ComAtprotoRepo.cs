using AppViewLite.Numerics;
using FishyFlip.Lexicon.App.Bsky.Feed;
using FishyFlip.Lexicon.Com.Atproto.Repo;
using FishyFlip.Models;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Mvc;

namespace AppViewLite.Web.ApiCompat
{
    [ApiController]
    [EnableCors("BskyClient")]
    public class ComAtprotoRepo : FishyFlip.Xrpc.Lexicon.Com.Atproto.Repo.RepoController
    {
        private readonly BlueskyEnrichedApis apis;
        private readonly RequestContext ctx;
        public ComAtprotoRepo(BlueskyEnrichedApis apis, RequestContext ctx)
        {
            this.apis = apis;
            this.ctx = ctx;
        }

        public override Task<Results<ATResult<ApplyWritesOutput>, ATErrorResult>> ApplyWritesAsync([FromBody] ApplyWritesInput input, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }
        public async override Task<Results<ATResult<CreateRecordOutput>, ATErrorResult>> CreateRecordAsync([FromBody] CreateRecordInput input, CancellationToken cancellationToken)
        {
            var result = await apis.CreateRecordAsync(input.Record, ctx, rkey: input.Rkey);
            var uri = new ATUri("at://" + ctx.UserContext.Did + "/" + input.Collection + "/" + result.RkeyString);
            return new CreateRecordOutput
            {
                Uri = uri,
                Cid = ApiCompatUtils.GetSyntheticCid(uri),
            }.ToJsonResultOk();
        }

        public async override Task<Results<ATResult<DeleteRecordOutput>, ATErrorResult>> DeleteRecordAsync([FromBody] DeleteRecordInput input, CancellationToken cancellationToken)
        {
            await apis.DeleteRecordAsync(input.Collection, Tid.Parse(input.Rkey), ctx);
            return new DeleteRecordOutput
            {
            }.ToJsonResultOk();
        }

        public override Task<Results<ATResult<DescribeRepoOutput>, ATErrorResult>> DescribeRepoAsync([FromQuery] ATIdentifier repo, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public async override Task<Results<ATResult<GetRecordOutput>, ATErrorResult>> GetRecordAsync([FromQuery] ATIdentifier repo, [FromQuery] string collection, [FromQuery] string rkey, [FromQuery] string? cid = null, CancellationToken cancellationToken = default)
        {
            var result = await apis.GetRecordAsync(((ATDid)repo).Handler, collection, rkey, ctx, cancellationToken);
            return result.ToJsonResultOk();
        }

        public override Task<Results<Ok, ATErrorResult>> ImportRepoAsync([FromBody] StreamContent content, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<ATResult<ListMissingBlobsOutput>, ATErrorResult>> ListMissingBlobsAsync([FromQuery] int? limit = 500, [FromQuery] string? cursor = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<ATResult<ListRecordsOutput>, ATErrorResult>> ListRecordsAsync([FromQuery] ATIdentifier repo, [FromQuery] string collection, [FromQuery] int? limit = 50, [FromQuery] string? cursor = null, [FromQuery] bool? reverse = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<ATResult<PutRecordOutput>, ATErrorResult>> PutRecordAsync([FromBody] PutRecordInput input, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<ATResult<UploadBlobOutput>, ATErrorResult>> UploadBlobAsync([FromBody] StreamContent content, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }
    }
}

