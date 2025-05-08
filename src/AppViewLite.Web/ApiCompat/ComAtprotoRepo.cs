using AppViewLite.Numerics;
using FishyFlip.Lexicon.App.Bsky.Feed;
using FishyFlip.Lexicon.Com.Atproto.Repo;
using FishyFlip.Models;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace AppViewLite.Web.ApiCompat
{
    [Route("/xrpc")]
    [ApiController]
    [EnableCors("BskyClient")]
    public class ComAtprotoRecord : ControllerBase
    {
        private readonly BlueskyEnrichedApis apis;
        private readonly RequestContext ctx;
        public ComAtprotoRecord(BlueskyEnrichedApis apis, RequestContext ctx)
        {
            this.apis = apis;
            this.ctx = ctx;
        }

        [HttpPost("com.atproto.repo.createRecord")]
        public async Task<IResult> CreateRecord()
        {
            var input = await ApiCompatUtils.RequestBodyToATObjectAsync<CreateRecordInput>(Request.Body);
            var result = await apis.CreateRecordAsync(input.Record, ctx, rkey: input.Rkey);
            var uri = new ATUri("at://" + ctx.UserContext.Did + "/" + input.Collection + "/" + result.RkeyString);
            return new CreateRecordOutput
            {
                Uri = uri,
                Cid = ApiCompatUtils.GetSyntheticCid(uri),
            }.ToJsonResponse();
        }

        [HttpPost("com.atproto.repo.deleteRecord")]
        public async Task<IResult> DeleteRecord()
        {
            var input = await ApiCompatUtils.RequestBodyToATObjectAsync<DeleteRecordInput>(Request.Body);
            await apis.DeleteRecordAsync(input.Collection, Tid.Parse(input.Rkey), ctx);
            return new DeleteRecordOutput
            {
            }.ToJsonResponse();
        }

        [HttpGet("com.atproto.repo.getRecord")]
        public async Task<IResult> GetRecord(string repo, string collection, string rkey)
        {
            var result = await apis.GetRecordAsync(repo, collection, rkey, ctx);
            return result.ToJsonResponse();
        }
    }
}

