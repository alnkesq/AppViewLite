using FishyFlip.Lexicon.App.Bsky.Feed;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace AppViewLite.Web.ApiCompat
{
    [Route("/xrpc")]
    [ApiController]
    [EnableCors("BskyClient")]
    public class ComAtprotoIdentity : ControllerBase
    {
        private readonly BlueskyEnrichedApis apis;
        private readonly RequestContext ctx;
        public ComAtprotoIdentity(BlueskyEnrichedApis apis, RequestContext ctx)
        {
            this.apis = apis;
            this.ctx = ctx;
        }

        [HttpGet("com.atproto.identity.resolveHandle")]
        public async Task<IResult> ResolveHandle(string handle)
        {
            return new FishyFlip.Lexicon.Com.Atproto.Identity.ResolveHandleOutput
            {
                Did = new FishyFlip.Models.ATDid(await apis.ResolveHandleAsync(handle, ctx))
            }.ToJsonResponse();
        }
    }
}

