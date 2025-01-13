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
        [HttpGet("com.atproto.identity.resolveHandle")]
        public async Task<FishyFlip.Lexicon.Com.Atproto.Identity.ResolveHandleOutput> ResolveHandle(string handle)
        {
            return new FishyFlip.Lexicon.Com.Atproto.Identity.ResolveHandleOutput
            {
                Did = new FishyFlip.Models.ATDid(await BlueskyEnrichedApis.Instance.ResolveHandleAsync(handle))
            };
        }
    }
}

