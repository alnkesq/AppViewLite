using FishyFlip.Lexicon.App.Bsky.Feed;
using FishyFlip.Lexicon.App.Bsky.Labeler;
using FishyFlip.Models;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Mvc;

namespace AppViewLite.Web.ApiCompat
{
    [ApiController]
    [EnableCors("BskyClient")]
    public class AppBskyLabeler : FishyFlip.Xrpc.Lexicon.App.Bsky.Labeler.LabelerController
    {
        private readonly BlueskyEnrichedApis apis;
        public AppBskyLabeler(BlueskyEnrichedApis apis)
        {
            this.apis = apis;
        }

        public override Task<Results<ATResult<GetServicesOutput>, ATErrorResult>> GetServicesAsync([FromQuery] List<ATDid> dids, [FromQuery] bool? detailed = null, CancellationToken cancellationToken = default)
        {
            return new GetServicesOutput
            {
                Views = []
            }.ToJsonResultOkTask();
        }
    }
}

