using FishyFlip.Lexicon;
using FishyFlip.Lexicon.Com.Atproto.Identity;
using FishyFlip.Models;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Mvc;

namespace AppViewLite.Web.ApiCompat
{
    [ApiController]
    [EnableCors("BskyClient")]
    public class ComAtprotoIdentity : FishyFlip.Xrpc.Lexicon.Com.Atproto.Identity.IdentityController
    {
        private readonly BlueskyEnrichedApis apis;
        private readonly RequestContext ctx;
        public ComAtprotoIdentity(BlueskyEnrichedApis apis, RequestContext ctx)
        {
            this.apis = apis;
            this.ctx = ctx;
        }

        public override Task<Results<ATResult<GetRecommendedDidCredentialsOutput>, ATErrorResult>> GetRecommendedDidCredentialsAsync(CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<ATResult<IdentityInfo>, ATErrorResult>> RefreshIdentityAsync([FromBody] RefreshIdentityInput input, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok, ATErrorResult>> RequestPlcOperationSignatureAsync(CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<ATResult<ResolveDidOutput>, ATErrorResult>> ResolveDidAsync([FromQuery] ATDid did, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public async override Task<Results<ATResult<ResolveHandleOutput>, ATErrorResult>> ResolveHandleAsync([FromQuery] ATHandle handle, CancellationToken cancellationToken = default)
        {
            return new ResolveHandleOutput
            {
                Did = new FishyFlip.Models.ATDid(await apis.ResolveHandleAsync(handle.Handle, ctx))
            }.ToJsonResultOk();
        }

        public override Task<Results<ATResult<IdentityInfo>, ATErrorResult>> ResolveIdentityAsync([FromQuery] ATIdentifier identifier, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<ATResult<SignPlcOperationOutput>, ATErrorResult>> SignPlcOperationAsync([FromBody] SignPlcOperationInput input, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok, ATErrorResult>> SubmitPlcOperationAsync([FromBody] SubmitPlcOperationInput input, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok, ATErrorResult>> UpdateHandleAsync([FromBody] UpdateHandleInput input, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }
    }
}

