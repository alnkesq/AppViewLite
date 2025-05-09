using Duende.IdentityModel.Client;
using FishyFlip.Lexicon;
using FishyFlip.Lexicon.Com.Atproto.Server;
using FishyFlip.Models;
using FishyFlip.Tools;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Mvc;

namespace AppViewLite.Web.ApiCompat
{
    [ApiController]
    [EnableCors("BskyClient")]
    public class ComAtProtoServer : FishyFlip.Xrpc.Lexicon.Com.Atproto.Server.ServerController
    {
        private readonly BlueskyEnrichedApis apis;
        private readonly RequestContext ctx;
        public ComAtProtoServer(BlueskyEnrichedApis apis, RequestContext ctx)
        {
            this.apis = apis;
            this.ctx = ctx;
        }

        public override Task<Results<Ok, ATErrorResult>> ActivateAccountAsync(CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok<CheckAccountStatusOutput>, ATErrorResult>> CheckAccountStatusAsync(CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok, ATErrorResult>> ConfirmEmailAsync([FromBody] ConfirmEmailInput input, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok<CreateAccountOutput>, ATErrorResult>> CreateAccountAsync([FromBody] CreateAccountInput input, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok<AppPassword>, ATErrorResult>> CreateAppPasswordAsync([FromBody] CreateAppPasswordInput input, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok<CreateInviteCodeOutput>, ATErrorResult>> CreateInviteCodeAsync([FromBody] CreateInviteCodeInput input, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok<CreateInviteCodesOutput>, ATErrorResult>> CreateInviteCodesAsync([FromBody] CreateInviteCodesInput input, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override async Task<Results<Ok<CreateSessionOutput>, ATErrorResult>> CreateSessionAsync([FromBody] CreateSessionInput input, CancellationToken cancellationToken)
        {
            var identifier = input.Identifier.Trim('@');
            if (identifier.Contains('@')) throw new ATNetworkErrorException(new FishyFlip.Models.ATError(400, new FishyFlip.Models.ErrorDetail { Message = "Only login via handle is supported, not email address." }));
            var result = await apis.LogInAsync(identifier, input.Password, ctx);
            var session = result.Session;
            var userCtx = session.UserContext;
            var pdsSession = userCtx.PdsSession!;
            return new CreateSessionOutput
            {
                Active = true,
                Did = new FishyFlip.Models.ATDid(userCtx.Did!),
                Email = userCtx.PdsSession!.Email,
                Handle = pdsSession.Handle,
                DidDoc = pdsSession.DidDoc,
                AccessJwt = pdsSession.AccessJwt,
                RefreshJwt = pdsSession.RefreshJwt,
                EmailConfirmed = true,
                //AccessJwt = result.Cookie, // Fails with invalid token (string is not treated opaquely by social-app)

            }.ToJsonResultOk();
        }

        public override Task<Results<Ok, ATErrorResult>> DeactivateAccountAsync([FromBody] DeactivateAccountInput input, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok, ATErrorResult>> DeleteAccountAsync([FromBody] DeleteAccountInput input, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok, ATErrorResult>> DeleteSessionAsync(CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok<DescribeServerOutput>, ATErrorResult>> DescribeServerAsync(CancellationToken cancellationToken = default)
        {
            return new DescribeServerOutput
            {
                AvailableUserDomains = [],
                Did = new FishyFlip.Models.ATDid("did:web:invalid"),
            }.ToJsonResultOkTask();
        }

        public override Task<Results<Ok<GetAccountInviteCodesOutput>, ATErrorResult>> GetAccountInviteCodesAsync([FromQuery] bool? includeUsed = null, [FromQuery] bool? createAvailable = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok<GetServiceAuthOutput>, ATErrorResult>> GetServiceAuthAsync([FromQuery] ATDid aud, [FromQuery] int? exp = 0, [FromQuery] string? lxm = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok<GetSessionOutput>, ATErrorResult>> GetSessionAsync(CancellationToken cancellationToken = default)
        {
            if (!ctx.IsLoggedIn)
            {
                Response.StatusCode = 400;
                return ATErrorResult.Unauthorized().ToJsonResultTask<GetSessionOutput>();
            }
            var userCtx = ctx.UserContext;
            var pdsSession = userCtx.PdsSession!;
            return new GetSessionOutput()
            {
                Active = true,
                Did = new FishyFlip.Models.ATDid(userCtx.Did!),
                Email = pdsSession.Email,
                Handle = pdsSession.Handle,
                DidDoc = pdsSession.DidDoc,
                EmailConfirmed = true,
            }.ToJsonResultOkTask();
        }

        public override Task<Results<Ok<ListAppPasswordsOutput>, ATErrorResult>> ListAppPasswordsAsync(CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok<RefreshSessionOutput>, ATErrorResult>> RefreshSessionAsync(CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok, ATErrorResult>> RequestAccountDeleteAsync(CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok, ATErrorResult>> RequestEmailConfirmationAsync(CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok<RequestEmailUpdateOutput>, ATErrorResult>> RequestEmailUpdateAsync(CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok, ATErrorResult>> RequestPasswordResetAsync([FromBody] RequestPasswordResetInput input, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok<ReserveSigningKeyOutput>, ATErrorResult>> ReserveSigningKeyAsync([FromBody] ReserveSigningKeyInput input, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok, ATErrorResult>> ResetPasswordAsync([FromBody] ResetPasswordInput input, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok, ATErrorResult>> RevokeAppPasswordAsync([FromBody] RevokeAppPasswordInput input, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok, ATErrorResult>> UpdateEmailAsync([FromBody] UpdateEmailInput input, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }
    }
}

