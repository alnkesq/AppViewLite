using FishyFlip.Lexicon.Com.Atproto.Server;
using FishyFlip.Models;
using FishyFlip.Tools;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Mvc;

namespace AppViewLite.Web.ApiCompat
{
    [Route("/xrpc")]
    [ApiController]
    [EnableCors("BskyClient")]
    public class ComAtProtoServer : Controller
    {
        private readonly BlueskyEnrichedApis apis;
        private readonly RequestContext ctx;
        public ComAtProtoServer(BlueskyEnrichedApis apis, RequestContext ctx)
        {
            this.apis = apis;
            this.ctx = ctx;
        }

        [HttpGet("com.atproto.server.describeServer")]
        public IResult DescribeServer()
        {
            return new DescribeServerOutput
            {
                AvailableUserDomains = [],
                Did = new FishyFlip.Models.ATDid("did:web:invalid"),
            }.ToJsonResponse();
        }


        [HttpPost("com.atproto.server.createSession")]
        public async Task<IResult> CreateSession(CreateSessionInput parameters)
        {
            var identifier = parameters.Identifier.Trim('@');
            if (identifier.Contains('@')) throw new ATNetworkErrorException(new FishyFlip.Models.ATError(400, new FishyFlip.Models.ErrorDetail { Message = "Only login via handle is supported, not email address." }));
            var result = await apis.LogInAsync(identifier, parameters.Password, ctx);
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

            }.ToJsonResponse();
        }


        [HttpGet("com.atproto.server.getSession")]
        public object GetSession()
        {
            if (!ctx.IsLoggedIn)
            {
                Response.StatusCode = 400;
                return new
                {
                    error = "ExpiredToken",
                    message = "Invalid session."
                };
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
            }.ToJsonResponse();
        }
    }
}

