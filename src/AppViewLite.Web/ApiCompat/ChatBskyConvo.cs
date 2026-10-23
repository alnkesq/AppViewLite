using CarpaNet.AspNetCore;
using ChatBsky.Convo;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Mvc;
using Microsoft.IdentityModel.Tokens.Experimental;

namespace AppViewLite.Web.ApiCompat
{
    [ApiController]
    [EnableCors("BskyClient")]
    public class ChatBskyConvo : Xrpc.ChatBsky.Convo.ConvoController
    {
        public override Task<Results<Ok<AcceptConvoOutput>, ATErrorResult>> AcceptConvoAsync([FromBody] AcceptConvoInput input, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok<AddReactionOutput>, ATErrorResult>> AddReactionAsync([FromBody] AddReactionInput input, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok<DefsDeletedMessageView>, ATErrorResult>> DeleteMessageForSelfAsync([FromBody] DeleteMessageForSelfInput input, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok<GetConvoOutput>, ATErrorResult>> GetConvoAsync([FromQuery(Name = "convoId")] string convoId, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok<GetConvoAvailabilityOutput>, ATErrorResult>> GetConvoAvailabilityAsync([FromQuery(Name = "members")] List<string> members, CancellationToken cancellationToken = default)
        {
            return new GetConvoAvailabilityOutput
            {
                CanChat = false,
            }.ToJsonResultOkTask();
        }

        public override Task<Results<Ok<GetConvoForMembersOutput>, ATErrorResult>> GetConvoForMembersAsync([FromQuery(Name = "members")] List<string> members, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok<GetLogOutput>, ATErrorResult>> GetLogAsync([FromQuery] string? cursor = null, CancellationToken cancellationToken = default)
        {
            return new GetLogOutput
            {
                Logs = []
            }.ToJsonResultOkTask();
        }

        public override Task<Results<Ok<GetMessagesOutput>, ATErrorResult>> GetMessagesAsync([FromQuery(Name = "convoId")] string convoId, [FromQuery(Name = "limit")] long? limit = null, [FromQuery(Name = "cursor")] string? cursor = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok<LeaveConvoOutput>, ATErrorResult>> LeaveConvoAsync([FromBody] LeaveConvoInput input, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok<ListConvosOutput>, ATErrorResult>> ListConvosAsync([FromQuery(Name = "kind")] string? kind = null, [FromQuery(Name = "limit")] long? limit = null, [FromQuery(Name = "cursor")] string? cursor = null, [FromQuery(Name = "status")] string? status = null, [FromQuery(Name = "readState")] string? readState = null, [FromQuery(Name = "lockStatus")] string? lockStatus = null, CancellationToken cancellationToken = default)
        {
            return new ListConvosOutput
            {
                Convos = []
            }.ToJsonResultOkTask();
        }

        public override Task<Results<Ok<MuteConvoOutput>, ATErrorResult>> MuteConvoAsync([FromBody] MuteConvoInput input, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok<RemoveReactionOutput>, ATErrorResult>> RemoveReactionAsync([FromBody] RemoveReactionInput input, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok<DefsMessageView>, ATErrorResult>> SendMessageAsync([FromBody] SendMessageInput input, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok<SendMessageBatchOutput>, ATErrorResult>> SendMessageBatchAsync([FromBody] SendMessageBatchInput input, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok<UnmuteConvoOutput>, ATErrorResult>> UnmuteConvoAsync([FromBody] UnmuteConvoInput input, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok<UpdateAllReadOutput>, ATErrorResult>> UpdateAllReadAsync([FromBody] UpdateAllReadInput input, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok<UpdateReadOutput>, ATErrorResult>> UpdateReadAsync([FromBody] UpdateReadInput input, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }
    }
}

