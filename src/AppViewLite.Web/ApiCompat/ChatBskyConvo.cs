using FishyFlip.Lexicon.App.Bsky.Unspecced;
using FishyFlip.Lexicon.Chat.Bsky.Convo;
using FishyFlip.Models;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Mvc;

namespace AppViewLite.Web.ApiCompat
{
    [ApiController]
    [EnableCors("BskyClient")]
    public class ChatBskyConvo : FishyFlip.Xrpc.Lexicon.Chat.Bsky.Convo.ConvoController
    {

        public override Task<Results<Ok<AcceptConvoOutput>, ATErrorResult>> AcceptConvoAsync([FromBody] AcceptConvoInput input, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok<AddReactionOutput>, ATErrorResult>> AddReactionAsync([FromBody] AddReactionInput input, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok<DeletedMessageView>, ATErrorResult>> DeleteMessageForSelfAsync([FromBody] DeleteMessageForSelfInput input, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok<GetConvoOutput>, ATErrorResult>> GetConvoAsync([FromQuery] string convoId, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok<GetConvoAvailabilityOutput>, ATErrorResult>> GetConvoAvailabilityAsync([FromQuery] List<ATDid> members, CancellationToken cancellationToken = default)
        {
            return new GetConvoAvailabilityOutput
            {
                CanChat = false,
            }.ToJsonResultOkTask();
        }

        public override Task<Results<Ok<GetConvoForMembersOutput>, ATErrorResult>> GetConvoForMembersAsync([FromQuery] List<ATDid> members, CancellationToken cancellationToken = default)
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

        public override Task<Results<Ok<GetMessagesOutput>, ATErrorResult>> GetMessagesAsync([FromQuery] string convoId, [FromQuery] int? limit = 50, [FromQuery] string? cursor = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok<LeaveConvoOutput>, ATErrorResult>> LeaveConvoAsync([FromBody] LeaveConvoInput input, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok<ListConvosOutput>, ATErrorResult>> ListConvosAsync([FromQuery] int? limit = 50, [FromQuery] string? cursor = null, [FromQuery] string? readState = null, [FromQuery] string? status = null, CancellationToken cancellationToken = default)
        {
            return new ListConvosOutput
            {
                Convos = []
            }.ToJsonResultOkTask();
        }

        public override Task<Results<Ok<MuteConvoOutput>, ATErrorResult>> MuteConvoAsync([FromBody] MuteConvoInput input, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok<RemoveReactionOutput>, ATErrorResult>> RemoveReactionAsync([FromBody] RemoveReactionInput input, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok<MessageView>, ATErrorResult>> SendMessageAsync([FromBody] SendMessageInput input, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok<SendMessageBatchOutput>, ATErrorResult>> SendMessageBatchAsync([FromBody] SendMessageBatchInput input, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok<UnmuteConvoOutput>, ATErrorResult>> UnmuteConvoAsync([FromBody] UnmuteConvoInput input, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok<UpdateAllReadOutput>, ATErrorResult>> UpdateAllReadAsync([FromBody] UpdateAllReadInput input, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<Ok<UpdateReadOutput>, ATErrorResult>> UpdateReadAsync([FromBody] UpdateReadInput input, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }
    }
}

