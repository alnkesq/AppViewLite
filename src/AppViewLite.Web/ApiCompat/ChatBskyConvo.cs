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

        public override Task<Results<ATResult<AcceptConvoOutput>, ATErrorResult>> AcceptConvoAsync([FromBody] AcceptConvoInput input, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<ATResult<AddReactionOutput>, ATErrorResult>> AddReactionAsync([FromBody] AddReactionInput input, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<ATResult<DeletedMessageView>, ATErrorResult>> DeleteMessageForSelfAsync([FromBody] DeleteMessageForSelfInput input, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<ATResult<GetConvoOutput>, ATErrorResult>> GetConvoAsync([FromQuery] string convoId, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<ATResult<GetConvoAvailabilityOutput>, ATErrorResult>> GetConvoAvailabilityAsync([FromQuery] List<ATDid> members, CancellationToken cancellationToken = default)
        {
            return new GetConvoAvailabilityOutput
            {
                CanChat = false,
            }.ToJsonResultOkTask();
        }

        public override Task<Results<ATResult<GetConvoForMembersOutput>, ATErrorResult>> GetConvoForMembersAsync([FromQuery] List<ATDid> members, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<ATResult<GetLogOutput>, ATErrorResult>> GetLogAsync([FromQuery] string? cursor = null, CancellationToken cancellationToken = default)
        {
            return new GetLogOutput
            {
                Logs = []
            }.ToJsonResultOkTask();
        }

        public override Task<Results<ATResult<GetMessagesOutput>, ATErrorResult>> GetMessagesAsync([FromQuery] string convoId, [FromQuery] int? limit = 50, [FromQuery] string? cursor = null, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<ATResult<LeaveConvoOutput>, ATErrorResult>> LeaveConvoAsync([FromBody] LeaveConvoInput input, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<ATResult<ListConvosOutput>, ATErrorResult>> ListConvosAsync([FromQuery] int? limit = 50, [FromQuery] string? cursor = null, [FromQuery] string? readState = null, [FromQuery] string? status = null, CancellationToken cancellationToken = default)
        {
            return new ListConvosOutput
            {
                Convos = []
            }.ToJsonResultOkTask();
        }

        public override Task<Results<ATResult<MuteConvoOutput>, ATErrorResult>> MuteConvoAsync([FromBody] MuteConvoInput input, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<ATResult<RemoveReactionOutput>, ATErrorResult>> RemoveReactionAsync([FromBody] RemoveReactionInput input, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<ATResult<MessageView>, ATErrorResult>> SendMessageAsync([FromBody] SendMessageInput input, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<ATResult<SendMessageBatchOutput>, ATErrorResult>> SendMessageBatchAsync([FromBody] SendMessageBatchInput input, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<ATResult<UnmuteConvoOutput>, ATErrorResult>> UnmuteConvoAsync([FromBody] UnmuteConvoInput input, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<ATResult<UpdateAllReadOutput>, ATErrorResult>> UpdateAllReadAsync([FromBody] UpdateAllReadInput input, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override Task<Results<ATResult<UpdateReadOutput>, ATErrorResult>> UpdateReadAsync([FromBody] UpdateReadInput input, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }
    }
}

