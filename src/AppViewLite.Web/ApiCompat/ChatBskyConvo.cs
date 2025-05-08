using FishyFlip.Lexicon.App.Bsky.Unspecced;
using FishyFlip.Lexicon.Chat.Bsky.Convo;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Mvc;

namespace AppViewLite.Web.ApiCompat
{
    [Route("/xrpc")]
    [ApiController]
    [EnableCors("BskyClient")]
    public class ChatBskyConvo : Controller
    {

        [HttpGet("chat.bsky.convo.listConvos")]
        public IResult ListConvos(int limit)
        {
            return new ListConvosOutput
            {
                Convos = []
            }.ToJsonResponse();
        }

        [HttpGet("chat.bsky.convo.getLog")]
        public IResult GetLog(int limit)
        {
            return new GetLogOutput
            {
                Logs = []
            }.ToJsonResponse();
        }

        //[HttpGet("chat.bsky.convo.getConvoForMembers")]
        //public IResult GetConvoForMembers(string[] members)
        //{
        //    return new GetConvoForMembersOutput
        //    {
        //        Convo = new ConvoView 
        //        {

        //        }
        //    }.ToJsonResponse();
        //}


        [HttpGet("chat.bsky.convo.getConvoAvailability")]
        public IResult GetConvoAvailability([FromQuery] string[] members)
        {
            return new GetConvoAvailabilityOutput
            {
                CanChat = false,
            }.ToJsonResponse();
        }



    }
}

