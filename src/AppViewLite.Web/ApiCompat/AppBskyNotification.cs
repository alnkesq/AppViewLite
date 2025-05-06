using FishyFlip.Lexicon.App.Bsky.Notification;
using FishyFlip.Lexicon.App.Bsky.Unspecced;
using FishyFlip.Lexicon.Chat.Bsky.Convo;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Mvc;

namespace AppViewLite.Web.ApiCompat
{
    [Route("/xrpc")]
    [ApiController]
    [EnableCors("BskyClient")]
    public class AppBskyNotification : Controller
    {

        [HttpGet("app.bsky.notification.listNotifications")]
        public IResult ListNotifications(int limit)
        {
            return new ListNotificationsOutput
            {
                Notifications = []
            }.ToJsonResponse();
        }
        [HttpPost("app.bsky.notification.updateSeen")]
        public object UpdateSeen(UpdateSeenInput input)
        {
            return Ok();
        }
    }
}

