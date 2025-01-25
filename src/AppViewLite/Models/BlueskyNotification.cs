using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Models
{
    public class BlueskyNotification
    {
        public DateTime EventDate;
        public NotificationKind Kind;
        public BlueskyPost? Post;
        public BlueskyProfile? Profile;
        public bool Hidden;
        public Notification NotificationCore;
    }
}

