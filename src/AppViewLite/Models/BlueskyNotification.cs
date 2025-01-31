using System;

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
        public BlueskyFeedGenerator? Feed;
        public NotificationCoalesceKey CoalesceKey => new(Post?.PostId ?? default, Kind, Feed?.RKeyHash ?? default);
    }
}

