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
        public BlueskyList? List;
        public NotificationCoalesceKey CoalesceKey
        {
            get
            {
                return new(Post?.PostId ?? default, Kind, Feed?.RKeyHash ?? default, List?.ListId.RelationshipRKey ?? default);
            }
        }

        internal static bool ShouldEmbedFullPost(NotificationKind kind)
        {
            return kind is NotificationKind.MentionedYou or NotificationKind.QuotedYourPost or NotificationKind.RepliedToYourThread or NotificationKind.RepliedToYourPost or NotificationKind.RepliedToADescendant;
        }

        public override string ToString()
        {
            return EventDate + " " + Kind.ToString();
        }
    }
}

