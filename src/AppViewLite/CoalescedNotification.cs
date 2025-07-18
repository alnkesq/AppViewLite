using AppViewLite.Models;
using AppViewLite.Numerics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite
{
    public class CoalescedNotification
    {
        public DateTime LatestDate;
        public NotificationKind Kind;
        public BlueskyPost? Post;
        public PostId PostId;
        public required List<BlueskyProfile> Profiles;
        public bool IsNew;
        public BlueskyFeedGenerator? Feed;
        public BlueskyList? List;
        public RelationshipHashedRKey FeedRKeyHash;
        public Tid ListRKey;

        public NotificationCoalesceKey CoalesceKey => new(PostId, Kind, FeedRKeyHash, ListRKey);
        public bool ShouldEmbedFullPost => BlueskyNotification.ShouldEmbedFullPost(Kind);
        public string KindDisplayText
        {
            get
            {
                return Kind switch
                {
                    NotificationKind.FollowedYou => "followed you",
                    NotificationKind.FollowedYouBack => "followed you back",
                    NotificationKind.LikedYourPost => "liked your post",
                    NotificationKind.LikedYourFeed => "liked your feed",
                    NotificationKind.RepostedYourPost => "reposted your post",
                    NotificationKind.QuotedYourPost => "quoted you",
                    NotificationKind.MentionedYou => "mentioned you",
                    NotificationKind.RepliedToYourPost => "replied to your post",
                    NotificationKind.RepliedToADescendant => "replied to your post",
                    NotificationKind.RepliedToYourThread => "replied to your thread",
                    NotificationKind.BlockedYou => "blocked you",
                    NotificationKind.UnfollowedYou => "unfollowed you",
                    NotificationKind.AddedYouToAList => "added you to a list",
                    NotificationKind.HidYourReply => "hid your reply",
                    NotificationKind.DetachedYourQuotePost => "detached your quote",
                    NotificationKind.LabeledYourPost => "labeled your post",
                    NotificationKind.LabeledYourProfile => "labeled your profile",
                    NotificationKind.LikedYourRepost => "liked your repost",
                    NotificationKind.RepostedYourRepost => "reposted your repost",
                    _ => throw AssertionLiteException.ThrowBadEnumException(Kind)
                };
            }
        }

    }

    public record struct NotificationCoalesceKey(PostId PostId, NotificationKind Kind, RelationshipHashedRKey FeedRKeyHash, Tid ListRKey);
}

