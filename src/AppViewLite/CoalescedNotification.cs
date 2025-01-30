using AppViewLite.Models;
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
        public List<BlueskyProfile>? Profiles;
        public bool IsNew;
        public BlueskyFeedGenerator? Feed;
        public RelationshipHashedRKey FeedRKeyHash;

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
                    NotificationKind.RepliedToYourThread => "replied to your thread",
                    _ => throw new Exception()
                };
            }
        }

    }
}

