using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Models
{
    public record struct UserEngagementStats(Plc Target, int FollowingSeenPosts, int FollowingEngagedPosts, int EngagedPosts) : IComparable<UserEngagementStats>
    {
        public int CompareTo(UserEngagementStats other)
        {
            var cmp = this.Target.CompareTo(other.Target);
            if (cmp != 0) return cmp;
            cmp = this.FollowingSeenPosts.CompareTo(other.FollowingSeenPosts);
            if (cmp != 0) return cmp;
            cmp = this.FollowingEngagedPosts.CompareTo(other.FollowingEngagedPosts);
            if (cmp != 0) return cmp;
            cmp = this.EngagedPosts.CompareTo(other.EngagedPosts);
            return cmp;

        }
    }
}

