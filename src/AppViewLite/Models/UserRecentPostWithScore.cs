using AppViewLite.Numerics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Models
{
    public record struct UserRecentPostWithScore(Tid RKey, Plc InReplyTo, int ApproximateLikeCount) : IComparable<UserRecentPostWithScore>
    {
        public int CompareTo(UserRecentPostWithScore other)
        {
            var cmp = this.RKey.CompareTo(other.RKey);
            if (cmp != 0) return cmp;
            cmp = this.InReplyTo.CompareTo(other.InReplyTo);
            if (cmp != 0) return cmp;
            return this.ApproximateLikeCount.CompareTo(other.ApproximateLikeCount);
        }
    }
}

