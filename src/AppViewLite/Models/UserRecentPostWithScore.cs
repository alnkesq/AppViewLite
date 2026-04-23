using AppViewLite.Numerics;
using System;

namespace AppViewLite.Models
{
    public record struct UserRecentPostWithScore(Tid RKey, Plc InReplyTo, int ApproximateLikeCount) : IComparable<UserRecentPostWithScore>
    {
        public readonly int CompareTo(UserRecentPostWithScore other)
        {
            var cmp = this.RKey.CompareTo(other.RKey);
            if (cmp != 0) return cmp;
            cmp = this.InReplyTo.CompareTo(other.InReplyTo);
            if (cmp != 0) return cmp;
            return this.ApproximateLikeCount.CompareTo(other.ApproximateLikeCount);
        }
    }
}

