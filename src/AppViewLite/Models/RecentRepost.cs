using AppViewLite.Numerics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Models
{
    public record struct RecentRepost(Tid RepostRKey, PostId PostId) : IComparable<RecentRepost>
    {
        public int CompareTo(RecentRepost other)
        {
            var cmp = this.RepostRKey.CompareTo(other.RepostRKey);
            if (cmp != 0) return cmp;
            return this.PostId.CompareTo(other.PostId);
        }
    }
}
