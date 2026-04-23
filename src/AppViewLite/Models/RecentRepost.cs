using AppViewLite.Numerics;
using System;
using System.Runtime.InteropServices;

namespace AppViewLite.Models
{
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public record struct RecentRepost(Tid RepostRKey, PostId PostId) : IComparable<RecentRepost>
    {
        public readonly int CompareTo(RecentRepost other)
        {
            var cmp = this.RepostRKey.CompareTo(other.RepostRKey);
            if (cmp != 0) return cmp;
            return this.PostId.CompareTo(other.PostId);
        }
    }
}

