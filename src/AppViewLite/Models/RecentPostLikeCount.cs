using AppViewLite.Numerics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Models
{
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public record struct RecentPostLikeCount(Tid PostRKey, int LikeCount) : IComparable<RecentPostLikeCount>
    {
        public int CompareTo(RecentPostLikeCount other)
        {
            var cmp = this.PostRKey.CompareTo(other.PostRKey);
            if (cmp != 0) return cmp;
            return this.LikeCount.CompareTo(other.LikeCount);
        }
    }
}

