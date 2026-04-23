using AppViewLite.Numerics;
using System;
using System.Runtime.InteropServices;

namespace AppViewLite.Models
{
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public record struct RecentPost(Tid RKey, Plc InReplyTo) : IComparable<RecentPost>
    {
        public readonly int CompareTo(RecentPost other)
        {
            var cmp = this.RKey.CompareTo(other.RKey);
            if (cmp != 0) return cmp;
            return this.InReplyTo.CompareTo(other.InReplyTo);
        }
    }
}

