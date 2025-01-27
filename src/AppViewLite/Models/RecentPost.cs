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
    public record struct RecentPost(Tid RKey, Plc InReplyTo) : IComparable<RecentPost>
    {
        public int CompareTo(RecentPost other)
        {
            var cmp = this.RKey.CompareTo(other.RKey);
            if (cmp != 0) return cmp;
            return this.InReplyTo.CompareTo(other.InReplyTo);
        }
    }
}

