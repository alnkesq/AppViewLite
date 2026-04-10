using System;
using System.Runtime.InteropServices;

namespace AppViewLite.Models
{
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public record struct LabelId(Plc Labeler /* empty if self-applied */, ulong NameHash) : IComparable<LabelId>
    {
        public int CompareTo(LabelId other)
        {
            var cmp = this.Labeler.CompareTo(other.Labeler);
            if (cmp != 0) return cmp;
            return this.NameHash.CompareTo(other.NameHash);
        }
    }
}

