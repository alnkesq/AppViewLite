using AppViewLite.Numerics;
using System;
using System.Runtime.InteropServices;

namespace AppViewLite.Models
{
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public record struct LabelEntry(Plc Labeler, ApproximateDateTime32 Date, ulong KindHash, bool Neg) : IComparable<LabelEntry>
    {
        public int CompareTo(LabelEntry other)
        {
            var cmp = this.Labeler.CompareTo(other.Labeler);
            if (cmp != 0) return cmp;
            cmp = this.Date.CompareTo(other.Date);
            if (cmp != 0) return cmp;
            cmp = this.KindHash.CompareTo(other.KindHash);
            if (cmp != 0) return cmp;
            cmp = this.Neg.CompareTo(other.Neg);
            return cmp;
        }
    }
}

