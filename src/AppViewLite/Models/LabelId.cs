using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Models
{
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public record struct LabelId(Plc Labeler, ulong NameHash) : IComparable<LabelId>
    {
        public int CompareTo(LabelId other)
        {
            var cmp = this.Labeler.CompareTo(other.Labeler);
            if (cmp != 0) return cmp;
            return this.NameHash.CompareTo(other.NameHash);
        }
    }
}

