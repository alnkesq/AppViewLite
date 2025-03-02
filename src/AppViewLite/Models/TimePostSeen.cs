using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Models
{
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public record struct TimePostSeen(DateTime Date, PostId PostId) : IComparable<TimePostSeen>
    {
        public int CompareTo(TimePostSeen other)
        {
            var cmp = this.Date.CompareTo(other.Date);
            if (cmp != 0) return cmp;
            return this.PostId.CompareTo(other.PostId);
        }
    }
}

