using System;
using System.Runtime.InteropServices;

namespace AppViewLite.Models
{
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public record struct RelationshipHash(uint High, ushort Low) : IComparable<RelationshipHash>
    {
        public int CompareTo(RelationshipHash other)
        {
            var z = High.CompareTo(other.High);
            if (z != 0) return z;
            return Low.CompareTo(other.Low);
        }
    }
}

