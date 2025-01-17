using AppViewLite.Numerics;
using System;
using System.Runtime.InteropServices;

namespace AppViewLite.Models
{
    [StructLayout(LayoutKind.Sequential, Pack = 4)]
    public record struct ListEntry(Plc Member, Tid ListItemRKey) : IComparable<ListEntry>
    {

        public int CompareTo(ListEntry other)
        {
            var z = Member.CompareTo(other.Member);
            if (z != 0) return z;
            return ListItemRKey.CompareTo(other.ListItemRKey);
        }

    }
}

