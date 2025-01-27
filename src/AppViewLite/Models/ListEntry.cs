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
        public string Serialize() => Member.PlcValue + "_" + ListItemRKey.TidValue;
        public static ListEntry Deserialize(string s)
        {
            var parts = s.Split('_');
            return new ListEntry(new Plc(int.Parse(parts[0])), new Tid(long.Parse(parts[1])));
        }
    }
}

