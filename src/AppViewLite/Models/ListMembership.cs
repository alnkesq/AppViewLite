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
    public record struct ListMembership(Plc ListAuthor, Tid ListRKey, Tid ListItemRKey) : IComparable<ListMembership>
    {
        public int CompareTo(ListMembership other)
        {
            var cmp = this.ListAuthor.CompareTo(other.ListAuthor);
            if (cmp != 0) return cmp;
            cmp = this.ListRKey.CompareTo(other.ListRKey);
            if (cmp != 0) return cmp;
            return this.ListItemRKey.CompareTo(other.ListItemRKey);

        }

        public string Serialize() => ListAuthor.PlcValue + "_" + ListRKey.TidValue + "_" + ListItemRKey.TidValue;
        public static ListMembership Deserialize(string s)
        {
            var parts = s.Split('_');
            return new ListMembership(new Plc(int.Parse(parts[0])), new Tid(long.Parse(parts[1])), new Tid(long.Parse(parts[2])));
        }
    }
}

