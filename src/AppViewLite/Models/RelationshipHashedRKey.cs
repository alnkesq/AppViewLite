using Ipfs;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Models
{
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public record struct RelationshipHashedRKey(Plc Plc, ulong RKeyHash) : IComparable<RelationshipHashedRKey>
    {
        public RelationshipHashedRKey(Plc plc, ReadOnlySpan<char> rkey)
            : this(plc, System.IO.Hashing.XxHash64.HashToUInt64(MemoryMarshal.AsBytes<char>(rkey)))
        { 
        }
        public int CompareTo(RelationshipHashedRKey other)
        {
            var cmp = this.Plc.CompareTo(other.Plc);
            if (cmp != 0) return cmp;
            return this.RKeyHash.CompareTo(other.RKeyHash);
        }

        public readonly static RelationshipHashedRKey MaxValue = new(Plc.MaxValue, ulong.MaxValue);

        public string Serialize() => Plc.PlcValue + "_" + RKeyHash;
        public static RelationshipHashedRKey Deserialize(string s)
        {
            var a = s.Split('_');
            return new RelationshipHashedRKey(new Plc(int.Parse(a[0])), ulong.Parse(a[1]));
        }
    }
}

