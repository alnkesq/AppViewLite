using AppViewLite;
using System;
using System.Runtime.InteropServices;

namespace AppViewLite.Models
{
    // Not a "record struct" because C# doesn't provide a way of specifying "safe" (required by Pack=1) on the fields of a record, as of .NET 11
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public readonly struct RelationshipHashedRKey : IComparable<RelationshipHashedRKey>, IEquatable<RelationshipHashedRKey>
    {
        public safe readonly Plc Plc;
        public safe readonly ulong RKeyHash;
        public RelationshipHashedRKey(Plc plc, ReadOnlySpan<char> rkey)
            : this(plc, System.IO.Hashing.XxHash64.HashToUInt64(MostlySafe.AsBytes<char>(rkey)))
        {
        }
        public RelationshipHashedRKey(Plc plc, ulong rKeyHash)
        {
            this.Plc = plc;
            this.RKeyHash = rKeyHash;
        }

        public readonly int CompareTo(RelationshipHashedRKey other)
        {
            var cmp = this.Plc.CompareTo(other.Plc);
            if (cmp != 0) return cmp;
            return this.RKeyHash.CompareTo(other.RKeyHash);
        }

        public readonly static RelationshipHashedRKey MaxValue = new(Plc.MaxValue, ulong.MaxValue);

        public readonly string Serialize() => Plc.PlcValue + "_" + RKeyHash;
        public static RelationshipHashedRKey Deserialize(string s)
        {
            var a = s.Split('_');
            return new RelationshipHashedRKey(new Plc(int.Parse(a[0])), ulong.Parse(a[1]));
        }

        public override readonly string ToString() => $"{nameof(RelationshipHashedRKey)}({Plc.PlcValue}, {RKeyHash})";
        public bool Equals(RelationshipHashedRKey other)
        {
            return this.Plc == other.Plc && this.RKeyHash == other.RKeyHash;
        }
        public override bool Equals(object? obj)
        {
            return obj is RelationshipHashedRKey other && this.Equals(other);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Plc, RKeyHash);
        }



        public static bool operator ==(RelationshipHashedRKey left, RelationshipHashedRKey right)
        {
            return left.Equals(right);
        }

        public static bool operator !=(RelationshipHashedRKey left, RelationshipHashedRKey right)
        {
            return !(left == right);
        }
    }

}

