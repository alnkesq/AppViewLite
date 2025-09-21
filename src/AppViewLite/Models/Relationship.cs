using AppViewLite.Numerics;
using System;
using System.Runtime.InteropServices;

namespace AppViewLite.Models
{
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public record struct Relationship(Plc Actor, Tid RelationshipRKey) : IComparable<Relationship>
    {

        public int CompareTo(Relationship other)
        {
            var z = Actor.CompareTo(other.Actor);
            if (z != 0) return z;
            return RelationshipRKey.CompareTo(other.RelationshipRKey);
        }

        public string Serialize() => Actor.PlcValue + "_" + RelationshipRKey.TidValue;
        public static Relationship Deserialize(string s)
        {
            var parts = s.Split('_');
            return new Relationship(new Plc(int.Parse(parts[0])), new Tid(long.Parse(parts[1])));
        }

        public override string ToString() => $"{nameof(Relationship)}({Actor.PlcValue}, {RelationshipRKey})";
    }
}

