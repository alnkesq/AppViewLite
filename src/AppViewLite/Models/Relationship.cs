using AppViewLite.Numerics;
using System;
using System.Runtime.InteropServices;

namespace AppViewLite.Models
{
    [StructLayout(LayoutKind.Sequential, Pack = 4)]
    public record struct Relationship(Plc Actor, Tid RelationshipRKey) : IComparable<Relationship>
    {
        public int CompareTo(Relationship other)
        {
            var z = Actor.CompareTo(other.Actor);
            if (z != 0) return z;
            return RelationshipRKey.CompareTo(other.RelationshipRKey);
        }
    }
}

