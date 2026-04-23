using System;

namespace AppViewLite.Models
{
    public record struct Pds(int PdsId) : IComparable<Pds>
    {
        public readonly int CompareTo(Pds other)
        {
            return this.PdsId.CompareTo(other.PdsId);
        }

        public override readonly string ToString()
        {
            return $"Pds({PdsId})";
        }
    }
}

