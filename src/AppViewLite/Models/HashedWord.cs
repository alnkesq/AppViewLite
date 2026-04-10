using System;

namespace AppViewLite.Models
{
    public record struct HashedWord(ulong Hash) : IComparable<HashedWord>
    {
        public int CompareTo(HashedWord other)
        {
            return this.Hash.CompareTo(other.Hash);
        }
    }
}

