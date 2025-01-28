using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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

