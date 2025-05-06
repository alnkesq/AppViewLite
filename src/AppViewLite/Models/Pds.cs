using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Models
{
    public record struct Pds(int PdsId) : IComparable<Pds>
    {
        public int CompareTo(Pds other)
        {
            return this.PdsId.CompareTo(other.PdsId);
        }

        public override string ToString()
        {
            return $"Pds({PdsId})";
        }
    }
}

