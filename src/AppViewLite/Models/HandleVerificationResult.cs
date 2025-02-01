using AppViewLite.Numerics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Models
{
    public record struct HandleVerificationResult(ApproximateDateTime32 VerificationDate, Plc Plc) : IComparable<HandleVerificationResult>
    {
        public int CompareTo(HandleVerificationResult other)
        {
            var cmp = this.VerificationDate.CompareTo(other.VerificationDate);
            if (cmp != 0) return cmp;
            return this.Plc.CompareTo(other.Plc);

        }
    }
}

