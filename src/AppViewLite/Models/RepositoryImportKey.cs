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
    public record struct RepositoryImportKey(Plc Plc, DateTime ImportDate) : IComparable<RepositoryImportKey>
    {
        public int CompareTo(RepositoryImportKey other)
        {
            var cmp = this.Plc.CompareTo(other.Plc);
            if (cmp != 0) return cmp;
            cmp = this.ImportDate.CompareTo(other.ImportDate);
            return cmp;
        }
    }
}

