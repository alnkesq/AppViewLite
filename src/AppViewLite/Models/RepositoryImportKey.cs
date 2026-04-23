using System;
using System.Runtime.InteropServices;

namespace AppViewLite.Models
{
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public record struct RepositoryImportKey(Plc Plc, DateTime ImportDate) : IComparable<RepositoryImportKey>
    {
        public readonly int CompareTo(RepositoryImportKey other)
        {
            var cmp = this.Plc.CompareTo(other.Plc);
            if (cmp != 0) return cmp;
            cmp = this.ImportDate.CompareTo(other.ImportDate);
            return cmp;
        }
    }
}

