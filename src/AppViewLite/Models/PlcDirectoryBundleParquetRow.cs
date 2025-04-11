using DuckDbSharp.Types;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Models
{
    public class PlcDirectoryBundleParquetRow
    {
        public DuckDbUuid PlcAsUInt128;
        public ushort EarliestDateApprox16;
        public string? BskySocialUserName;
        public string? CustomDomain;
        public string? Pds;
        public string[]? MultipleHandles;
        public string[]? OtherUrls;
        public string? AtProtoLabeler;
        public DateTime? Date;
    }
}

