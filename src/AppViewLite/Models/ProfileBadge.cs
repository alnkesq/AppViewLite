using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Models
{
    public class ProfileBadge
    {
        public required string Kind;
        public string? Description;
        public string? Url;
        public string? Did;
        public string? Handle;
        public bool IsHandleBased;

    }

    public class ProfileBadgeWikidataParquet
    {
        public long WikidataId;
        public string? Handle;
        public bool IsOrganization;
        public bool IsGov;
    }
}

