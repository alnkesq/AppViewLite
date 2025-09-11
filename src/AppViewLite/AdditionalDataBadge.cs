using AppViewLite.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite
{
    internal class AdditionalDataBadge : BlueskyModerationBase
    {
        private readonly string text;
        public AdditionalDataBadge(string text)
        {
            this.text = text;
        }
        public override string DisplayNameOrFallback => text;

        public override string? Description => null;

        public override string? BaseUrl => null;

        public override FacetData[]? DescriptionFacets => null;

        public override string? GetAvatarUrl(RequestContext ctx)
        {
            return null;
        }
    }
}

