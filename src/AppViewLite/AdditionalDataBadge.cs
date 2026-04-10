using AppViewLite.Models;

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

