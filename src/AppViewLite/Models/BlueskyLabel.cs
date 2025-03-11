using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Models
{
    public class BlueskyLabel : BlueskyModerationBase
    {
        public Plc LabelerPlc => LabelId.Labeler;
        public required string Name;
        public BlueskyLabelData? Data;
        public string? DisplayName => Data?.DisplayName;
        public override string DisplayNameOrFallback => DisplayName ?? Name;

        public override string? Description => Data?.Description;

        public override string BaseUrl => Moderator.BaseUrl + "?labeler=1";

        public override FacetData[]? DescriptionFacets => null;

        public LabelId LabelId;

        public override string GetAvatarUrl(RequestContext ctx)
        {
            return Moderator.AvatarUrl!;
        }
    }
}

