using Ipfs;
using AppViewLite.Numerics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Models
{
    public class BlueskyList : BlueskyModerationBase
    {
        public ListData? Data;
        public Relationship ListId;
        public override string DisplayNameOrFallback => Data?.DisplayName ?? (ModeratorDid + "/" + ListId.RelationshipRKey);

        public string BlueskyUrl => $"https://bsky.app/profile/{ModeratorDid}/lists/{ListId.RelationshipRKey}";
        public RelationshipStr ListIdStr;
        public string RKey => ListIdStr.RKey;
        public override string BaseUrl => $"{Moderator.BaseUrl}/lists/{RKey}";

        public string? AvatarUrl => BlueskyEnrichedApis.Instance.GetAvatarUrl(ModeratorDid, Data?.AvatarCid, Moderator.Pds);

        public override string? Description => Data?.Description;
        public override FacetData[]? DescriptionFacets => Data?.DescriptionFacets;

        public Tid? MembershipRkey;

        public override string GetAvatarUrl(RequestContext ctx)
        {
            return AvatarUrl ?? (Data?.Purpose is ListPurposeEnum.Curation ? $"/assets/colorized/default-feed-avatar-{ctx.AccentColor}.svg" : $"/assets/colorized/default-list-avatar-{ctx.AccentColor}.svg");
        }
    }
}

