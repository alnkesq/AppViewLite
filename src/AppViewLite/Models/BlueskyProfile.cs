using AppViewLite.Numerics;

namespace AppViewLite.Models
{
    public class BlueskyProfile
    {
        public string Did;
        public int PlcId;
        public Plc Plc => new Plc(PlcId);

        public string? DisplayName => BasicData?.DisplayName;
        public string DisplayNameOrFallback
        {
            get
            {
                if (DisplayName != null) return DisplayName;
                if (PossibleHandle != null)
                {
                    var dot = PossibleHandle.IndexOf('.');
                    if (dot != -1) return PossibleHandle.Substring(0, dot);
                    else return PossibleHandle;
                }
                return Did ?? PlcId.ToString();
            }
        }

        public string BaseUrl => "/@" + Did;
        public string BlueskyUrl => $"https://bsky.app/profile/{Did}";
        public string? AvatarCid => BasicData?.AvatarCid;

        public BlueskyProfileBasicInfo? BasicData;
        public string? AvatarUrl => BlueskyEnrichedApis.GetAvatarUrl(Did, AvatarCid);

        public Tid? RelationshipRKey;
        public BlockReason BlockReason;
        public bool IsYou;

        public Tid? IsFollowedBySelf;
        public bool FollowsYou;
        public bool HasBannerImage => BasicData?.BannerCidBytes != null;

        public string? PossibleHandle;

        public ProfileBadge[]? Badges;

        public override string ToString()
        {
            return DisplayNameOrFallback;
        }
    }
}

