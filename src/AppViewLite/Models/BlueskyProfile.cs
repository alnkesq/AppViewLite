using AppViewLite.Numerics;

namespace AppViewLite.Models
{
    public class BlueskyProfile
    {
        public string Did;
        public int PlcId;
        public Plc Plc => new Plc(PlcId);

        public string? DisplayName => BasicData?.DisplayName;
        public string DisplayNameOrFallback => DisplayName ?? Did ?? PlcId.ToString();
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
    }
}

