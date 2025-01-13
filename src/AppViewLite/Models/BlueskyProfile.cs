using AppViewLite.Numerics;

namespace AppViewLite.Models
{
    public class BlueskyProfile
    {
        public string Did;
        public int PlcId;

        public string? DisplayName => BasicData?.DisplayName;
        public string DisplayNameOrFallback => DisplayName ?? Did ?? PlcId.ToString();
        public string BaseUrl => "/@" + Did;
        public string BlueskyUrl => $"https://bsky.app/profile/{Did}";
        public string? AvatarCid => BasicData?.AvatarCid;

        public BlueskyProfileBasicInfo? BasicData;
        public string? AvatarUrl => AvatarCid != null ? $"https://cdn.bsky.app/img/avatar_thumbnail/plain/{Did}/{AvatarCid}@jpeg" : null;

        public Tid? RelationshipRKey;
    }
}

