using AppViewLite.Numerics;
using AppViewLite.PluggableProtocols;

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
                if (PluggableProtocol?.GetDisplayNameFromDid(Did) is { } username) return username;
                if (PossibleHandle != null)
                {
                    var at = PossibleHandle.IndexOf('@');
                    if (at != -1) return PossibleHandle.Substring(0, at);
                    var dot = PossibleHandle.IndexOf('.');
                    if (dot != -1) return PossibleHandle.Substring(0, dot);
                    else return PossibleHandle;
                }
                return Did ?? PlcId.ToString();
            }
        }

        public BlueskyLabel[]? Labels;

        public string BaseUrl => "/@" + (HandleIsUncertain || PossibleHandle == null ? Did : PossibleHandle);

        public string? FollowingUrl => PluggableProtocol != null ? PluggableProtocol.GetFollowingUrl(Did) : BaseUrl + "/following";
        public string? FollowersUrl => PluggableProtocol != null ? PluggableProtocol.GetFollowersUrl(Did) : BaseUrl + "/followers";

        public string BlueskyUrl => $"https://bsky.app/profile/{Did}";

        public BlueskyProfileBasicInfo? BasicData;
        public string? AvatarUrl
        {
            get
            {
                if (BasicData == null) return null; // still loading. Blank is better than generic avatar.
                return
                    BlueskyEnrichedApis.Instance.GetAvatarUrl(Did, BasicData.AvatarCidBytes, Pds)
                    ?? PluggableProtocol?.GetDefaultAvatar(Did)
                    ?? "/assets/default-user-avatar.svg";
            }
        }

        public Tid? RelationshipRKey;
        public BlockReason BlockReason;
        public bool IsYou;

        public Tid? IsFollowedBySelf;
        public bool FollowsYou;
        public bool HasBannerImage => BasicData?.BannerCidBytes != null;

        public string? PossibleHandle;
        public string? Pds;
        public bool HandleIsUncertain;
        public bool IsBlockedByAdministrativeRule;
        public bool IsMediaBlockedByAdministrativeRule;

        public ProfileBadge[]? Badges;
        public DidDocProto? DidDoc;

        public PluggableProtocol? PluggableProtocol;

        public override string ToString()
        {
            return DisplayNameOrFallback;
        }
    }
}

