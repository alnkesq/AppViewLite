using AppViewLite.Numerics;
using AppViewLite.PluggableProtocols;
using System;
using System.Collections.Generic;

namespace AppViewLite.Models
{
    public class BlueskyProfile
    {
        public required string Did;
        public int PlcId;
        public Plc Plc => new Plc(PlcId);
        public bool DidOmitDescription;
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

        public IReadOnlyList<BlueskyModerationBase> Labels = [];

        public string BaseUrl => "/@" + (HandleIsUncertain || PossibleHandle == null ? Did : PossibleHandle);

        public string? FollowingUrl => PluggableProtocol != null ? PluggableProtocol.GetFollowingUrl(Did) : BaseUrl + "/following";
        public string? FollowersUrl => PluggableProtocol != null ? PluggableProtocol.GetFollowersUrl(Did) : BaseUrl + "/followers";

        public string BlueskyUrl => $"https://bsky.app/profile/{Did}";

        public BlueskyProfileBasicInfo? BasicData;
        public AppViewLiteUserContext? UserContext;
        public string? AvatarUrl
        {
            get
            {
                if (BasicData == null) return null; // still loading. Blank is better than generic avatar.
                return
                    BlueskyEnrichedApis.Instance.GetAvatarUrl(Did, BasicData.AvatarCidBytes, Pds)
                    ?? PluggableProtocol?.GetDefaultAvatar(Did)
                    ?? $"/assets/colorized/default-{(DidDoc?.AtProtoLabeler != null ? "labeler" : "user")}-avatar-{UserContext?.PrivateProfile?.AccentColor ?? AccentColor.Blue}.svg";
            }
        }

        public Tid? RelationshipRKey;
        public BlockReason BlockReason;
        public bool IsYou;

        public Tid? IsFollowedBySelf;
        public Tid? IsBlockedBySelf;
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

        public PrivateFollow? PrivateFollow;

        public string FollowRKeyForAttribute => IsFollowedBySelf?.ToString() ?? (HasPrivateFollowFlag(PrivateFollowFlags.PrivateFollow) ? "x" : "-");
        public string BlockRKeyForAttribute => IsBlockedBySelf?.ToString() ?? "-";
        public int FollowsYouForAttribute => FollowsYou ? 1 : 0;

        public bool HasPrivateFollowFlag(PrivateFollowFlags flag) => (PrivateFollow!.Flags & flag) == flag;

        public bool IsFollowedEvenPrivatelyBySelf => IsFollowedBySelf != default || HasPrivateFollowFlag(PrivateFollowFlags.PrivateFollow);

        public override string ToString()
        {
            return DisplayNameOrFallback;
        }

        public (string? Text, FacetData[]? Facets) GetDisplayInverseFollowRelationship(RequestContext ctx)
        {
            var b = BlockReason.ToDisplayStringWithList(BlockSubjects.YouAndAuthor, ctx);
            if (b != null) return b.ToTextWithFacets();
            return ((FollowsYou ? "Follows you" : null), null);
        }
    }
}

