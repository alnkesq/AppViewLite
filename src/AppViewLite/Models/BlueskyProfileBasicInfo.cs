using ProtoBuf;
using System;

namespace AppViewLite.Models
{
    [ProtoContract]
    public class BlueskyProfileBasicInfo
    {
        [ProtoMember(1)] public byte[]? DisplayNameBpe;
        [ProtoMember(2)] public byte[]? DescriptionBpe;
        [ProtoMember(3)] public byte[]? AvatarCidBytes;
        [ProtoMember(4)] public byte[]? BannerCidBytes;

        [ProtoMember(5)] public long? PinnedPostTid;


        [ProtoMember(14)] public string? DisplayName;
        [ProtoMember(15)] public string? Description;
        [ProtoMember(16)] public bool? Dummy;

        [ProtoMember(17)] public long? PluggableProtocolFollowerCount;
        [ProtoMember(18)] public long? PluggableProtocolFollowingCount;
        [ProtoMember(19)] public FacetData[]? DescriptionFacets;
        [ProtoMember(20)] public bool? HasExplicitFacets;

        [ProtoMember(21)] public CustomFieldProto[]? CustomFields;
        [ProtoMember(22)] public FacetData[]? DisplayNameFacets;

        public string? Error;
        
    }

    [ProtoContract]
    public class CustomFieldProto
    {
        [ProtoMember(1)] public string Name;
        [ProtoMember(2)] public string Value;
        [ProtoMember(3)] public DateTime? VerifiedAt;
    }
}

