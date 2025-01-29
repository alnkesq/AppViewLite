using ProtoBuf;

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


        [ProtoMember(14)] public string DisplayName;
        [ProtoMember(15)] public string Description;
        [ProtoMember(16)] public bool? Dummy;

        public string? AvatarCid;
        public string? BannerCid;
        public string? Error;
        
    }
}

