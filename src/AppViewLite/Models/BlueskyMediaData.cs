using ProtoBuf;

namespace AppViewLite.Models
{
    [ProtoContract]
    public class BlueskyMediaData
    {
        [ProtoMember(1)] public string? AltText;
        [ProtoMember(2)] public byte[] Cid;
        [ProtoMember(3)] public bool IsVideo;
    }
}

