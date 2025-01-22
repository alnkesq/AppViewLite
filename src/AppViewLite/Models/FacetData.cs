using ProtoBuf;

namespace AppViewLite.Models
{
    [ProtoContract]
    public class FacetData
    {
        [ProtoMember(1)] public int Start;
        [ProtoMember(2)] public int Length;
        [ProtoMember(3)] public string Did;
        [ProtoMember(4)] public string Link;
        [ProtoMember(5)] public byte[] LinkBpe;
    }
}

