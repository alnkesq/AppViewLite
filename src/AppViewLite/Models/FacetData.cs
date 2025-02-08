using ProtoBuf;
using System;
using System.Text;

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
        [ProtoMember(6)] public bool? SameLinkAsText;

        public bool IsLink => Link != null || SameLinkAsText == true;
        public int End => Start + Length;

        public bool IsDisjoint(FacetData other)
        {
            return
                this.End <= other.Start ||
                this.Start >= other.End;
        }

        public string? GetLink(ReadOnlySpan<byte> fullPostUtf8)
        {
            if (Link != null) return Link;
            if (SameLinkAsText == true)
                return Encoding.UTF8.GetString(fullPostUtf8.Slice(Start, Length));
            return null;
        }
    }
}

