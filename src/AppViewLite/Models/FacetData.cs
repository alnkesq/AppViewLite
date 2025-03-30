using ProtoBuf;
using DuckDbSharp.Types;
using System;
using System.Text;

namespace AppViewLite.Models
{
    [ProtoContract]
    public class FacetData
    {
        [ProtoMember(1)] public int Start;
        [ProtoMember(2)] public int Length;
        [ProtoMember(3)] public string? Did;
        [ProtoMember(4)] public string? Link;
        [ProtoMember(5)] public byte[]? LinkBpe;
        [ProtoMember(6)] public bool? SameLinkAsText;

        [ProtoMember(7)] public ulong? CustomEmojiHashHi;
        [ProtoMember(8)] public ulong? CustomEmojiHashLo;
        [ProtoMember(9)] public bool? Bold;
        [ProtoMember(10)] public string? InlineImageUrl;
        [ProtoMember(11)] public byte[]? InlineImageUrlBpe;
        [ProtoMember(12)] public string? InlineImageAlt;
        [ProtoMember(13)] public byte[]? InlineImageAltBpe;
        [ProtoMember(14)] public bool? Del;
        [ProtoMember(15)] public bool? Quote;

        public DuckDbUuid? CustomEmojiHash
        {
            get => CustomEmojiHashHi != null ? DuckDbUuid.FromUpperLowerFlat(CustomEmojiHashHi.Value, CustomEmojiHashLo!.Value) : null;
            set
            {
                if (value != null)
                {
                    CustomEmojiHashHi = value!.Value.GetUpperFlat();
                    CustomEmojiHashLo = value.Value.GetLower();
                }
                else
                {
                    CustomEmojiHashHi = null;
                    CustomEmojiHashLo = null;
                }
            }
        }

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
            {
                if (Start + Length < fullPostUtf8.Length) // post text could be trimmed
                    return Encoding.UTF8.GetString(fullPostUtf8.Slice(Start, Length));
            }
            return null;
        }
    }
}

