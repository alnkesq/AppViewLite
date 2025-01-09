using ProtoBuf;

namespace AppViewLite.Models
{
    [ProtoContract]
    public class BlueskyPostData
    {
        [ProtoMember(1)] public string? Text;

        [ProtoMember(2)] public int? QuotedPlc;
        [ProtoMember(3)] public long? QuotedRKey;

        [ProtoMember(4)] public int? InReplyToPlc;
        [ProtoMember(5)] public long? InReplyToRKey;

        [ProtoMember(6)] public string[]? ExternalLinks;
        [ProtoMember(7)] public BlueskyMediaData[]? Media;

        [ProtoIgnore] public string? Error;
        [ProtoIgnore] public bool? Deleted;

        [ProtoMember(8)] public string? ExternalTitle;
        [ProtoMember(9)] public string? ExternalUrl;
        [ProtoMember(10)] public string? ExternalDescription;
        [ProtoMember(11)] public byte[]? ExternalThumbCid;
        [ProtoMember(12)] public string? EmbedRecordUri;

        [ProtoMember(13)] public byte[]? TextBpe;

        [ProtoMember(14)] public FacetData[]? Facets;


    }
}

