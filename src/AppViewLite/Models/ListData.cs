using ProtoBuf;

namespace AppViewLite.Models
{
    [ProtoContract]
    public class ListData
    {
        [ProtoMember(1)] public string? DisplayName;
        [ProtoMember(2)] public string? Description;
        [ProtoMember(3)] public ListPurposeEnum Purpose;
        [ProtoMember(4)] public byte[]? AvatarCid;
        [ProtoMember(5)] public FacetData[]? DescriptionFacets;
        public string? Error;
        public bool Deleted;
    }
}

