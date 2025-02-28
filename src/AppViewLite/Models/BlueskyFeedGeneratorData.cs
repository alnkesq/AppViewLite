using ProtoBuf;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Models
{
    [ProtoContract]
    public class BlueskyFeedGeneratorData
    {

        [ProtoMember(1)] public string? DisplayName;
        [ProtoMember(2)] public byte[]? AvatarCid;
        [ProtoMember(3)] public string? ImplementationDid;
        [ProtoMember(4)] public string? Description;
        [ProtoMember(5)] public DateTime RetrievalDate;
        [ProtoMember(6)] public FacetData[]? DescriptionFacets;
        [ProtoMember(7)] public bool IsVideo;
        [ProtoMember(8)] public bool? AcceptsInteractions;
        [ProtoMember(9)] public required string RKey;
        public bool Deleted;
        public string? Error;
    }
}

