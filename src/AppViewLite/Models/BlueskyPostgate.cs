using ProtoBuf;

namespace AppViewLite.Models
{
    [ProtoContract]
    public class BlueskyPostgate
    {
        [ProtoMember(1)] public RelationshipProto[]? DetachedEmbeddings;
        [ProtoMember(2)] public bool DisallowQuotes;
        [ProtoMember(3)] public bool Dummy;

    }
}

