using ProtoBuf;

namespace AppViewLite.Models
{
    [ProtoContract]
    public class LabelConfiguration
    {
        [ProtoMember(1)] public required string LabelerDid;
        [ProtoMember(2)] public required string LabelName;
        [ProtoMember(3)] public ModerationBehavior Behavior;

    }
}

