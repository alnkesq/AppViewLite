using ProtoBuf;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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

