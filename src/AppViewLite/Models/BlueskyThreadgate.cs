using ProtoBuf;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Models
{
    [ProtoContract]
    internal class BlueskyThreadgate
    {
        [ProtoMember(1)] public RelationshipProto[]? HiddenReplies;
        [ProtoMember(2)] public bool AllowlistedOnly;
        [ProtoMember(3)] public bool AllowMentioned;
        [ProtoMember(4)] public bool AllowFollowing;
        [ProtoMember(5)] public RelationshipProto[]? AllowLists;
        [ProtoMember(6)] public bool Dummy;
    }
}

