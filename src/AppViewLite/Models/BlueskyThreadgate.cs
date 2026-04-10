using ProtoBuf;
using System.Linq;

namespace AppViewLite.Models
{
    [ProtoContract]
    public class BlueskyThreadgate
    {
        [ProtoMember(1)] public RelationshipProto[]? HiddenReplies;
        [ProtoMember(2)] public bool AllowlistedOnly;
        [ProtoMember(3)] public bool AllowMentioned;
        [ProtoMember(4)] public bool AllowFollowing;
        [ProtoMember(5)] public RelationshipProto[]? AllowLists;
        [ProtoMember(6)] public bool Dummy;
        [ProtoMember(7)] public bool AllowFollowers;

        public bool IsHiddenReply(PostId postId)
        {
            return HiddenReplies?.Any(x => x.PostId == postId) == true;
        }
    }
}

