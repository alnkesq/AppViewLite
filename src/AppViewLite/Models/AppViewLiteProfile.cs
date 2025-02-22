using ProtoBuf;
using System;
using System.Collections.Generic;

namespace AppViewLite.Models
{
    [ProtoContract]
    public class AppViewLiteProfileProto
    {
        [ProtoMember(1)] public DateTime FirstLogin;
        [ProtoMember(2)] public List<AppViewLiteSessionProto>? Sessions;
        [ProtoMember(3)] public byte[]? PdsSessionCbor;
        [ProtoMember(4)] public PrivateFollow[]? PrivateFollows;
    }

    [ProtoContract]
    public class AppViewLiteSessionProto
    {
        [ProtoMember(1)] public required string SessionToken;
        [ProtoMember(2)] public DateTime LastSeen;
        [ProtoMember(3)] public bool IsReadOnlySimulation;

    }

    [ProtoContract]
    public class PrivateFollow
    {
        [ProtoMember(1)] public int Plc;
        [ProtoMember(2)] public PrivateFollowFlags Flags;
        [ProtoMember(3)] public DateTime DatePrivateFollowed;
    }


    [Flags]
    public enum PrivateFollowFlags : ulong
    { 
        None = 0,
        PrivateFollow = 1,

        MuteImageSelfReposts = 2,
        MuteTextualSelfReposts = 4,
        MuteAllSelfReposts = MuteImageSelfReposts | MuteTextualSelfReposts,

        MuteImageNonFollowedReposts = 8,
        MuteTextualNonFollowedReposts = 16,
        MuteAllNonFollowedReposts = MuteImageNonFollowedReposts | MuteTextualNonFollowedReposts,

        // 32, 64: never used, can recycle

        MuteImagePosts = 128,
        MuteTextualPosts = 256,
        MuteAllPosts = MuteImagePosts | MuteTextualPosts,
    }
}

