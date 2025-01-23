using ProtoBuf;
using System;
using System.Collections.Generic;

namespace AppViewLite.Models
{
    [ProtoContract]
    public class AppViewLiteProfileProto
    {
        [ProtoMember(1)] public DateTime FirstLogin;
        [ProtoMember(2)] public List<AppViewLiteSessionProto> Sessions;
        [ProtoMember(3)] public byte[] PdsSessionCbor;
    }

    [ProtoContract]
    public class AppViewLiteSessionProto
    {
        [ProtoMember(1)] public string SessionToken;
        [ProtoMember(2)] public DateTime LastSeen;
        [ProtoMember(3)] public bool IsReadOnlySimulation;

    }
}

