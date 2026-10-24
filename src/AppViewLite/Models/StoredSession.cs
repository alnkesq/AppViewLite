using ProtoBuf;
using System;
using System.Collections.Generic;
using System.Text;

namespace AppViewLite.Models
{
    [ProtoContract]
    public class StoredSession
    {
        [ProtoMember(1)] public required string Pds;
        [ProtoMember(2)] public required string AccessJwt;
        [ProtoMember(3)] public required string RefreshJwt;
        [ProtoMember(4)] public required DateTime RefreshJwtExpirationDate;
    }
}

