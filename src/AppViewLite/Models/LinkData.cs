using ProtoBuf;
using System;
using System.Collections.Generic;
using System.Text;

namespace AppViewLite.Models
{
    [ProtoContract]
    public class LinkData
    {
        [ProtoMember(1)] public string? Title;
        [ProtoMember(2)] public byte[]? IconCid;
        [ProtoMember(3)] public required string Url;
    }
}

