using ProtoBuf;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Models
{
    [ProtoContract]
    public class DidDocProto
    {
        [ProtoMember(1)] public string? BskySocialUserName;
        [ProtoMember(2)] public int? PdsId;
        [ProtoMember(3)] public string? CustomHandle;
        [ProtoMember(4)] public DateTime Date;
        [ProtoMember(5)] public string? Pds;

        public string Handle => (CustomHandle ?? (BskySocialUserName != null ? BskySocialUserName + ".bsky.social" : null))!;

        public override string ToString()
        {
            return $"{Date} {Handle}: {Pds ?? PdsId.ToString()}";
        }
    }
}

