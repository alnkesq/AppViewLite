using ProtoBuf;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Models
{
    [ProtoContract]
    public class ListData
    {
        [ProtoMember(1)] public string? DisplayName;
        [ProtoMember(2)] public string? Description;
        [ProtoMember(3)] public ListPurposeEnum Purpose;
        [ProtoMember(4)] public byte[]? AvatarCid;
    }
}

