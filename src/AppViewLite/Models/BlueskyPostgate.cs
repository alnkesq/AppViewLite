using ProtoBuf;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Models
{
    [ProtoContract]
    public class BlueskyPostgate
    {
        [ProtoMember(1)] public RelationshipProto[]? DetachedEmbeddings;
        [ProtoMember(2)] public bool DisallowQuotes;
        [ProtoMember(3)] public bool Dummy;
        
    }
}

