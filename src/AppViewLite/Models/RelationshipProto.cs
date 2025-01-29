using ProtoBuf;
using AppViewLite.Numerics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Models
{
    [ProtoContract]
    public class RelationshipProto
    {
        [ProtoMember(1)] public int Plc;
        [ProtoMember(2)] public long Tid;

        public static RelationshipProto FromPostId(PostId p) => new RelationshipProto { Plc = p.Author.PlcValue, Tid = p.PostRKey.TidValue };
        public PostId PostId => new PostId(new(Plc), new(Tid));
    }
}

