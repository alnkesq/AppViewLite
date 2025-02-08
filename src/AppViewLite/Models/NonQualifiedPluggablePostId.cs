using AppViewLite.Numerics;
using ProtoBuf;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Models
{
    [ProtoContract]
    public struct NonQualifiedPluggablePostId
    {

        public string AsString => this.String ?? this.Int64.ToString();
        public override string ToString()
        {
            return Tid + " (" + AsString  + ")";
        }
        public static NonQualifiedPluggablePostId CreatePreferInt64(Tid tid, string s)
        {
            if (long.TryParse(s, out var num)) return new NonQualifiedPluggablePostId(tid, num);
            return new NonQualifiedPluggablePostId(tid, s);
        }
        public NonQualifiedPluggablePostId(Tid tid, long int64)
        {
            if (tid == default) throw new ArgumentException();
            if (int64 <= 0) throw new ArgumentException();
            this.Int64 = int64;
            this.Tid = tid;
        }
        public NonQualifiedPluggablePostId(Tid tid, string s)
        {
            if (tid == default) throw new ArgumentException();
            if (string.IsNullOrEmpty(s)) throw new ArgumentException();
            this.String = s;
            this.Tid = tid;
        }

        [ProtoMember(1)] public long Int64;
        [ProtoMember(2)] public string String;
        [ProtoIgnore] public Tid Tid;



    }
}

