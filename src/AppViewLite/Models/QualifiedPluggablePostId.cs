using AppViewLite.Numerics;
using DuckDbSharp.Types;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Models
{
    public struct QualifiedPluggablePostId 
    {
        public readonly string Did { get; }
        public readonly NonQualifiedPluggablePostId PostId { get; }


        public override string ToString()
        {
            return Did + "/" + PostId.ToString();
        }

        public Tid Tid => PostId.Tid;


        public QualifiedPluggablePostId(string did, NonQualifiedPluggablePostId postId)
        {
            ArgumentException.ThrowIfNullOrEmpty(did);
            this.Did = did;
            this.PostId = postId;
        }
        public QualifiedPluggablePostId(string did, Tid tid, string postId)
            : this(did, new NonQualifiedPluggablePostId(tid, postId))
        {
        }
        public QualifiedPluggablePostId(string did, Tid tid, long postId)
            : this(did, new NonQualifiedPluggablePostId(tid, postId))
        {
        }
        public QualifiedPluggablePostId(string did, Tid tid, byte[] postId)
            : this(did, new NonQualifiedPluggablePostId(tid, postId))
        {
        }


        public DuckDbUuid GetExternalPostIdHash()
        {
            return StringUtils.HashToUuid([
                ..MemoryMarshal.AsBytes<char>(Did),
                (byte)(PostId.String != null ? 1 : PostId.Bytes != null ? 2 : 3),
                ..MemoryMarshal.AsBytes<char>(PostId.String),
                ..PostId.Bytes.AsSpan(),
                ..MemoryMarshal.AsBytes<long>([PostId.Int64]),
            ]);
        }

        public bool HasExternalIdentifier => PostId.HasExternalIdentifier;

        public QualifiedPluggablePostId WithTid(Tid updatedTid) => new QualifiedPluggablePostId(Did, PostId.WithTid(updatedTid));
    }
}

