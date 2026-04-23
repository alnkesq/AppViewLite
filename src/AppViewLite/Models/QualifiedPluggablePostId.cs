using AppViewLite.Numerics;
using DuckDbSharp.Types;
using System;
using System.Runtime.InteropServices;

namespace AppViewLite.Models
{
    public readonly struct QualifiedPluggablePostId
    {
        public readonly string Did { get; }
        public readonly NonQualifiedPluggablePostId PostId { get; }


        public override readonly string ToString()
        {
            return Did + "/" + PostId.ToString();
        }

        public readonly Tid Tid => PostId.Tid;


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


        public readonly DuckDbUuid GetExternalPostIdHash()
        {
            return StringUtils.HashToUuid([
                ..MemoryMarshal.AsBytes<char>(Did),
                (byte)(PostId.String != null ? 1 : PostId.Bytes != null ? 2 : 3),
                ..MemoryMarshal.AsBytes<char>(PostId.String),
                ..PostId.Bytes.AsSpan(),
                ..MemoryMarshal.AsBytes<long>([PostId.Int64]),
            ]);
        }

        public readonly bool HasExternalIdentifier => PostId.HasExternalIdentifier;

        public readonly QualifiedPluggablePostId WithTid(Tid updatedTid) => new QualifiedPluggablePostId(Did, PostId.WithTid(updatedTid));
    }
}

