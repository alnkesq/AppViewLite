using AppViewLite.Numerics;
using System;
using System.Collections.Generic;
using System.Linq;
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

 
      
    }
}

