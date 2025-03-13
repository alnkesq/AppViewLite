using AppViewLite.Models;
using AppViewLite.Numerics;
using AppViewLite.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite
{
    public class AppViewLitePruningContext : PruningContext
    {
        public required HashSet<Plc> PreserveUsers;
        public required HashSet<PostId> PreservePosts;
        public required Tid OldPostThreshold;

        public bool ShouldPreservePost(PostId postId)
        {
            if (postId.PostRKey.CompareTo(OldPostThreshold) >= 0) return true;
            if (ShouldPreserveUser(postId.Author)) return true;

            return false;
        }
        public bool ShouldPreserveUser(Plc user)
        {
            if (PreserveUsers.Contains(user)) return true;


            return false;
        }
    }
}

