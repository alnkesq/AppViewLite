using AppViewLite.Numerics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Models
{
    internal record struct ProfilePostsContinuation(Tid MaxTidPosts, Tid MaxTidReposts, Tid MaxTidLikes, PostIdString[] FastReturnedPosts)
    {

        public string Serialize()
        {
            return $"{MaxTidPosts.TidValue}|{MaxTidReposts.TidValue}|{MaxTidLikes.TidValue}{string.Join(null, FastReturnedPosts.Select(x => "|" + x.Serialize()))}";
        }

        public static ProfilePostsContinuation Deserialize(string s)
        {
            var parts = s.Split('|');
            return new ProfilePostsContinuation(
                new Tid(long.Parse(parts[0])),
                new Tid(long.Parse(parts[1])),
                new Tid(long.Parse(parts[2])),
                parts.Skip(3).Select(PostIdString.Deserialize).ToArray()
                );
        }
    }
}

