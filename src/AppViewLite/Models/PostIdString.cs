using FishyFlip.Lexicon.App.Bsky.Feed;
using FishyFlip.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Models
{
    public record PostIdString(string Did, string RKey)
    {
        public ATUri ToUri() => new ATUri("at://" + Did + "/" + Post.RecordType + "/" + RKey);

        public string Serialize() => Did + "/" + RKey;
        public static PostIdString Deserialize(string s)
        {
            var a = s.Split('/');
            return new PostIdString(a[0], a[1]);
        }

        public override string ToString() => $"{nameof(PostIdString)}({Did}, {RKey})";
    }
}

