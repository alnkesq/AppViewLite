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
    }
}

