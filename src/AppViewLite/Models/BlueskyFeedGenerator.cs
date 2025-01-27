using FishyFlip.Models;
using Ipfs;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Models
{
    public class BlueskyFeedGenerator
    {
        public required string Did;
        public required string RKey;
        public required BlueskyFeedGeneratorData Data;
        public BlueskyProfile Author;
        public RelationshipHashedRKey FeedId;
        public long LikeCount;
        public string DisplayNameOrFallback => Data.DisplayName ?? (Did + "/" + RKey);
        public Plc Plc => FeedId.Plc;
        public string BaseUrl => $"/feed/{Did}/{RKey}";

        public string AvatarUrl => BlueskyEnrichedApis.GetAvatarUrl(Did, Data.AvatarCid != null ? Cid.Read(Data.AvatarCid).ToString() : null);

        public string DisplayName => Data.DisplayName ?? RKey;

        public ATUri Uri => new ATUri("at://" + Did + "/app.bsky.feed.generator/" + RKey);
    }
}

