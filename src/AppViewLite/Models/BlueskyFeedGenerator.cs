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
        public BlueskyFeedGeneratorData? Data;
        public required BlueskyProfile Author;
        public RelationshipHashedRKey FeedId;
        public long LikeCount;
        public bool IsPinned;
        public ATUri AtUri => new ATUri($"at://{Did}/app.bsky.feed.generator/{RKey}");

        public string DisplayNameOrFallback => Data?.DisplayName ?? (Did + "/" + RKey);
        public Plc Plc => FeedId.Plc;
        public string BaseUrl => $"{Author.BaseUrl}/feed/{RKey}";

        public string? AvatarUrl => BlueskyEnrichedApis.Instance.GetAvatarUrl(Did, Data?.AvatarCid, Author.Pds);

        public string DisplayName => Data?.DisplayName ?? RKey;

        public ATUri Uri => new ATUri("at://" + Did + "/app.bsky.feed.generator/" + RKey);

        public RelationshipHashedRKey RKeyHash => new(Plc, RKey);

        public string? ImplementationDisplayName
        {
            get
            {
                var did = Data?.ImplementationDid;
                if (did == null) return null;
                if (!did.StartsWith("did:web:", StringComparison.Ordinal)) return did;
                var domain = did.Substring(8);
                domain = StringUtils.TrimWww(domain);
                if (domain.StartsWith("api.", StringComparison.Ordinal)) domain = domain.Substring(4);
                if (domain == "skyfeed.me") return "SkyFeed";
                if (domain == "bluefeed.app") return "BlueFeed";
                if (domain == "graze.social") return "Graze";
                if (domain == "blueskyfeedcreator.com") return "BlueSkyFeedCreator";
                return domain;
            }
        }
    }
}

