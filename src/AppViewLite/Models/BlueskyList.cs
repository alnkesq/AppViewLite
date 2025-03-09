using Ipfs;
using AppViewLite.Numerics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Models
{
    public class BlueskyList
    {
        public required string Did;
        public ListData? Data;
        public Relationship ListId;
        public string DisplayNameOrFallback => Data?.DisplayName ?? (Did + "/" + ListId.RelationshipRKey);

        public string BlueskyUrl => $"https://bsky.app/profile/{Did}/lists/{ListId.RelationshipRKey}";
        public RelationshipStr ListIdStr;
        public required BlueskyProfile Author;
        public string RKey => ListIdStr.RKey;
        public string BaseUrl => $"{Author.BaseUrl}/lists/{RKey}";

        public string? AvatarUrl => BlueskyEnrichedApis.Instance.GetAvatarUrl(Did, Data?.AvatarCid, Author.Pds);

        public Tid? MembershipRkey;
    }
}

