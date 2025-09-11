using AppViewLite.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite
{
    public abstract class BlueskyModerationBase
    {
        public string? ModeratorDid;
        public BlueskyProfile? Moderator;
        public abstract string DisplayNameOrFallback { get; }
        public string? NicknameOrDisplayName => PrivateNickname ?? DisplayNameOrFallback;
        public string? PrivateNickname;
        public abstract string? Description { get; }
        public abstract string? GetAvatarUrl(RequestContext ctx);
        public abstract string? BaseUrl { get; }
        public abstract FacetData[]? DescriptionFacets { get; }
        public ModerationBehavior Mode;

    }
}

