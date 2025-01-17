using FishyFlip.Models;
using System;

namespace AppViewLite.Models
{
    public class BlueskyPost
    {
        public BlueskyProfile Author;
        public BlueskyProfile? RepostedBy;
        public DateTime? RepostDate;
        public string RKey;

        public DateTime Date;

        public long LikeCount;
        public long RepostCount;
        public long QuoteCount;
        public long ReplyCount;

        public BlueskyPost? QuotedPost;

        public string BaseUrl => $"/@{Author.Did}/{RKey}";
        public string BlueskyUrl => $"{Author.BlueskyUrl}/post/{RKey}";
        public BlueskyPostData? Data;

        public BlueskyProfile? InReplyToUser;

        public PostId PostId;
        public string Did => Author.Did;

        public BlueskyPost? InReplyToFullPost;
        public BlueskyPost? RootFullPost;
        public bool IsReply => Data?.InReplyToPlc != null;
        public bool IsRootPost => !IsReply;

        public PostId? InReplyToPostId => Data?.InReplyToPostId;
        public PostId RootPostId => Data?.RootPostId ?? this.PostId;

        public bool IsLikedBySelf;
        public bool IsRepostedBySelf;
        public bool IsRepost => RepostDate != null;
    }
}

