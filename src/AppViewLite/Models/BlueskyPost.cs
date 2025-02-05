using FishyFlip.Models;
using AppViewLite.Numerics;
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

        public Tid? IsLikedBySelf;
        public Tid? IsRepostedBySelf;
        public bool IsRepost => RepostDate != null;
        public object? EmbedRecord;


        public PostBlockReasonKind PostBlockReason;
        public BlockReason ParentAndAuthorBlockReason;
        public BlockReason RootAndAuthorBlockReason;
        public BlockReason QuoterAndAuthorBlockReason;
        public BlockReason QuoteeAndAuthorBlockReason;
        public BlockReason FocalAndAuthorBlockReason;

        //public BlueskyPostgate? Postgate;
        public BlueskyThreadgate? Threadgate;
        public bool ViolatesThreadgate;

        public bool IsSelfRepost => RepostedBy?.Plc == PostId.Author;
        public bool IsNonSelfRepost => RepostedBy != null && !IsSelfRepost;
        public bool IsImagePost => Data?.Media != null;

        public string? GetBlurReason(bool isFocal, bool isQuotee, bool isThreadView, bool isQuoteList)
        {
    
            var r = Author.BlockReason.ToDisplayString(BlockSubjects.YouAndAuthor);
            if (r != null)
                return r;

            if (isFocal) return null;

            if (!isThreadView && !isQuotee && !isQuoteList) return null;

            if (QuoterAndAuthorBlockReason != default)
                return QuoterAndAuthorBlockReason.ToDisplayString(BlockSubjects.QuoterAndAuthor);
            if (QuoteeAndAuthorBlockReason != default)
                return QuoteeAndAuthorBlockReason.ToDisplayString(BlockSubjects.QuoteeAndAuthor);

            if (PostBlockReason != PostBlockReasonKind.None)
            {

                if (isQuotee && !(PostBlockReason is PostBlockReasonKind.HiddenReply or PostBlockReasonKind.NotAllowlistedReply))
                    return null;

                return PostBlockReason switch
                {
                    PostBlockReasonKind.RemovedByQuotee => "Removed by author.",
                    PostBlockReasonKind.DisabledQuotes => "The author disabled quotes.",
                    PostBlockReasonKind.RemovedByQuoteeOnQuoter => "The quoted user dislikes this quote.",
                    PostBlockReasonKind.DisabledQuotesOnQuoter => "The quoted user requested not to be quoted.",

                    PostBlockReasonKind.HiddenReply => "This reply was hidden by the thread author.",
                    PostBlockReasonKind.NotAllowlistedReply => "The thread author turned off replies.",
                    _ => throw new NotSupportedException(),
                };
            }

            if (FocalAndAuthorBlockReason.ToDisplayString(BlockSubjects.FocalAndAuthor) is { } s) return s;

            if (isQuotee) return null;

            if (isQuoteList) return null;

            return
                ParentAndAuthorBlockReason.ToDisplayString(BlockSubjects.ParentAndAuthor) ??
                RootAndAuthorBlockReason.ToDisplayString(BlockSubjects.RootAndAuthor);
            
        }

        public override string ToString()
        {
            return "[" + Author?.DisplayNameOrFallback + "] " + Data?.Text;
        }
    }
}

