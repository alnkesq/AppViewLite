using FishyFlip.Models;
using AppViewLite.Numerics;
using System;
using AppViewLite.PluggableProtocols;
using System.Runtime.CompilerServices;
using System.Linq;
using System.Collections.Generic;

namespace AppViewLite.Models
{
    public class BlueskyPost
    {
        public required BlueskyProfile Author;
        public BlueskyProfile? RepostedBy;
        public DateTime? RepostDate;
        public required string RKey;

        public DateTime Date;

        public long LikeCount;
        public long RepostCount;
        public long QuoteCount;
        public long ReplyCount;

        public BlueskyPost? QuotedPost;

        public string BaseUrl => $"{Author.BaseUrl}/{RKey}";
        public string BlueskyUrl => $"{Author.BlueskyUrl}/post/{RKey}";
        public BlueskyPostData? Data;

        public BlueskyProfile? InReplyToUser;

        public PostId PostId;
        public string Did => Author.Did;
        public Plc AuthorId => Author.Plc;

        public BlueskyPost? InReplyToFullPost;
        public BlueskyPost? RootFullPost;
        public bool IsReply => Data?.InReplyToPlc != null || Data?.IsReplyToUnspecifiedPost == true;
        public bool IsNativeReply => Data?.InReplyToPlc != null;
        public bool IsRootPost => !IsReply;

        public PostId? InReplyToPostId => Data?.InReplyToPostId;
        public PostId RootPostId => Data?.RootPostId ?? this.PostId;

        public Tid? IsLikedBySelf;
        public Tid? IsBookmarkedBySelf;
        public Tid? IsRepostedBySelf;
        public bool IsRepost => RepostedBy != null;
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

        public BlueskyLabel[] Labels = [];
        public PluggableProtocol? PluggableProtocol => Author.PluggableProtocol;

        public PostIdString PostIdStr => new PostIdString(Did, RKey);

        public bool IsSelfRepost => RepostedBy?.Plc == PostId.Author;
        public bool IsNonSelfRepost => RepostedBy != null && !IsSelfRepost;
        public bool IsImagePost => Data?.Media != null;

        public QualifiedPluggablePostId QualifiedPluggablePostId => GetPluggablePostId(Did, Data?.PluggablePostId);
        public QualifiedPluggablePostId QualifiedPluggableInReplyToPostId => GetPluggablePostId(InReplyToUser?.Did, Data?.PluggableInReplyToPostId);

        private QualifiedPluggablePostId GetPluggablePostId(string? did, NonQualifiedPluggablePostId? postId)
        {
            if (postId == null) return new QualifiedPluggablePostId(did!, new NonQualifiedPluggablePostId(this.PostId.PostRKey));
            return new QualifiedPluggablePostId(did!, postId.Value);
        }

        public string? OriginalPostUrl => Author.PluggableProtocol?.TryGetOriginalPostUrl(QualifiedPluggablePostId, this);

        public IEnumerable<BlueskyModerationBase> AllLabels => this.Labels.Concat(this.Author.Labels);
        public BlockReasonDisplayStringAndList? GetBlurReason(bool isFocal, bool isQuotee, bool isThreadView, bool isQuoteList, RequestContext ctx)
        {
    
            var r = Author.BlockReason.ToDisplayStringWithList(BlockSubjects.YouAndAuthor, ctx);
            if (r != null)
                return r;

            if (isFocal) return null;

            var labelBlur = AllLabels.FirstOrDefault(x => x.Mode is ModerationBehavior.BlurAll or ModerationBehavior.Mute);
            if (labelBlur != null)
                return new BlockReasonDisplayStringAndList("[" + labelBlur.DisplayNameOrFallback + "]", labelBlur);

            if (!isThreadView && !isQuotee && !isQuoteList) return null;

            if (QuoterAndAuthorBlockReason != default)
                return QuoterAndAuthorBlockReason.ToDisplayStringWithList(BlockSubjects.QuoterAndAuthor, ctx);
            if (QuoteeAndAuthorBlockReason != default)
                return QuoteeAndAuthorBlockReason.ToDisplayStringWithList(BlockSubjects.QuoteeAndAuthor, ctx);

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

            if (FocalAndAuthorBlockReason.ToDisplayStringWithList(BlockSubjects.FocalAndAuthor, ctx) is { } s) return s;

            if (isQuotee) return null;

            if (isQuoteList) return null;

            return
                ParentAndAuthorBlockReason.ToDisplayStringWithList(BlockSubjects.ParentAndAuthor, ctx) ??
                RootAndAuthorBlockReason.ToDisplayStringWithList(BlockSubjects.RootAndAuthor, ctx);
            
        }

        public override string ToString()
        {
            return "[" + Author?.DisplayNameOrFallback + "] " + Data?.Text;
        }

        public bool IsMuted;
        public bool DidPopulateViewerFlags;
        public MuteRule? MutedByRule;
        internal bool ShouldMuteCore(RequestContext ctx)
        {
            if (RepostedBy != null)
            {
                if (IsSelfRepost)
                {
                    if (IsUserMuted(RepostedBy, PrivateFollowFlags.MuteImageSelfReposts, PrivateFollowFlags.MuteTextualSelfReposts))
                        return true;
                }
                else
                {
                    if (!Author.IsFollowedEvenPrivatelyBySelf)
                    {
                        if (IsUserMuted(RepostedBy, PrivateFollowFlags.MuteImageNonFollowedReposts, PrivateFollowFlags.MuteTextualNonFollowedReposts))
                            return true;
                    }
                }
            }

            if (!IsSelfRepost)
            {
                if (IsUserMuted(Author, PrivateFollowFlags.MuteImagePosts, PrivateFollowFlags.MuteTextualPosts))
                    return true;
            }

            if (Data != null && ctx.IsLoggedIn && ctx.UserContext.PrivateProfile!.MuteRules is { Length: >0 } muteRules)
            {
                var words = StringUtils.GetAllWords(Data.Text).ToArray();
                var urls = Data.GetExternalUrls().Distinct().Select(x => StringUtils.TryParseUri(x)).WhereNonNull().ToArray();
                this.MutedByRule = muteRules.FirstOrDefault(x => x.AppliesTo(words, urls, this));
                if (MutedByRule != null)
                {
                    return true;
                }
            }


            if (Labels!.Any(x => x.Mode == ModerationBehavior.Mute)) return true;

            return false;
            
        }

        private bool IsUserMuted(BlueskyProfile user, PrivateFollowFlags imageFlag, PrivateFollowFlags textFlag)
        {
            if (user.Labels!.Any(x => x.Mode == ModerationBehavior.Mute)) return true;
            if (IsImagePost)
            {
                return user.HasPrivateFollowFlag(imageFlag);
            }
            else
            {
                return user.HasPrivateFollowFlag(textFlag);
            }
        }

        public string? GetExternalDomainForMuteHint()
        {

            var postUtf8 = Data?.GetUtf8IfNeededByCompactFacets();
            var externalDomainForMute = (StringUtils.TryParseUri(Data?.ExternalUrl) ?? Data?.Facets?.Select(x => x.GetLink(postUtf8)).WhereNonNull().Select(x => StringUtils.TryParseUri(x)).Where(x => x != null && x.Host != "bsky.app").FirstOrDefault(x => x != null))?.Host;
            if (externalDomainForMute != null) externalDomainForMute = StringUtils.TrimWww(externalDomainForMute);

            return externalDomainForMute ?? QuotedPost?.GetExternalDomainForMuteHint();
        }

        public bool ShouldUseCompactView;
    }
}

