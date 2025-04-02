using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Models
{
    public record struct BlockReason(BlockReasonKind Kind, Relationship List)
    {

        public BlockReasonDisplayStringAndList? ToDisplayStringWithList(BlockSubjects subjects, RequestContext ctx)
        {
            var text = ToDisplayString(subjects);
            var listId = List;
            var list = text != null && listId != default ? BlueskyEnrichedApis.Instance.WithRelationshipsLock(rels => rels.GetList(listId, ctx: ctx), ctx) : null;
            return text != null ? new BlockReasonDisplayStringAndList(text, list) : null;
        }
        public string? ToDisplayString(BlockSubjects subjects)
        {
            if (this == default) return null;
            if (subjects == BlockSubjects.YouAndAuthor)
            {
                if (List != default)
                {
                    return Kind switch
                    {
                        BlockReasonKind.BlockedBy => "You are blocked, because of a [blocklist] you're in.",
                        BlockReasonKind.Blocks => "You are subscribed to a [blocklist] that includes this user.",
                        BlockReasonKind.MutualBlock => "You [block] each other.",
                        _ => throw AssertionLiteException.ThrowBadEnumException(Kind),
                    };
                }
                else
                {
                    return Kind switch
                    {
                        BlockReasonKind.BlockedBy => "You are blocked.",
                        BlockReasonKind.Blocks => "You block this user.",
                        BlockReasonKind.MutualBlock => "You block each other.",
                        _ => throw AssertionLiteException.ThrowBadEnumException(Kind),
                    };

                }
            }
            else if (subjects == BlockSubjects.RootAndAuthor)
            {
                if (List != default)
                {
                    return Kind switch
                    {
                        BlockReasonKind.BlockedBy => "This user is subscribed to a [blocklist] that includes the thread author.",
                        BlockReasonKind.Blocks => "The thread author is subscribed to a [blocklist] that includes this user.",
                        BlockReasonKind.MutualBlock => "The thread author and this user [block] each other.",
                        _ => throw AssertionLiteException.ThrowBadEnumException(Kind),
                    };
                }
                else
                {
                    return Kind switch
                    {
                        BlockReasonKind.BlockedBy => "This user blocks the thread author.",
                        BlockReasonKind.Blocks => "The thread author blocks this user.",
                        BlockReasonKind.MutualBlock => "The thread author and this user block each other.",
                        _ => throw AssertionLiteException.ThrowBadEnumException(Kind),
                    };
                }
            }
            else if (subjects == BlockSubjects.ParentAndAuthor)
            {
                if (List != default)
                {
                    return Kind switch
                    {
                        BlockReasonKind.BlockedBy => "This user is subscribed to a [blocklist] that includes the person they're replying to.",
                        BlockReasonKind.Blocks => "This user is in a [blocklist] that the person they're replying to is subscribed to.",
                        BlockReasonKind.MutualBlock => "This user and the person they're replying to [block] each other.",
                        _ => throw AssertionLiteException.ThrowBadEnumException(Kind),
                    };
                }
                else
                {
                    return Kind switch
                    {
                        BlockReasonKind.BlockedBy => "This user blocks the person they're replying to.",
                        BlockReasonKind.Blocks => "This user is blocked by the person they're replying to.",
                        BlockReasonKind.MutualBlock => "This user and the person they're replying to block each other.",
                        _ => throw AssertionLiteException.ThrowBadEnumException(Kind),
                    };
                }
            }
            else if (subjects == BlockSubjects.FocalAndAuthor)
            {
                if (List != default)
                {
                    return Kind switch
                    {
                        BlockReasonKind.BlockedBy => "This user is subscribed to a [blocklist] that includes the author of the focused post.",
                        BlockReasonKind.Blocks => "This user is in a [blocklist] that the author of the focused post is subscribed to.",
                        BlockReasonKind.MutualBlock => "This user and the author of the focused post [block] each other.",
                        _ => throw AssertionLiteException.ThrowBadEnumException(Kind),
                    };
                }
                else
                {
                    return Kind switch
                    {
                        BlockReasonKind.BlockedBy => "This user blocks the author of the focused post.",
                        BlockReasonKind.Blocks => "This user is blocked by the author of the focused post.",
                        BlockReasonKind.MutualBlock => "This user and the author of the focused post to block each other.",
                        _ => throw AssertionLiteException.ThrowBadEnumException(Kind),
                    };
                }
            }
            else if (subjects == BlockSubjects.QuoterAndAuthor)
            {
                if (List != default)
                {
                    return Kind switch
                    {
                        BlockReasonKind.BlockedBy => "This user is subscribed to a [blocklist] that includes the person quoting this.",
                        BlockReasonKind.Blocks => "The user quoting this is subscribed to a [blocklist] that includes this user.",
                        BlockReasonKind.MutualBlock => "This user and the person quoting this [block] each other.",
                        _ => throw AssertionLiteException.ThrowBadEnumException(Kind),
                    };
                }
                else
                {
                    return Kind switch
                    {
                        BlockReasonKind.BlockedBy => "This user blocks the person quoting this.",
                        BlockReasonKind.Blocks => "The person quoting this blocks this user.",
                        BlockReasonKind.MutualBlock => "This user and the person quoting this block each other.",
                        _ => throw AssertionLiteException.ThrowBadEnumException(Kind),
                    };
                }
            }
            else if (subjects == BlockSubjects.QuoteeAndAuthor)
            {
                if (List != default)
                {
                    return Kind switch
                    {
                        BlockReasonKind.BlockedBy => "The quoted user is subscribed to a [blocklist] that includes this person.",
                        BlockReasonKind.Blocks => "This user is subscribed to a [blocklist] that includes the quoted user.",
                        BlockReasonKind.MutualBlock => "This user and the person being quoted [block] each other.",
                        _ => throw AssertionLiteException.ThrowBadEnumException(Kind),
                    };
                }
                else
                {
                    return Kind switch
                    {
                        BlockReasonKind.BlockedBy => "The quoted user blocks this person.",
                        BlockReasonKind.Blocks => "The quoted user is blocked by this person.",
                        BlockReasonKind.MutualBlock => "This user and the person being quoted block each other.",
                        _ => throw AssertionLiteException.ThrowBadEnumException(Kind),
                    };
                }
            }
            else throw AssertionLiteException.ThrowBadEnumException(subjects);

        }
    }

    [Flags]
    public enum BlockReasonKind
    {
        None = 0,
        BlockedBy = 1,
        Blocks = 2,
        MutualBlock = BlockedBy | Blocks
    }

    public enum PostBlockReasonKind
    {
        None,
        RemovedByQuotee,
        DisabledQuotes,
        HiddenReply,
        NotAllowlistedReply,

        RemovedByQuoteeOnQuoter,
        DisabledQuotesOnQuoter,

    }

    public enum BlockSubjects
    {
        YouAndAuthor,
        ParentAndAuthor,
        RootAndAuthor,
        QuoterAndAuthor,
        QuoteeAndAuthor,
        FocalAndAuthor,
    }

    public record BlockReasonDisplayStringAndList(string DisplayText, BlueskyModerationBase? List)
    {
        public static implicit operator BlockReasonDisplayStringAndList?(string? text) => text != null ? new BlockReasonDisplayStringAndList(text, default) : null;

        public override string ToString()
        {
            return DisplayText;
        }

        public (string? Text, FacetData[]?) ToTextWithFacets()
        {
            if (List == null) return (DisplayText, null);

            var open = DisplayText.IndexOf('[');
            if (open == -1) return (DisplayText, null);
            var close = DisplayText.IndexOf(']');

            return (this.DisplayText.Replace("[", null).Replace("]", null), [new FacetData { Link = List.BaseUrl, Start = Encoding.UTF8.GetByteCount(DisplayText.AsSpan(0, open)), Length = Encoding.UTF8.GetByteCount(DisplayText.AsSpan(open + 1, close - open - 1)) }]);
        }
    }
}

