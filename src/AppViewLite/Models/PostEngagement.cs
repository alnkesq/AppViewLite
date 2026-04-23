using System;
using System.Runtime.InteropServices;

namespace AppViewLite.Models
{
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public record struct PostEngagement(PostIdTimeFirst PostId, PostEngagementKind Kind) : IComparable<PostEngagement>
    {
        public readonly int CompareTo(PostEngagement other)
        {
            var cmp = this.PostId.CompareTo(other.PostId);
            if (cmp != 0) return cmp;
            return this.Kind.CompareTo(other.Kind);
        }
    }
    public record struct PostEngagementStr(PostIdString PostId, PostEngagementKind Kind, RelationshipStr FromFeed, float Weight)
    {
    }

    [Flags]
    public enum PostEngagementKind : byte
    {
        None = 0,
        SeenInFollowingFeed = 1,
        OpenedThread = 2,
        ViewedInTheaterOrWatchedVideo = 4,
        Downloaded = 8,
        LikedOrBookmarked = 16,
        OpenedExternalLink = 32,
    }
}

