using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Models
{
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public record struct PostEngagement(PostIdTimeFirst PostId, PostEngagementKind Kind) : IComparable<PostEngagement>
    {
        public int CompareTo(PostEngagement other)
        {
            var cmp = this.PostId.CompareTo(other.PostId);
            if (cmp != 0) return cmp;
            return this.Kind.CompareTo(other.Kind);
        }
    }
    public record struct PostEngagementStr(PostIdString PostId, PostEngagementKind Kind)
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

