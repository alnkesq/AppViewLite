namespace AppViewLite.Models
{
    public record struct PostStatsNotification(PostId PostId, string Did, string RKey, long LikeCount, long RepostCount, long QuoteCount, long ReplyCount)
    {
    }
}

