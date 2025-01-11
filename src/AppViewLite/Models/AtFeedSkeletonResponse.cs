namespace AppViewLite.Models
{
    public class AtFeedSkeletonResponse
    {
        public required AtFeedSkeletonPost[] feed { get; set; }
        public string? cursor { get; set; }
    }
}
