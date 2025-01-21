namespace AppViewLite.Models
{
    public record struct PostsAndContinuation(BlueskyPost[] Posts, string? NextContinuation)
    {
        public static implicit operator (BlueskyPost[] Posts, string? NextContinuation)(PostsAndContinuation value)
        {
            return (value.Posts, value.NextContinuation);
        }

        public static implicit operator PostsAndContinuation((BlueskyPost[] Posts, string? NextContinuation) value)
        {
            return new PostsAndContinuation(value.Posts, value.NextContinuation);
        }

        public PostsAndContinuation()
            : this([], null)
        {
        }
    }
}



