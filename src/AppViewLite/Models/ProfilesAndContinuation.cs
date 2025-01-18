namespace AppViewLite.Models
{
    public record struct ProfilesAndContinuation(BlueskyProfile[] Profiles, string? NextContinuation)
    {
        public static implicit operator (BlueskyProfile[] Profiles, string? NextContinuation)(ProfilesAndContinuation value)
        {
            return (value.Profiles, value.NextContinuation);
        }

        public static implicit operator ProfilesAndContinuation((BlueskyProfile[] Profiles, string? NextContinuation) value)
        {
            return new ProfilesAndContinuation(value.Profiles, value.NextContinuation);
        }
    }
}



