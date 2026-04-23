namespace AppViewLite.Models
{
    public record struct Versioned<T>(T Value, long MinVersion)
    {
        public readonly void BumpMinimumVersion(RequestContext ctx)
        {
            ctx.BumpMinimumVersion(MinVersion);
        }
        public readonly void BumpMinimumVersion(ref long minVersion)
        {
            if (this.MinVersion > minVersion)
                minVersion = this.MinVersion;
        }
    }
}

