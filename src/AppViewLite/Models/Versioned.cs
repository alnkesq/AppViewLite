namespace AppViewLite.Models
{
    public record struct Versioned<T>(T Value, long MinVersion)
    {
        public void BumpMinimumVersion(RequestContext ctx)
        {
            ctx.BumpMinimumVersion(MinVersion);
        }
        public void BumpMinimumVersion(ref long minVersion)
        {
            if (this.MinVersion > minVersion)
                minVersion = this.MinVersion;
        }
    }
}

