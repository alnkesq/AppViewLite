using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.RateLimiting;
using System.Threading.Tasks;

namespace AppViewLite
{
    public static class HostRateLimiter
    {
        private readonly static PartitionedRateLimiter<string> _rateLimiter;

        static HostRateLimiter()
        {
            string[] defaultLimits = ["plc.directory:50"];
            var qpsByHost = new Dictionary<string, double>();

            foreach (var item in defaultLimits.Concat(AppViewLiteConfiguration.GetStringList(AppViewLiteParameter.APPVIEWLITE_MAX_QPS_BY_HOST) ?? []))
            {
                var parts = item.Split(":", StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
                if (parts.Length != 2 || !double.TryParse(parts[1], out var maxQps) || !BlueskyEnrichedApis.IsValidDomain(parts[0]) || parts[0].StartsWith("www.", StringComparison.Ordinal))
                    throw new Exception("Invalid format for APPVIEWLITE_MAX_QPS_BY_HOST entry: " + item);
                qpsByHost[parts[0].ToLowerInvariant()] = maxQps;
            }

            _rateLimiter = PartitionedRateLimiter.Create<string, string>(host =>
            {
                return new RateLimitPartition<string>(host, h =>
                {
                    if (!(qpsByHost.TryGetValue(host, out var maxQps) || qpsByHost.TryGetValue("*", out maxQps)))
                        maxQps = 5;

                    var limiter = new TokenBucketRateLimiter(new TokenBucketRateLimiterOptions
                    {
                        TokenLimit = 1,
                        QueueProcessingOrder = QueueProcessingOrder.OldestFirst,
                        QueueLimit = int.MaxValue,
                        ReplenishmentPeriod = TimeSpan.FromSeconds(1.0 / maxQps),
                        TokensPerPeriod = 1,
                        AutoReplenishment = true
                    });
                    return limiter;
                });
            });
        }

        public static Task<RateLimitLease> AcquireUrlAsync(Uri url, CancellationToken ct = default) => AcquireHostAsync(url.Host, ct);

        public static async Task<RateLimitLease> AcquireHostAsync(string host, CancellationToken ct = default)
        {
            host = RemoveSubdomains(host);
            var lease = await _rateLimiter.AcquireAsync(host, cancellationToken: ct);
            if (!lease.IsAcquired)
                throw new Exception("Too many throttled requests are currently waiting to access " + host);
            return lease;
        }

        private static string RemoveSubdomains(string host)
        {
            host = host.ToLowerInvariant();
            var parts = host.Split('.');
            if (parts.Length is 1 or 2) return host;
            var secondLast = parts[parts.Length - 2];
            var keep = 2;
            if (secondLast is "gov" or "com" or "edu" or "co" or "org")
                keep = 3;
            return string.Join(".", parts.Skip(parts.Length - keep));
        }
    }
}

