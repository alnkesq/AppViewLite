using AppViewLite.Models;
using AppViewLite.Numerics;
using AppViewLite.Storage;
using System;
using System.IO;

namespace AppViewLite
{
    internal class RecentLikeRelationshipsCache : RelationshipProbabilisticCache<PostIdTimeFirst>
    {
        private readonly TimeSpan maxAge;
        public DateTime Threshold => DateTime.UtcNow - maxAge;
        private TimeSpan SafetyMargin = TimeSpan.FromHours(2);

        public RecentLikeRelationshipsCache(ProbabilisticSetParameters parameters, TimeSpan maxAge) : base(parameters)
        {
            this.maxAge = maxAge;
        }
        public override string Identifier => "recent-relset-" + parameters + "-" + maxAge.TotalHours;

        protected override void ReadInto(CombinedPersistentMultiDictionary<PostIdTimeFirst, Relationship>.SliceInfo slice, ProbabilisticSet<(PostIdTimeFirst, Plc)> cache)
        {
            var threshold = Tid.FromDateTime(Threshold);
            foreach (var group in slice.Reader.Enumerate())
            {
                var target = group.Key;
                if (target.PostRKey.CompareTo(threshold) >= 0)
                {
                    var valueSpan = group.Values.Span;
                    for (long i = 0; i < valueSpan.Length; i++)
                    {
                        cache.Add((target, valueSpan[i].Actor));
                    }
                }
            }
        }
        public override bool IsAlreadyMaterialized(string cachePath, CombinedPersistentMultiDictionary<PostIdTimeFirst, Relationship>.SliceInfo sourceSlice)
        {
            if (!File.Exists(cachePath)) return false;

            var cacheWasMaterializedAt = File.GetLastWriteTimeUtc(cachePath);
            var sourceEndsAt = sourceSlice.EndTime;

            var ageOnPreviousMaterialization = cacheWasMaterializedAt - sourceEndsAt;
            var ageNow = DateTime.UtcNow - sourceEndsAt;

            //if (ageWhenCacheWasMaterialized < TimeSpan.Zero) throw new Exception($"LastWriteTimeUtc of {cachePath} is antecedent to its source slice. This shouldn't normally happen.");

            if (ageOnPreviousMaterialization < maxAge && ageNow > maxAge)
            {
                // It's worth rebuilding this cache, because by now, it will be mostly empty (won't pollute the probabilistic set)
                return false;
            }

            return true;

        }



        public override void Add(PostIdTimeFirst key, Relationship value)
        {
            if (key.PostRKey.Date >= Threshold)
                base.Add(key, value);
        }

        public override bool PossiblyContains(PostIdTimeFirst target, Plc actor)
        {
            var stricterThreshold = Threshold + SafetyMargin;
            if (target.PostRKey.Date < stricterThreshold) return true;
            return base.PossiblyContains(target, actor);
        }
    }
}
