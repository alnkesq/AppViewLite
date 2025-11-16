using AppViewLite.Models;
using AppViewLite.Numerics;
using AppViewLite.Storage;
using System;

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
