using AppViewLite.Models;
using AppViewLite.Storage;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;

namespace AppViewLite
{
    internal class UserPairEngagementCache : SlicedCacheBase<Plc, PostEngagement, ImmutableMultiDictionaryReader<Plc, UserEngagementStats>>
    {
        public override string Identifier => "user-pair-stats-2";

        protected override ImmutableMultiDictionaryReader<Plc, UserEngagementStats> ReadCache(string cachePath)
        {
            return new ImmutableMultiDictionaryReader<Plc, UserEngagementStats>(cachePath, PersistentDictionaryBehavior.SortedValues, allowEmpty: true);
        }

        public override bool IsAlreadyMaterialized(string cachePath)
        {
            return File.Exists(cachePath + ".col2.dat");
        }
        public override void MaterializeCacheFileThreadSafe(CombinedPersistentMultiDictionary<Plc, PostEngagement>.SliceInfo slice, string destination)
        {
            using var dest = new ImmutableMultiDictionaryWriter<Plc, UserEngagementStats>(destination, PersistentDictionaryBehavior.SortedValues);
            var userStats = new Dictionary<Plc, UserEngagementStats>();
            foreach (var viewerGroup in slice.Reader.Enumerate())
            {
                userStats.Clear();

                var viewer = viewerGroup.Key;
                foreach (var post in viewerGroup.Values)
                {
                    ref var stats = ref CollectionsMarshal.GetValueRefOrAddDefault(userStats, post.PostId.Author, out var _);
                    stats.Target = post.PostId.Author;
                    var seenInFollowing = (post.Kind & PostEngagementKind.SeenInFollowingFeed) != 0;
                    if (seenInFollowing)
                        stats.FollowingSeenPosts++;
                    var engagement = post.Kind & ~PostEngagementKind.SeenInFollowingFeed;
                    if (engagement != default)
                    {
                        stats.EngagedPosts++;
                        if (seenInFollowing)
                            stats.FollowingEngagedPosts++;
                    }
                }
                dest.AddPresorted(viewer, userStats.Values.Where(x => x.FollowingSeenPosts >= 2 || x.EngagedPosts >= 1).Order());
            }
            dest.CommitAndGetSize();
        }

        public override void EnsureSupportsSourceBehavior(PersistentDictionaryBehavior behavior)
        {
            if (behavior == PersistentDictionaryBehavior.PreserveOrder) throw new NotSupportedException();
        }

        public override object? GetCounters()
        {
            var sizes = cacheSlices.Select(x => x.Cache.PageIndexSize).Where(x => x != 0).ToArray();
            var total = sizes.Sum();
            return new { KeyIndexSizesTotal = total, KeyIndexSizes = sizes };
        }

        public override void CheckProbabilisticSetHealthThreadSafe(ProbabilisticSetHealthCheckContext context)
        {
        }
    }
}
