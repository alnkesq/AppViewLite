using AppViewLite.Models;
using AppViewLite.Storage;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;

namespace AppViewLite
{
    internal class UserPairEngagementCache : CombinedPersistentMultiDictionary<Plc, PostEngagement>.CachedView
    {
        public override string Identifier => "user-pair-stats-2";

        public override bool CanBeUsedByReplica => false;

        public List<(SliceName OriginalSlice, ImmutableMultiDictionaryReader<Plc, UserEngagementStats> Cache)> cacheSlices = new();

        public long Version = 1;

        public override void Add(Plc key, PostEngagement value)
        {
            // No live update.
        }

        public override void LoadCacheFile(CombinedPersistentMultiDictionary<Plc, PostEngagement>.SliceInfo originalSlice, string cachePath, int sliceIndex)
        {
            var cache = new ImmutableMultiDictionaryReader<Plc, UserEngagementStats>(cachePath, PersistentDictionaryBehavior.SortedValues);
            cacheSlices.Insert(sliceIndex, (originalSlice.SliceName, cache));
            BumpVersion();
        }


        public override void LoadFromOriginalSlice(CombinedPersistentMultiDictionary<Plc, PostEngagement>.SliceInfo slice)
        {
            AssertionLiteException.Throw("LoadFromOriginalSlice not supported for UserPairEngagementCache");
        }

        public override bool IsAlreadyMaterialized(string cachePath)
        {
            return File.Exists(cachePath + ".col2.dat");
        }
        public override void MaterializeCacheFile(CombinedPersistentMultiDictionary<Plc, PostEngagement>.SliceInfo slice, string destination)
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

        public override bool ShouldPersistCacheForSlice(CombinedPersistentMultiDictionary<Plc, PostEngagement>.SliceInfo slice)
        {
            return true;
        }

        public override void Dispose()
        {
            foreach (var slice in cacheSlices)
            {
                slice.Cache.Dispose();
            }
        }

        public override void OnSliceAdded(int insertedAt, CombinedPersistentMultiDictionary<Plc, PostEngagement>.SliceInfo slice)
        {
            var cachePath = GetCachePathForSlice(slice);
            MaterializeCacheFile(slice, cachePath);
            LoadCacheFile(slice, cachePath, insertedAt);
            BumpVersion();
        }

        public override void OnSliceRemoved(int removedAt)
        {
            cacheSlices[removedAt].Cache.Dispose();
            cacheSlices.RemoveAt(removedAt);
            BumpVersion();
        }

        private void BumpVersion()
        {
            Version++;
        }

        public override object? GetCounters()
        {
            return null;
        }
    }
}
