using System;
using System.Collections.Generic;
using System.Linq;

namespace AppViewLite.Storage
{
    public abstract class SlicedCacheBase<TKey, TValue, TSliceCache> : CombinedPersistentMultiDictionary<TKey, TValue>.CachedView
        where TKey : unmanaged, IComparable<TKey>
        where TValue : unmanaged, IComparable<TValue>, IEquatable<TValue>
    {

        public sealed override bool CanBeUsedByReplica => false;

        public List<(SliceName OriginalSlice, TSliceCache Cache)> cacheSlices = new();

        public long Version = 1;

        public sealed override void Add(TKey key, TValue value)
        {
            // No live update.
        }


        public sealed override void LoadCacheFile(CombinedPersistentMultiDictionary<TKey, TValue>.SliceInfo originalSlice, string cachePath, int sliceIndex)
        {
            var cache = ReadCache(cachePath);
            cacheSlices.Insert(sliceIndex, (originalSlice.SliceName, cache));
            BumpVersion();
        }


        public sealed override void OnSliceRemoved(int removedAt)
        {
            (cacheSlices[removedAt].Cache as IDisposable)?.Dispose();
            cacheSlices.RemoveAt(removedAt);
            BumpVersion();
        }

        private void BumpVersion()
        {
            Version++;
        }

        public sealed override void LoadFromOriginalSlice(CombinedPersistentMultiDictionary<TKey, TValue>.SliceInfo slice)
        {
            CombinedPersistentMultiDictionary.Abort("LoadFromOriginalSlice not supported for SlicedCacheBase");
        }

        protected abstract TSliceCache ReadCache(string cachePath);

        public sealed override bool ShouldPersistCacheForSlice(CombinedPersistentMultiDictionary<TKey, TValue>.SliceInfo slice)
        {
            return true;
        }

        public override void Dispose()
        {
            foreach (var slice in cacheSlices)
            {
                (slice.Cache as IDisposable)?.Dispose();
            }
        }

        public sealed override void OnSliceAdded(int insertedAt, CombinedPersistentMultiDictionary<TKey, TValue>.SliceInfo slice)
        {
            var cachePath = GetCachePathForSlice(slice);
            if (!IsAlreadyMaterialized(cachePath, slice))
            {
                MaterializeCacheFileThreadSafe(slice, cachePath);
            }
            LoadCacheFile(slice, cachePath, insertedAt);
            BumpVersion();
        }
        public override void PrematerializeSliceCacheOnBackgroundCompactation(CombinedPersistentMultiDictionary<TKey, TValue>.SliceInfo slice)
        {
            var cachePath = GetCachePathForSlice(slice);
            MaterializeCacheFileThreadSafe(slice, cachePath);
        }
        public override void AssertSliceCount(int count)
        {
            CombinedPersistentMultiDictionary.Assert(this.cacheSlices.Count == count);
        }
    }
}

