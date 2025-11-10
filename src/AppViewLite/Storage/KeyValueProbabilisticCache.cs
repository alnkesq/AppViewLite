using AppViewLite.Storage;
using System;

namespace AppViewLite.Storage
{
    public class KeyValueProbabilisticCache<TKey, TValue> : CombinedPersistentMultiDictionary<TKey, TValue>.CachedView where TKey : unmanaged, IComparable<TKey> where TValue : unmanaged, IComparable<TValue>, IEquatable<TValue>
    {
        private readonly ProbabilisticSetParameters parameters;
        private readonly ProbabilisticSet<(TKey, TValue)> probabilisticSet;
        public KeyValueProbabilisticCache(ProbabilisticSetParameters parameters)
        {
            this.parameters = parameters;
            this.probabilisticSet = new(parameters);
        }

        public override string Identifier => "kv-" + parameters;

        public override bool CanBeUsedByReplica => true;

        public override void LoadCacheFile(CombinedPersistentMultiDictionary<TKey, TValue>.SliceInfo slice, string cachePath, int sliceIndex)
        {
            probabilisticSet.UnionWith(ProbabilisticSetIo.ReadCompressedProbabilisticSetFromFile(cachePath));
        }

        public override void LoadFromOriginalSlice(CombinedPersistentMultiDictionary<TKey, TValue>.SliceInfo slice)
        {
            ReadInto(slice, probabilisticSet);
        }

        public override void MaterializeCacheFile(CombinedPersistentMultiDictionary<TKey, TValue>.SliceInfo slice, string destination)
        {
            var cache = new ProbabilisticSet<(TKey, TValue)>(parameters);
            ReadInto(slice, cache);
            ProbabilisticSetIo.WriteCompressedProbabilisticSetToFile(destination, cache);
        }

        private static void ReadInto(CombinedPersistentMultiDictionary<TKey, TValue>.SliceInfo slice, ProbabilisticSet<(TKey, TValue)> cache)
        {
            foreach (var group in slice.Reader.Enumerate())
            {
                var target = group.Key;
                var valueSpan = group.Values.Span;
                for (long i = 0; i < valueSpan.Length; i++)
                {
                    cache.Add((target, valueSpan[i]));
                }
            }
        }

        public override bool ShouldPersistCacheForSlice(CombinedPersistentMultiDictionary<TKey, TValue>.SliceInfo slice)
        {
            var sliceSize = slice.SizeInBytes;
            var cacheSize = probabilisticSet.SizeInBytes;
            return sliceSize * 16 > cacheSize;
        }

        public bool PossiblyContains(TKey key, TValue value)
        {
            return probabilisticSet.PossiblyContains((key, value));
        }

        public override void Add(TKey key, TValue value)
        {
            probabilisticSet.Add((key, value));
        }

        public override object? GetCounters()
        {
            return probabilisticSet.GetCounters();
        }

        public override void EnsureSupportsSourceBehavior(PersistentDictionaryBehavior behavior)
        {
            if (behavior == PersistentDictionaryBehavior.PreserveOrder) throw new NotSupportedException();
        }

        public override void CheckProbabilisticSetHealth(ProbabilisticSetHealthCheckContext context)
        {
            probabilisticSet.CheckProbabilisticSetHealth(context);
        }
    }

}
