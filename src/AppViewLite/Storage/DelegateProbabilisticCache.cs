using AppViewLite.Storage;
using System;

namespace AppViewLite.Storage
{
    public class DelegateProbabilisticCache<TKey, TValue, TProbabilisticKey> : CombinedPersistentMultiDictionary<TKey, TValue>.CachedView where TKey : unmanaged, IComparable<TKey> where TValue : unmanaged, IComparable<TValue>, IEquatable<TValue> where TProbabilisticKey : unmanaged
    {
        private readonly ProbabilisticSetParameters parameters;
        private readonly ProbabilisticSet<TProbabilisticKey> probabilisticSet;
        private readonly string baseIdentifier;
        private readonly Func<TKey, TValue, TProbabilisticKey> getProbabilisticKeyThreadSafe;
        public DelegateProbabilisticCache(string baseIdentifier, ProbabilisticSetParameters parameters, Func<TKey, TValue, TProbabilisticKey> getProbabilisticKeyThreadSafe)
        {
            this.baseIdentifier = baseIdentifier;
            this.parameters = parameters;
            this.getProbabilisticKeyThreadSafe = getProbabilisticKeyThreadSafe;
            this.probabilisticSet = new(parameters);
        }

        public override string Identifier => baseIdentifier + "-" + parameters;

        public override bool CanBeUsedByReplica => true;

        public override void LoadCacheFile(CombinedPersistentMultiDictionary<TKey, TValue>.SliceInfo slice, string cachePath, int sliceIndex)
        {
            probabilisticSet.UnionWith(ProbabilisticSetIo.ReadCompressedProbabilisticSetFromFile(cachePath));
        }

        public override void LoadFromOriginalSlice(CombinedPersistentMultiDictionary<TKey, TValue>.SliceInfo slice)
        {
            ReadInto(slice, probabilisticSet);
        }

        public override void MaterializeCacheFileThreadSafe(CombinedPersistentMultiDictionary<TKey, TValue>.SliceInfo slice, string destination)
        {
            var cache = new ProbabilisticSet<TProbabilisticKey>(parameters);
            ReadInto(slice, cache);
            ProbabilisticSetIo.WriteCompressedProbabilisticSetToFile(destination, cache);
        }

        private void ReadInto(CombinedPersistentMultiDictionary<TKey, TValue>.SliceInfo slice, ProbabilisticSet<TProbabilisticKey> cache)
        {
            foreach (var group in slice.Reader.Enumerate())
            {
                var target = group.Key;
                var valueSpan = group.Values.Span;
                for (long i = 0; i < valueSpan.Length; i++)
                {
                    cache.Add(getProbabilisticKeyThreadSafe(target, valueSpan[i]));
                }
            }
        }

        public override bool ShouldPersistCacheForSlice(CombinedPersistentMultiDictionary<TKey, TValue>.SliceInfo slice)
        {
            var sliceSize = slice.SizeInBytes;
            var cacheSize = probabilisticSet.SizeInBytes;
            return sliceSize * 16 > cacheSize;
        }

        public bool PossiblyContains(TProbabilisticKey probabilisticKey)
        {
            return probabilisticSet.PossiblyContains(probabilisticKey);
        }

        public override void Add(TKey key, TValue value)
        {
            probabilisticSet.Add(getProbabilisticKeyThreadSafe(key, value));
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
            this.probabilisticSet.CheckProbabilisticSetHealth(context);
        }
    }

}
