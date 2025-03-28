using AppViewLite.Storage;
using System;

namespace AppViewLite.Storage
{
    public class KeyValueProbabilisticCache<TKey, TValue> : CombinedPersistentMultiDictionary<TKey, TValue>.CachedView where TKey : unmanaged, IComparable<TKey> where TValue : unmanaged, IComparable<TValue>, IEquatable<TValue>
    {
        private readonly long sizeInBytes;
        private readonly int hashFunctions;
        private readonly ProbabilisticSet<(TKey, TValue)> probabilisticSet;
        public KeyValueProbabilisticCache(long sizeInBytes, int hashFunctions)
        {
            this.sizeInBytes = sizeInBytes;
            this.hashFunctions = hashFunctions;
            this.probabilisticSet = new(sizeInBytes, hashFunctions);
        }

        public override string Identifier => "kv-" + probabilisticSet.BitsPerFunction + "-" + hashFunctions;


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
            var cache = new ProbabilisticSet<(TKey, TValue)>(sizeInBytes, hashFunctions);
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
    }

}
