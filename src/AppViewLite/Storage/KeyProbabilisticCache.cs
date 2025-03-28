using AppViewLite.Storage;
using System;

namespace AppViewLite.Storage
{
    public class KeyProbabilisticCache<TKey, TValue> : CombinedPersistentMultiDictionary<TKey, TValue>.CachedView where TKey : unmanaged, IComparable<TKey> where TValue : unmanaged, IComparable<TValue>, IEquatable<TValue>
    {
        private readonly long sizeInBytes;
        private readonly int hashFunctions;
        private readonly ProbabilisticSet<TKey> probabilisticSet;
        public KeyProbabilisticCache(long sizeInBytes, int hashFunctions)
        {
            this.sizeInBytes = sizeInBytes;
            this.hashFunctions = hashFunctions;
            this.probabilisticSet = new(sizeInBytes, hashFunctions);
        }

        public override string Identifier => "k-" + probabilisticSet.BitsPerFunction + "-" + hashFunctions;


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
            var cache = new ProbabilisticSet<TKey>(sizeInBytes, hashFunctions);
            ReadInto(slice, cache);
            ProbabilisticSetIo.WriteCompressedProbabilisticSetToFile(destination, cache);
        }

        private static void ReadInto(CombinedPersistentMultiDictionary<TKey, TValue>.SliceInfo slice, ProbabilisticSet<TKey> cache)
        {
            foreach (var key in slice.Reader.EnumerateKeys())
            {
                cache.Add(key);
            }
        }

        public override bool ShouldPersistCacheForSlice(CombinedPersistentMultiDictionary<TKey, TValue>.SliceInfo slice)
        {
            var sliceSize = slice.SizeInBytes;
            var cacheSize = probabilisticSet.SizeInBytes;
            return sliceSize * 16 > cacheSize;
        }

        public bool PossiblyContainsKey(TKey key)
        {
            return probabilisticSet.PossiblyContains(key);
        }

        public override void Add(TKey key, TValue value)
        {
            probabilisticSet.Add(key);
        }
    }

}
