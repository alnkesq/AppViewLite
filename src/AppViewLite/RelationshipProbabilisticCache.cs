using AppViewLite.Models;
using AppViewLite.Storage;
using System;

namespace AppViewLite
{
    public class RelationshipProbabilisticCache<TTarget> : CombinedPersistentMultiDictionary<TTarget, Relationship>.CachedView where TTarget : unmanaged, IComparable<TTarget>
    {
        private readonly long sizeInBytes;
        private readonly int hashFunctions;
        private readonly ProbabilisticSet<(TTarget, Plc)> probabilisticSet;
        public RelationshipProbabilisticCache(long sizeInBytes, int hashFunctions)
        {
            this.sizeInBytes = sizeInBytes;
            this.hashFunctions = hashFunctions;
            this.probabilisticSet = new(sizeInBytes, hashFunctions);
        }

        public override string Identifier => "relset-" + probabilisticSet.BitsPerFunction + "-" + hashFunctions;


        public override void LoadCacheFile(CombinedPersistentMultiDictionary<TTarget, Relationship>.SliceInfo slice, string cachePath, int sliceIndex)
        {
            probabilisticSet.UnionWith(ProbabilisticSetIo.ReadCompressedProbabilisticSetFromFile(cachePath));
        }

        public override void LoadFromOriginalSlice(CombinedPersistentMultiDictionary<TTarget, Relationship>.SliceInfo slice)
        {
            ReadInto(slice, probabilisticSet);
        }

        public override void MaterializeCacheFile(CombinedPersistentMultiDictionary<TTarget, Relationship>.SliceInfo slice, string destination)
        {
            var cache = new ProbabilisticSet<(TTarget, Plc)>(sizeInBytes, hashFunctions);
            ReadInto(slice, cache);
            ProbabilisticSetIo.WriteCompressedProbabilisticSetToFile(destination, cache);
        }

        private static void ReadInto(CombinedPersistentMultiDictionary<TTarget, Relationship>.SliceInfo slice, ProbabilisticSet<(TTarget, Plc)> cache)
        {
            foreach (var group in slice.Reader.Enumerate())
            {
                var target = group.Key;
                var valueSpan = group.Values.Span;
                for (long i = 0; i < valueSpan.Length; i++)
                {
                    cache.Add((target, valueSpan[i].Actor));
                }
            }
        }

        public override bool ShouldPersistCacheForSlice(CombinedPersistentMultiDictionary<TTarget, Relationship>.SliceInfo slice)
        {
            var sliceSize = slice.SizeInBytes;
            var cacheSize = probabilisticSet.SizeInBytes;
            return sliceSize * 16 > cacheSize;
        }

        public bool PossiblyContains(TTarget target, Plc actor)
        {
            return probabilisticSet.PossiblyContains((target, actor));
        }

        public override void Add(TTarget key, Relationship value)
        {
            probabilisticSet.Add((key, value.Actor));
        }
    }

}
