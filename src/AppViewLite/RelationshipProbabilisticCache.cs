using AppViewLite.Models;
using AppViewLite.Storage;
using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using System.ServiceModel.Security;

namespace AppViewLite
{
    public class RelationshipProbabilisticCache<TTarget> : CombinedPersistentMultiDictionary<TTarget, Relationship>.CachedView where TTarget: unmanaged, IComparable<TTarget>
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


        public override void LoadCacheFile(string cachePath)
        {
            probabilisticSet.UnionWith(ProbabilisticSetCache.ReadCompressedProbabilisticSetFromFile(cachePath));
        }

        public override void LoadFromOriginalSlice(CombinedPersistentMultiDictionary<TTarget, Relationship>.SliceInfo slice)
        {
            ReadInto(slice, probabilisticSet);
        }

        public override void MaterializeCacheFile(CombinedPersistentMultiDictionary<TTarget, Relationship>.SliceInfo slice, string destination)
        {
            var cache = new ProbabilisticSet<(TTarget, Plc)>(sizeInBytes, hashFunctions);
            ReadInto(slice, cache);
            ProbabilisticSetCache.WriteCompressedProbabilisticSetToFile(destination, cache);
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

    public static class ProbabilisticSetCache
    {

        public static void WriteCompressedProbabilisticSetToFile<T>(string destination, ProbabilisticSet<T> probabilisticSet) where T : unmanaged
        {
            using (var stream = new System.IO.Compression.GZipStream(new FileStream(destination + ".tmp", FileMode.Create, FileAccess.Write, FileShare.None), System.IO.Compression.CompressionLevel.Fastest))
            {
                stream.Write(probabilisticSet.ArrayAsBytes);
            }
            File.Move(destination + ".tmp", destination, true);
        }

        public static IEnumerable<ReadOnlyMemory<ulong>> ReadCompressedProbabilisticSetFromFile(string path)
        {
            using var stream = new System.IO.Compression.GZipStream(new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read), System.IO.Compression.CompressionMode.Decompress);
            var buffer = new ulong[8 * 1024];
            while (true)
            {

                var bufferAsBytes = MemoryMarshal.AsBytes<ulong>(buffer);
                var readBytes = stream.ReadAtLeast(bufferAsBytes, bufferAsBytes.Length, throwOnEndOfStream: false);
                if (readBytes == 0) yield break;
                yield return new ReadOnlyMemory<ulong>(buffer, 0, readBytes / 8);
            }
        }
    }

}
