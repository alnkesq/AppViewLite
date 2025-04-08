using AppViewLite.Storage;
using System;
using System.IO;

namespace AppViewLite.Storage
{
    public class WhereSelectCache<TKey, TValue, TCacheKey, TCacheValue> : SlicedCacheBase<TKey, TValue, ImmutableMultiDictionaryReader<TCacheKey, TCacheValue>>
        where TKey : unmanaged, IComparable<TKey>
        where TValue : unmanaged, IComparable<TValue>, IEquatable<TValue>
        where TCacheKey : unmanaged, IComparable<TCacheKey>, IEquatable<TCacheKey>
        where TCacheValue : unmanaged, IComparable<TCacheValue>, IEquatable<TCacheValue>
    {
        private readonly string identifier;
        private readonly PersistentDictionaryBehavior behavior;
        private readonly Action<ImmutableMultiDictionaryReader<TKey, TValue>, ImmutableMultiDictionaryWriter<TCacheKey, TCacheValue>> materialize;
        public WhereSelectCache(string identifier, PersistentDictionaryBehavior behavior, Func<TKey, DangerousHugeReadOnlyMemory<TValue>, (TCacheKey CacheKey, TCacheValue[] CacheValues)> func)
            : this(identifier, behavior, (reader, writer) =>
            {
                foreach (var item in reader.Enumerate())
                {
                    var result = func(item.Key, item.Values);
                    if (result.CacheValues != null)
                    {
                        writer.AddPresorted(result.CacheKey, result.CacheValues.AsSpan());
                    }
                }
            })
        {

        }
        public WhereSelectCache(string identifier, PersistentDictionaryBehavior behavior, Action<ImmutableMultiDictionaryReader<TKey, TValue>, ImmutableMultiDictionaryWriter<TCacheKey, TCacheValue>> materialize)
        {
            this.identifier = identifier;
            this.behavior = behavior;
            this.materialize = materialize;
        }
        public override string Identifier => identifier;

        public override bool IsAlreadyMaterialized(string cachePath)
        {
            return File.Exists(cachePath + ".col0.dat");
        }
        public override void MaterializeCacheFile(CombinedPersistentMultiDictionary<TKey, TValue>.SliceInfo slice, string destination)
        {
            using var writer = new ImmutableMultiDictionaryWriter<TCacheKey, TCacheValue>(destination, behavior);
            materialize(slice.Reader, writer);
            writer.CommitAndGetSize();
        }

        protected override ImmutableMultiDictionaryReader<TCacheKey, TCacheValue> ReadCache(string cachePath)
        {
            return new ImmutableMultiDictionaryReader<TCacheKey, TCacheValue>(cachePath, behavior, allowEmpty: true);
        }

        public override void EnsureSupportsSourceBehavior(PersistentDictionaryBehavior behavior)
        {
        }

    }
}

