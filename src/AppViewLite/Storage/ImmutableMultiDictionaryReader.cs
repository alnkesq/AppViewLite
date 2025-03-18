using AppViewLite.Storage;
using System;
using System.Collections.Generic;
using AppViewLite;
using AppViewLite.Numerics;
using System.Runtime.CompilerServices;
using System.IO;
using System.Runtime.InteropServices;
using System.Linq;

namespace AppViewLite.Storage
{
    public class ImmutableMultiDictionaryReader<TKey, TValue> : IDisposable where TKey: unmanaged, IComparable<TKey> where TValue: unmanaged, IComparable<TValue>
    {
        private readonly SimpleColumnarReader columnarReader;
        public string PathPrefix;
        private readonly PersistentDictionaryBehavior behavior;
        private DangerousHugeReadOnlyMemory<TKey> pageKeys;
        private readonly MemoryMappedFileSlim? pageKeysMmap;
        public TKey MinimumKey { get; private set; }
        public TKey MaximumKey { get; private set; }
        public long SizeInBytes { get; private set; }

        public bool IsSingleValueOrKeySet => behavior is PersistentDictionaryBehavior.SingleValue or PersistentDictionaryBehavior.KeySetOnly;
        public bool HasOffsets => !IsSingleValueOrKeySet;
        public bool HasValues => behavior != PersistentDictionaryBehavior.KeySetOnly;

        public unsafe ImmutableMultiDictionaryReader(string pathPrefix, PersistentDictionaryBehavior behavior)
        {
            this.PathPrefix = pathPrefix;
            this.behavior = behavior;
            this.columnarReader = new SimpleColumnarReader(pathPrefix, 
                behavior == PersistentDictionaryBehavior.KeySetOnly ? 1 :
                behavior == PersistentDictionaryBehavior.SingleValue ? 2 :
                3);
            this.Keys = columnarReader.GetColumnHugeMemory<TKey>(0);
            if (HasOffsets)
                this.Offsets = columnarReader.GetColumnHugeMemory<UInt48>(2);

            if (KeyCount * Unsafe.SizeOf<TKey>() >= MinSizeBeforeKeyCache)
            {
                var keyCachePath = pathPrefix + ".keys" + KeyCountPerPage + ".cache";
                if (!File.Exists(keyCachePath))
                {
                    using (var cacheStream = new FileStream(keyCachePath + ".tmp", FileMode.Create, FileAccess.Write, FileShare.None))
                    {
                        var keySpan = Keys.Span;
                        for (long i = 0; i < keySpan.Length; i += KeyCountPerPage)
                        {
                            var startKey = keySpan[i];
                            cacheStream.Write(MemoryMarshal.AsBytes(new ReadOnlySpan<TKey>(in startKey)));
                        }
                        var maxKey = keySpan[keySpan.Length - 1];
                        cacheStream.Write(MemoryMarshal.AsBytes(new ReadOnlySpan<TKey>(in maxKey)));
                    }
                    File.Move(keyCachePath + ".tmp", keyCachePath);
                }
                this.pageKeysMmap = new MemoryMappedFileSlim(keyCachePath);
                var pageKeysPlusLast = new DangerousHugeReadOnlyMemory<TKey>((TKey*)(void*)pageKeysMmap.Pointer, pageKeysMmap.Length / Unsafe.SizeOf<TKey>());
                this.MinimumKey = pageKeysPlusLast[0];
                this.MaximumKey = pageKeysPlusLast[pageKeysPlusLast.Length - 1];
                this.pageKeys = pageKeysPlusLast.Slice(0, pageKeysPlusLast.Length - 1);
            }
            else
            {

                this.MinimumKey = Keys[0];
                this.MaximumKey = Keys[Keys.Length - 1];
            }
            this.SizeInBytes = 
                Keys.Length * Unsafe.SizeOf<TKey>() +
                (HasValues ? Values.Length * Unsafe.SizeOf<TValue>() : 0) + 
                (Offsets?.Length ?? 0) * Unsafe.SizeOf<UInt48>();

        }

        public IEnumerable<string> GetPotentiallyCorruptFiles()
        {
            if (IsPotentiallyCorrupt(((DangerousHugeReadOnlyMemory<TKey>)Keys)))
                yield return columnarReader.Columns[0].Path;

            if (behavior != PersistentDictionaryBehavior.KeySetOnly)
            {
                if (IsPotentiallyCorrupt(Values))
                    yield return columnarReader.Columns[1].Path;

                if (HasOffsets && Offsets!.Length >= 2 && IsPotentiallyCorrupt(((DangerousHugeReadOnlyMemory<UInt48>)Offsets!)))
                    yield return columnarReader.Columns[2].Path;
            }
        }

        private unsafe static bool IsPotentiallyCorrupt<T>(DangerousHugeReadOnlyMemory<T> column) where T : unmanaged
        {
            var span = new HugeReadOnlySpan<byte>((byte*)column.Pointer, column.Length * Unsafe.SizeOf<T>());
            var maxLength = (int)Math.Min(span.Length, 256);
            var begin = span.Slice(0, maxLength).AsSmallSpan;
            var end = span.Slice(span.Length - maxLength).AsSmallSpan;
            return !begin.ContainsAnyExcept((byte)0) || !end.ContainsAnyExcept((byte)0);
        }

        const int MinSizeBeforeKeyCache = 2 * 1024 * 1024;
        const int TargetPageSizeBytes = 4 * 1024;

        public unsafe readonly static int KeyCountPerPage = TargetPageSizeBytes / sizeof(TKey);


        public HugeReadOnlyMemory<TKey> Keys;
        public DangerousHugeReadOnlyMemory<TValue> Values => HasValues ? columnarReader.GetColumnDangerousHugeMemory<TValue>(1) : throw new InvalidOperationException();
        public HugeReadOnlyMemory<UInt48>? Offsets;

        public int ColumnCount => columnarReader.Columns.Count;

        public long KeyCount => Keys.Length;
        public long ValueCount => HasValues ? Values.Length : KeyCount;

        public void Dispose()
        {
            columnarReader.Dispose();
            pageKeysMmap?.Dispose();
            pageKeys = default;
        }

        public long GetIndex(TKey key)
        {
            var z = BinarySearch(key);
            return z < 0 ? -1 : z;
        }
        public bool ContainsKey(TKey key)
        {
            return GetIndex(key) != -1;
        }

        public int GetValueCount(long index)
        {
            if (index == -1) return 0;
            if (IsSingleValueOrKeySet)
                return 1;
            var offsets = this.Offsets!;
            MemoryInstrumentation.MaybeOnAccess(in offsets[index]);
            ulong startOffset = offsets[index];
            ulong endOffset = index == offsets.Length - 1 ? (ulong)Values.Length : offsets[index + 1];
            return checked((int)(endOffset - startOffset));
        }

        public int GetValueCount(TKey key) => GetValueCount(GetIndex(key));

        public bool Contains(TKey key, TValue value)
        {
            var keyIndex = GetIndex(key);
            if (keyIndex == -1) return false;
            var vals = GetValues(keyIndex);
            var index = vals.Span.BinarySearch(value);
            return index >= 0;
        }

        public DangerousHugeReadOnlyMemory<TValue> GetValues(TKey key) => GetValues(GetIndex(key));
        public DangerousHugeReadOnlyMemory<TValue> GetValues(TKey key, TValue? minExclusive, TValue? maxExclusive) => GetValues(GetIndex(key), minExclusive, maxExclusive);
        public DangerousHugeReadOnlyMemory<TValue> GetValues(long index, TValue? minExclusive, TValue? maxExclusive = null)
        {
            var vals = GetValues(index);
            if (vals.Length == 0) return vals;


            if (minExclusive != null)
            {
                var z = vals.Span.BinarySearch(minExclusive.Value);
                if (z >= 0)
                {
                    vals = vals.Slice(z + 1);
                }
                else
                {
                    z = ~z;
                    vals = vals.Slice(z);
                }
                if (vals.Length == 0) return vals;
            }

            if (maxExclusive != null)
            {
                var z = vals.Span.BinarySearch(maxExclusive.Value);
                if (z >= 0)
                {
                    vals = vals.Slice(0, z);
                }
                else
                {
                    z = ~z;
                    vals = vals.Slice(0, z);
                }
            }


            return vals;
        }
        public DangerousHugeReadOnlyMemory<TValue> GetValues(long index)
        {
            if (index == -1) return default;
            if (behavior == PersistentDictionaryBehavior.SingleValue) return Values.Slice(index, 1);
            if (behavior == PersistentDictionaryBehavior.KeySetOnly) return SingletonDefaultValue;
            var offset = Offsets![index];
            return Values.Slice(offset, GetValueCount(index));
        }
        private readonly static DangerousHugeReadOnlyMemory<TValue> SingletonDefaultValue = SingletonDefaultNativeMemory<TValue>.Singleton;

        public IEnumerable<(TKey Key, TValue Value)> EnumerateSingleValues()
        {
            var keys = this.Keys;
            var count = keys.Length;

            if (behavior == PersistentDictionaryBehavior.SingleValue)
            {
                var allValues = this.Values;

                for (long i = 0; i < count; i++)
                {
                    yield return (keys[i], allValues[i]);
                }
            }
            else if (behavior == PersistentDictionaryBehavior.KeySetOnly)
            {
                for (long i = 0; i < count; i++)
                {
                    yield return (keys[i], default);
                }
            }
            else throw new InvalidOperationException();
            
        }

        public IEnumerable<(TKey Key, DangerousHugeReadOnlyMemory<TValue> Values)> Enumerate()
        {
            var keys = this.Keys;
            var count = keys.Length;

            if (behavior == PersistentDictionaryBehavior.SingleValue)
            {
                var allValues = this.Values;
                for (long i = 0; i < count; i++)
                {
                    yield return (keys[i], allValues.Slice(i, 1));
                }
            }
            else if (behavior == PersistentDictionaryBehavior.KeySetOnly)
            {
                for (long i = 0; i < count; i++)
                {
                    yield return (keys[i], SingletonDefaultValue);
                }
            }
            else
            {
                var allValues = this.Values;
                var offsets = this.Offsets!;
                for (long i = 0; i < count; i++)
                {
                    var values = allValues.Slice(offsets[i], GetValueCount(i));
                    yield return (keys[i], values);
                }
            }
        }

        public long BinarySearch<TComparable>(TComparable comparable) where TComparable : IComparable<TKey>, allows ref struct
        {
            if (comparable.CompareTo(MaximumKey) > 0) return ~this.Keys.Length;
            if (comparable.CompareTo(MinimumKey) < 0) return ~0;

#if false
            var result = HugeSpanHelpers.BinarySearch(this.Keys.Span, comparable);

            if (pageKeys.Length != 0 && typeof(TKey) == typeof(TComparable))
            {
                var fast = BinarySearchPaginated(comparable);
                if (fast != result)
                    CombinedPersistentMultiDictionary.Abort(new Exception("Paginated binary search mismatch"));
            }
            return result;
#else


            if (pageKeys.Length != 0)
            {
                return BinarySearchPaginated(comparable);
            }
            else
            {
                return HugeSpanHelpers.BinarySearch(this.Keys.Span, comparable);
            }
#endif

        }

        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        private long BinarySearchPaginated<TComparable>(TComparable comparable) where TComparable : IComparable<TKey>, allows ref struct
        {
            var keyCacheSpan = this.pageKeys.Span;
            var pageIndex = HugeSpanHelpers.BinarySearch(keyCacheSpan, comparable);
            if (pageIndex < 0)
            {
                pageIndex = (~pageIndex) - 1;
                var keySpan = this.Keys.Span;
                var pageBaseIndex = pageIndex * KeyCountPerPage;
                var page =
                    (
                        pageIndex == keyCacheSpan.Length - 1 
                            ? keySpan.Slice(pageBaseIndex)
                            : keySpan.Slice(pageBaseIndex, KeyCountPerPage)
                    ).AsSmallSpan;

                var innerIndex = page.BinarySearch(comparable);
                if (innerIndex < 0)
                {

                    innerIndex = ~innerIndex;
                    return ~(pageBaseIndex + innerIndex);
                }
                else
                {
                    return pageBaseIndex + innerIndex;
                }
                
            }
            else
            {
                // this page starts exactly with the key we want.
                return pageIndex * KeyCountPerPage;
            }
        }
    }

    internal struct SingletonDefaultNativeMemory<T> where T : unmanaged
    {
        static unsafe SingletonDefaultNativeMemory()
        {
            var ptr = NativeMemory.AllocZeroed((nuint)Unsafe.SizeOf<T>());
            Singleton = new DangerousHugeReadOnlyMemory<T>((T*)ptr, 1);
        }
        public readonly static DangerousHugeReadOnlyMemory<T> Singleton;
    }
}

