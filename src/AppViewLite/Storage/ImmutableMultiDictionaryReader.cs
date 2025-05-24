using AppViewLite.Storage;
using System;
using System.Collections.Generic;
using AppViewLite;
using AppViewLite.Numerics;
using System.Runtime.CompilerServices;
using System.IO;
using System.Runtime.InteropServices;
using System.Linq;
using System.Diagnostics;

namespace AppViewLite.Storage
{
    public class ImmutableMultiDictionaryReader<TKey, TValue> : IDisposable where TKey : unmanaged, IComparable<TKey> where TValue : unmanaged, IComparable<TValue>
    {
        private readonly SimpleColumnarReader columnarReader;
        public string PathPrefix;
        private readonly PersistentDictionaryBehavior behavior;
        private DangerousHugeReadOnlyMemory<TKey> pageKeys;
        private readonly MemoryMappedFileSlim? pageKeysMmap;
        private readonly MemoryMappedFileSlim safeFileHandleKeys;
        private readonly MemoryMappedFileSlim? safeFileHandleValues;
        private readonly MemoryMappedFileSlim? safeFileHandleOffsets;

        public TKey MinimumKey { get; private set; }
        public TKey MaximumKey { get; private set; }
        public long SizeInBytes { get; private set; }
        public long KeyIndexSize => pageKeys.Length * Unsafe.SizeOf<TKey>();

        public bool IsSingleValueOrKeySet => behavior is PersistentDictionaryBehavior.SingleValue or PersistentDictionaryBehavior.KeySetOnly;
        public bool HasOffsets => !IsSingleValueOrKeySet;
        public bool HasValues => behavior != PersistentDictionaryBehavior.KeySetOnly;

        internal readonly Func<TKey, MultiDictionaryIoPreference>? GetIoPreferenceForKeyFunc;

        public MultiDictionaryIoPreference GetIoPreferenceForKey(TKey key)
        {
            if (GetIoPreferenceForKeyFunc != null) return GetIoPreferenceForKeyFunc(key);
            return default;
        }


        public unsafe ImmutableMultiDictionaryReader(string pathPrefix, PersistentDictionaryBehavior behavior, Func<TKey, MultiDictionaryIoPreference>? getIoPreference = null, bool allowEmpty = false)
        {
            this.PathPrefix = pathPrefix;
            this.behavior = behavior;
            this.columnarReader = new SimpleColumnarReader(pathPrefix,
                behavior == PersistentDictionaryBehavior.KeySetOnly ? 1 :
                behavior == PersistentDictionaryBehavior.SingleValue ? 2 :
                3);
            this.GetIoPreferenceForKeyFunc = getIoPreference;

            this.Keys = columnarReader.GetColumnHugeMemory<TKey>(0);
            this.safeFileHandleKeys = columnarReader.GetMemoryMappedFile(0);

            if (!allowEmpty && Keys.Length == 0) throw new Exception("Keys column is an empty file: " + safeFileHandleKeys.Length);

            if (HasValues)
            {
                this.safeFileHandleValues = columnarReader.GetMemoryMappedFile(1);
                if (behavior == PersistentDictionaryBehavior.SingleValue)
                {
                    if (columnarReader.GetColumnHugeSpan<TValue>(1).Length != Keys.Length)
                        throw new Exception("Value column should have the same number of elements as the key column. Was this file truncated due to a partial file copy? " + safeFileHandleValues.Path);
                }
            }

            if (HasOffsets)
            {
                this.Offsets = columnarReader.GetColumnHugeMemory<UInt48>(2);
                this.safeFileHandleOffsets = columnarReader.GetMemoryMappedFile(2);

                if (Offsets.Length != Keys.Length)
                    throw new Exception("Offsets column should have the same number of elements as the key column. Was this file truncated due to a partial file copy? " + safeFileHandleOffsets.Path);

                if (Values.Length < Offsets.Length)
                    throw new Exception("Value column should have at least as many entries as the offsets column. Was this file truncated due to a partial file copy? " + safeFileHandleValues!.Path);

                if (Offsets.Length != 0 && Values.Length <= Offsets[Offsets.Length - 1])
                    throw new Exception("Value column is smaller than it should be according to offsets file. Was this file truncated due to a partial file copy? " + safeFileHandleValues!.Path);
            }

            if (KeyCount * Unsafe.SizeOf<TKey>() >= MinSizeBeforeKeyCache)
            {
                var keyCachePath = CombinedPersistentMultiDictionary.ToPhysicalPath(pathPrefix + ".keys" + KeyCountPerPage + ".cache");
                if (!File.Exists(keyCachePath))
                {
                    Directory.CreateDirectory(Path.GetDirectoryName(keyCachePath)!);
                    CombinedPersistentMultiDictionary.Log("Materializing cache " + keyCachePath);
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

                if (Keys.Length != 0)
                {
                    this.MinimumKey = Keys[0];
                    this.MaximumKey = Keys[Keys.Length - 1];
                }
            }
            this.SizeInBytes =
                Keys.Length * Unsafe.SizeOf<TKey>() +
                (HasValues ? Values.Length * Unsafe.SizeOf<TValue>() : 0) +
                (Offsets?.Length ?? 0) * Unsafe.SizeOf<UInt48>();

        }

        public IEnumerable<string> GetPotentiallyCorruptFiles(bool defaultKeysAreValid, bool defaultValuesAreValid)
        {
            if (IsPotentiallyCorrupt(((DangerousHugeReadOnlyMemory<TKey>)Keys)) && !(defaultKeysAreValid && KeyCount < 512))
                yield return columnarReader.Columns[0].Path;

            if (behavior != PersistentDictionaryBehavior.KeySetOnly)
            {
                if (IsPotentiallyCorrupt(Values) && !(defaultValuesAreValid && ValueCount < 512))
                    yield return columnarReader.Columns[1].Path;

                if (HasOffsets && Offsets!.Length >= 2 && IsPotentiallyCorrupt(((DangerousHugeReadOnlyMemory<UInt48>)Offsets!)))
                    yield return columnarReader.Columns[2].Path;
            }
        }

        private unsafe static bool IsPotentiallyCorrupt<T>(DangerousHugeReadOnlyMemory<T> column) where T : unmanaged
        {
            var span = new HugeReadOnlySpan<byte>((byte*)column.Pointer, column.Length * Unsafe.SizeOf<T>());
            var maxLength = (int)Math.Min(span.Length, 256);
            var begin = span.Slice(0, maxLength).AsSmallSpan();
            var end = span.Slice(span.Length - maxLength).AsSmallSpan();
            return !begin.ContainsAnyExcept((byte)0) || !end.ContainsAnyExcept((byte)0);
        }

        const int MinSizeBeforeKeyCache = 1 * 1024 * 1024;

        public readonly static int TargetPageSizeBytes =
            CombinedPersistentMultiDictionary.UseDirectIo ? CombinedPersistentMultiDictionary.DiskSectorSize
            : 4 * 1024; // OS page size

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

        public long GetIndex(TKey key, MultiDictionaryIoPreference preference)
        {
            var z = BinarySearch(key, preference);
            return z < 0 ? -1 : z;
        }
        public bool ContainsKey(TKey key, MultiDictionaryIoPreference preference)
        {
            return GetIndex(key, preference) != -1;
        }

        public (long Offset, int Count) GetValueCountAndOffset(long index, MultiDictionaryIoPreference preference)
        {
            if (index == -1) return default;
            if (IsSingleValueOrKeySet)
            {
                return (index, 1);
            }
            if (AlignedNativeArena.ForCurrentThread != null && GetOffsetsIoPreference(preference) != IoMethodPreference.Mmap)
            {
                if (index == KeyCount - 1)
                {
                    var offset = ReadSingleOffset(index, preference);
                    return (offset, checked((int)(ValueCount - offset)));
                }
                else
                {
                    var offsets = ReadOffsetSpan(index, 2, preference);
                    return (offsets[0], checked((int)(offsets[1] - offsets[0])));
                }
            }
            else
            {
                var offsets = this.Offsets!;
                MemoryInstrumentation.MaybeOnAccess(in offsets[index]);
                ulong startOffset = offsets[index];
                ulong endOffset = index == offsets.Length - 1 ? (ulong)Values.Length : offsets[index + 1];
                return ((long)startOffset, checked((int)(endOffset - startOffset)));
            }
        }

        public int GetValueCount(long index, MultiDictionaryIoPreference preference)
        {
            if (index == -1) return 0;
            if (IsSingleValueOrKeySet) return 1;
            return GetValueCountAndOffset(index, preference).Count;
        }

        public int GetValueCount(TKey key, MultiDictionaryIoPreference preference) => GetValueCount(GetIndex(key, preference), preference);

        public bool Contains(TKey key, TValue value, MultiDictionaryIoPreference preference)
        {
            var keyIndex = GetIndex(key, preference);
            if (keyIndex == -1) return false;
            var vals = GetValues(keyIndex, preference);
            var index = vals.Span.BinarySearch(value);
            return index >= 0;
        }

        public void InitializeIoPreferenceForKey(TKey key, ref MultiDictionaryIoPreference preference)
        {
            preference |= GetIoPreferenceForKey(key);
        }

        public DangerousHugeReadOnlyMemory<TValue> GetValues(TKey key, MultiDictionaryIoPreference preference = default)
        {
            InitializeIoPreferenceForKey(key, ref preference);
            return GetValues(GetIndex(key, preference), preference);
        }

        public DangerousHugeReadOnlyMemory<TValue> GetValues(TKey key, TValue? minExclusive, TValue? maxExclusive, MultiDictionaryIoPreference preference = default)
        {
            InitializeIoPreferenceForKey(key, ref preference);
            return GetValues(GetIndex(key, preference), minExclusive, maxExclusive, preference);
        }

        private DangerousHugeReadOnlyMemory<TValue> GetValues(long index, TValue? minExclusive, TValue? maxExclusive, MultiDictionaryIoPreference preference)
        {
            var vals = GetValues(index, preference);
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
        public DangerousHugeReadOnlyMemory<TValue> GetValues(long index, MultiDictionaryIoPreference preference)
        {
            if (index == -1) return default;
            if (behavior == PersistentDictionaryBehavior.SingleValue) return ReadValueSpan(index, 1, preference);
            if (behavior == PersistentDictionaryBehavior.KeySetOnly) return SingletonDefaultValue;
            var position = GetValueCountAndOffset(index, preference);
            return ReadValueSpan(position.Offset, position.Count, preference);
        }


        public DangerousHugeReadOnlyMemory<TKey> ReadKeySpan(long index, long length, MultiDictionaryIoPreference preference)
        {
            return ReadSpanCore(safeFileHandleKeys, (DangerousHugeReadOnlyMemory<TKey>)Keys, index, length, GetKeysIoPreference(preference));
        }
        public DangerousHugeReadOnlyMemory<UInt48> ReadOffsetSpan(long index, long length, MultiDictionaryIoPreference preference)
        {
            return ReadSpanCore(safeFileHandleOffsets!, (DangerousHugeReadOnlyMemory<UInt48>)Offsets!, index, length, GetOffsetsIoPreference(preference));
        }
        public DangerousHugeReadOnlyMemory<TValue> ReadValueSpan(long index, long length, MultiDictionaryIoPreference preference)
        {
            return ReadSpanCore(safeFileHandleValues!, Values, index, length, GetValuesIoPreference(preference));
        }


        public TKey ReadSingleKey(long index, MultiDictionaryIoPreference preference) => ReadSingleCore(safeFileHandleKeys, ((DangerousHugeReadOnlyMemory<TKey>)Keys), index, GetKeysIoPreference(preference));
        public UInt48 ReadSingleOffset(long index, MultiDictionaryIoPreference preference) => ReadSingleCore(safeFileHandleOffsets!, ((DangerousHugeReadOnlyMemory<UInt48>)Offsets!), index, GetOffsetsIoPreference(preference));
        public TValue ReadSingleValue(long index, MultiDictionaryIoPreference preference) => ReadSingleCore(safeFileHandleValues!, Values, index, GetValuesIoPreference(preference));


        private const int MaxDirectIoReadSize = 8 * 1024 * 1024;

        public unsafe T ReadSingleCore<T>(MemoryMappedFileSlim fileHandle, DangerousHugeReadOnlyMemory<T> hugeSpan, long index, IoMethodPreference preference) where T : unmanaged
        {
            if (AlignedNativeArena.ForCurrentThread != null && preference != IoMethodPreference.Mmap)
            {
                return ReadSpanCore(fileHandle, hugeSpan, index, 1, preference)[0];
            }
            else
            {
                return hugeSpan[index];
            }
        }

        public unsafe DangerousHugeReadOnlyMemory<T> ReadSpanCore<T>(MemoryMappedFileSlim fileHandle, DangerousHugeReadOnlyMemory<T> hugeSpan, long index, long length, IoMethodPreference preference) where T : unmanaged
        {
            CombinedPersistentMultiDictionary.Assert(index >= 0);
            var directIoArena = AlignedNativeArena.ForCurrentThread;
            if (directIoArena != null && preference != IoMethodPreference.Mmap)
            {
                var lengthInBytes = length * Unsafe.SizeOf<T>();
                if (lengthInBytes < MaxDirectIoReadSize && fileHandle.Length >= MinSizeBeforeKeyCache)
                {
                    var offsetInBytes = index * Unsafe.SizeOf<T>();

                    if (offsetInBytes + lengthInBytes + (long)directIoArena.Alignment < (long)fileHandle.Length) // reads very close to the end can't be done (end might not be aligned)
                    {
                        if (false)
                        {
                            var s = Stopwatch.GetTimestamp();
                            while (Stopwatch.GetElapsedTime(s).TotalMilliseconds < 20)
                            {
                            }
                        }
                        if (CombinedPersistentMultiDictionary.PrintDirectIoReads)
                        {
                            var sliceKind = Path.GetFileName(fileHandle.Path);
                            var kind = sliceKind.Substring(sliceKind.IndexOf('.') + 1);
                            var sliceKey = Path.GetFileName(Path.GetDirectoryName(fileHandle.Path)) + "_" + kind;
                            CombinedPersistentMultiDictionary.DirectIoReadStats.AddOrUpdate(sliceKey, 0, (_, prev) => prev += lengthInBytes);

                            bool isDuplicateRead = false;
                            lock (fileHandle.RecentReadsForDebugging)
                            {
                                if (fileHandle.RecentReadsForDebugging.Any(x => x.File == fileHandle && Math.Abs(x.Offset - offsetInBytes) < 200))
                                {
                                    isDuplicateRead = true;
                                }
                                if (fileHandle.RecentReadsForDebugging.Count >= 10)
                                    fileHandle.RecentReadsForDebugging.Dequeue();
                                fileHandle.RecentReadsForDebugging.Enqueue((fileHandle, offsetInBytes));
                            }
                            Console.Error.WriteLine((isDuplicateRead ? "Dupe: " : "Read: ") + fileHandle.Path + " " + offsetInBytes + " (" + lengthInBytes + ")");
                        }



                        var spanAsBytes = DirectIo.ReadUnaligned(fileHandle.SafeFileHandle, offsetInBytes, checked((int)lengthInBytes), directIoArena, fileHandle.DirectIoReadCache);
                        var result = new DangerousHugeReadOnlyMemory<T>((T*)(void*)spanAsBytes.Pointer, length);

                        //CombinedPersistentMultiDictionary.Assert(result.Span.AsSmallSpan.SequenceEqual(hugeSpan.Slice(index, length).Span.AsSmallSpan));

                        return result;
                    }
                }
            }
            return hugeSpan.Slice(index, length);
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

        public IEnumerable<(TKey Key, TValue Value)> EnumerateKeyValuePairs()
        {
            return Enumerate().SelectMany(x => x.Values, (a, b) => (a.Key, Value: b));
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
                    var values = allValues.Slice(offsets[i], GetValueCount(i, MultiDictionaryIoPreference.OffsetsMmap));
                    yield return (keys[i], values);
                }
            }
        }

        public long BinarySearch<TComparable>(TComparable comparable, MultiDictionaryIoPreference preference = default) where TComparable : IComparable<TKey>
        {
            if (comparable.CompareTo(MaximumKey) > 0) return ~this.Keys.Length;
            if (comparable.CompareTo(MinimumKey) < 0) return ~0;

            if (typeof(TComparable) == typeof(TKey))
            {
                InitializeIoPreferenceForKey((TKey)(object)comparable, ref preference);
            }
            else
            {

            }
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
                return BinarySearchPaginated(comparable, preference);
            }
            else
            {
                return HugeSpanHelpers.BinarySearch(this.Keys.Span, comparable);
            }
#endif

        }

        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        private long BinarySearchPaginated<TComparable>(TComparable comparable, MultiDictionaryIoPreference preference) where TComparable : IComparable<TKey>, allows ref struct
        {
            var keyCacheSpan = this.pageKeys.Span;
            var pageIndex = HugeSpanHelpers.BinarySearch(keyCacheSpan, comparable);
            if (pageIndex < 0)
            {
                pageIndex = (~pageIndex) - 1;
                var pageBaseIndex = pageIndex * KeyCountPerPage;
                var count = pageIndex == keyCacheSpan.Length - 1 ? KeyCount - pageBaseIndex : KeyCountPerPage;
                var page = ReadKeySpan(pageBaseIndex, count, preference);

                var innerIndex = page.Span.AsSmallSpan().BinarySearch(comparable);
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

        public static IoMethodPreference GetKeysIoPreference(MultiDictionaryIoPreference preference)
        {
            return NormalizeIoPreference((IoMethodPreference)(((int)preference >> 0) & 3));
        }

        public static IoMethodPreference GetOffsetsIoPreference(MultiDictionaryIoPreference preference)
        {
            return NormalizeIoPreference((IoMethodPreference)(((int)preference >> 2) & 3));
        }
        public static IoMethodPreference GetValuesIoPreference(MultiDictionaryIoPreference preference)
        {
            return NormalizeIoPreference((IoMethodPreference)(((int)preference >> 4) & 3));
        }

        private static IoMethodPreference NormalizeIoPreference(IoMethodPreference preference)
        {
            if (preference == (IoMethodPreference.DirectIo | IoMethodPreference.Mmap))
            {
                return default;
            }
            return preference;
        }

        public DangerousHugeReadOnlyMemory<TKey> EnumerateKeys()
        {
            return ((DangerousHugeReadOnlyMemory<TKey>)Keys);
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

    [Flags]
    public enum MultiDictionaryIoPreference
    {
        None = 0,

        KeysMmap = 1,
        KeysDirectIo = 2,

        OffsetsMmap = 4,
        OffsetsDirectIo = 8,

        ValuesMmap = 16,
        ValuesDirectIo = 32,

        AllMmap = KeysMmap | OffsetsMmap | ValuesMmap,
        KeysAndOffsetsMmap = KeysMmap | OffsetsMmap,
    }
}

