using AppViewLite;
using AppViewLite.Storage;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.ComponentModel;
using System.Diagnostics.CodeAnalysis;
using System.Collections;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Collections.Concurrent;
using DuckDbSharp.Bindings;
using System.Threading;

namespace AppViewLite.Storage
{

    public enum PersistentDictionaryBehavior
    {
        SortedValues,
        SingleValue,
        PreserveOrder,
        KeySetOnly,
    }

    public interface ICheckpointable : IFlushable
    {
        public (string TableName, SliceName[] ActiveSlices)[] GetActiveSlices();
    }

    public record struct SliceName(DateTime StartTime, DateTime EndTime, long PruneId)
    {
        public string BaseName => StartTime.Ticks + "-" + EndTime.Ticks + (PruneId != 0 ? "-" + PruneId : null);
        public bool IsPruned => (PruneId % 2) != 0;

        public static SliceName ParseBaseName(string name)
        {
            var parts = name.Split('-');
            if (parts.Length == 2) return new SliceName(new DateTime(long.Parse(parts[0]), DateTimeKind.Utc), new DateTime(long.Parse(parts[1]), DateTimeKind.Utc), 0);
            else if (parts.Length == 3) return new SliceName(new DateTime(long.Parse(parts[0]), DateTimeKind.Utc), new DateTime(long.Parse(parts[1]), DateTimeKind.Utc), long.Parse(parts[2]));
            else throw new ArgumentException();
        }
    }

    public abstract class CombinedPersistentMultiDictionary
    {
        public readonly string DirectoryPath;
        protected readonly PersistentDictionaryBehavior behavior;
        public readonly string Name;
        public DateTime LastFlushed;
        public long OriginalWriteBytes;
        public long CompactationWriteBytes;
        public PersistentDictionaryBehavior Behavior => behavior;

        internal readonly static SemaphoreSlim CompactationSemaphore = new SemaphoreSlim(1);

        [ThreadStatic] public static NativeArenaSlim? UnalignedArenaForCurrentThread;

        public abstract Task? HasPendingCompactationNotReadyForCommitYet { get; }
        public unsafe static HugeReadOnlySpan<T> ToSpan<T>(IEnumerable<T> enumerable) where T : unmanaged
        {
            if (TryGetSpan(enumerable, out var span))
            {
                return span;
            }
            return ToNativeArrayCore(enumerable, UnalignedArenaForCurrentThread!);
        }

        public unsafe static DangerousHugeReadOnlyMemory<T> ToNativeArray<T>(IEnumerable<T> enumerable) where T : unmanaged
        {
            var arena = UnalignedArenaForCurrentThread!;
            if (TryGetSpan(enumerable, out var src))
            {
                if (src.IsEmpty) return default;
                var size = checked((int)(Unsafe.SizeOf<T>() * src.Length));
                var ptr = arena.Allocate(size);
                src.AsSmallSpan().CopyTo(new Span<T>(ptr, (int)src.Length));
                Console.Error.WriteLine("ToNativeArray: " + src.Length);
                return new DangerousHugeReadOnlyMemory<T>((T*)ptr, src.Length);
            }

            return ToNativeArrayCore(enumerable, arena);

        }

        private static unsafe DangerousHugeReadOnlyMemory<T> ToNativeArrayCore<T>(IEnumerable<T> enumerable, NativeArenaSlim arena) where T : unmanaged
        {
            if (enumerable.TryGetNonEnumeratedCount(out var count))
            {
                if (count == 0) return default;

                var size = Unsafe.SizeOf<T>() * count;
                var ptr = arena.Allocate(size);


                var dest = new Span<T>((T*)ptr, count);
                var index = 0;
                foreach (var item in enumerable)
                {
                    dest[index++] = item;
                }
                return new DangerousHugeReadOnlyMemory<T>((T*)ptr, count);

            }
            else
            {
                Console.Error.WriteLine("ToNativeArray (enumeration)");
                return ToNativeArray(enumerable.ToArray());
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool TryGetSpan<TSource>(IEnumerable<TSource> source, out HugeReadOnlySpan<TSource> span) where TSource : unmanaged
        {
            bool result = true;
            if (source.GetType() == typeof(TSource[]))
            {
                span = Unsafe.As<TSource[]>(source);
            }
            else if (source is DangerousHugeReadOnlyMemory<TSource> nativeArray)
            {
                span = nativeArray;
            }
            //else if (source.GetType() == typeof(List<TSource>))
            //{
            //    span = CollectionsMarshal.AsSpan(Unsafe.As<List<TSource>>(source));
            //}
            else
            {
                span = default;
                result = false;
            }
            return result;
        }

        public abstract string[] GetPotentiallyCorruptFiles();

        public abstract IEnumerable GetValuesSortedUntyped(object key, object? minExclusive);
        public abstract IEnumerable GetValuesSortedDescendingUntyped(object key, object? maxExclusive);
        public abstract IEnumerable GetValuesPreserveOrderUntyped(object key);
        public abstract IEnumerable EnumerateSortedDescendingUntyped(object? maxExclusive);
        public abstract IEnumerable<(string Name, object Value)> GetCounters();
        protected CombinedPersistentMultiDictionary(string directory, PersistentDictionaryBehavior behavior)
        {
            this.DirectoryPath = directory;
            this.behavior = behavior;
            this.Name = Path.GetFileName(directory);
            LastFlushed = DateTime.UtcNow;
        }

        public static string ToHumanBytes(long bytes)
        {
            if (bytes < 0) return "-" + ToHumanBytes(-bytes);
            if (bytes == 0) return "0 KB";
            if (bytes < 1024) return ((double)Math.Max(bytes, 102) / 1024).ToString("0.0") + " KB";
            if (bytes < 1024 * 1024) return ((double)bytes / 1024).ToString("0.0") + " KB";
            if (bytes < 1024 * 1024 * 1024) return ((double)bytes / (1024L * 1024)).ToString("0.0") + " MB";
            if (bytes < 1024L * 1024 * 1024 * 1024) return ((double)bytes / (1024L * 1024 * 1024)).ToString("0.0") + " GB";
            return ((double)bytes / (1024L * 1024 * 1024 * 1024)).ToString("0.0") + " TB";
        }

        public static SliceName GetSliceInterval(string fileName)
        {
            var dot = fileName.IndexOf('.');
            var baseName = fileName.Substring(0, dot);
            return SliceName.ParseBaseName(baseName);

        }
        public virtual long InMemorySize { get; }
        public virtual long OnDiskSize { get; }

        public virtual long KeyCount { get; }
        public virtual long ValueCount { get; }
        public virtual int SliceCount { get; }

        [DoesNotReturn]
        public static void Abort(string? message)
        {
            Abort(new Exception(message));
        }

        public static Action<string>? LogCallback;


        public static bool UseDirectIo = true;
        public static int DiskSectorSize = 512;
        public static bool PrintDirectIoReads;
        public static Func<string, string> ToPhysicalPath = x => x;
        public static ConcurrentDictionary<string, long> DirectIoReadStats = new();

        public static void Log(string text)
        {
            if (LogCallback != null) LogCallback(text);
            else Console.Error.WriteLine(text);
        }

        [DoesNotReturn]
        public static void Abort(Exception? ex)
        {
            while (ex != null)
            {
                Log(ex.Message);
                if (ex.StackTrace != null)
                    Log(ex.StackTrace);
                ex = ex.InnerException;
            }
            BlueskyRelationships.ThrowFatalError(ex?.Message ?? "Unexpected condition.");
        }

        public static void Assert(bool condition)
        {
            if (!condition)
                Abort(new Exception("Failed assertion."));
        }

        public abstract void ReturnQueueForNextReplica();

        public abstract bool MaybePrune(Func<PruningContext> getPruningContext, long minSizeForPruning, TimeSpan pruningInterval);

    }

    public class CombinedPersistentMultiDictionary<TKey, TValue> : CombinedPersistentMultiDictionary, IDisposable, IFlushable, ICheckpointable, ICloneableAsReadOnly where TKey : unmanaged, IComparable<TKey> where TValue : unmanaged, IComparable<TValue>, IEquatable<TValue>
    {
        public int WriteBufferSize = 128 * 1024;

        private Stopwatch? lastFlushed;
        public Func<IEnumerable<TValue>, IEnumerable<TValue>>? OnCompactation;
        public Func<PruningContext, TKey, bool>? ShouldPreserveKey;
        public Func<PruningContext, TKey, TValue, bool>? ShouldPreserveValue;

        public event EventHandler? AfterCompactation;
        private bool DeleteOldFilesOnCompactation;

        private CachedView[]? Caches;

        private Func<TKey, MultiDictionaryIoPreference>? GetIoPreferenceForKeyFunc;

        public MultiDictionaryIoPreference GetIoPreferenceForKey(TKey key)
        {
            if (GetIoPreferenceForKeyFunc != null) return GetIoPreferenceForKeyFunc(key);
            return default;
        }

#nullable disable
        private CombinedPersistentMultiDictionary(string directory, List<SliceInfo> slices, PersistentDictionaryBehavior behavior)
            : base(directory, behavior)
        {
            this.slices = slices;
        }
#nullable restore

        public CombinedPersistentMultiDictionary(string directory, SliceName[]? sliceNames, PersistentDictionaryBehavior behavior = PersistentDictionaryBehavior.SortedValues, CachedView[]? caches = null, Func<TKey, MultiDictionaryIoPreference>? getIoPreferenceForKey = null)
            : base(directory, behavior)
        {

            CompactStructCheck<TKey>.Check();
            CompactStructCheck<TValue>.Check();

            if (behavior == PersistentDictionaryBehavior.KeySetOnly && typeof(TValue) != typeof(byte))
                throw new ArgumentException("When behavior is KeySetOnly, the dummy TValue must be System.Byte.");

            System.IO.Directory.CreateDirectory(directory);
            this.GetIoPreferenceForKeyFunc = getIoPreferenceForKey;
            this.slices = new();
            this.prunedSlices = new();
            if (caches != null && caches.Length == 0) caches = null;
            this.Caches = caches;
            if (caches != null)
            {
                foreach (var cache in caches)
                {
                    cache.EnsureSupportsSourceBehavior(behavior);
                }
            }

            try
            {
                if (sliceNames != null)
                {
                    DeleteOldFilesOnCompactation = false;
                    foreach (var sliceName in sliceNames)
                    {
                        if (sliceName.IsPruned) this.prunedSlices.Add(sliceName);
                        else
                        {
                            var s = OpenSlice(directory, sliceName, behavior, mandatory: true);
                            AddToCaches(s, this.slices.Count);
                            this.slices.Add(s);
                        }
                    }
                }
                else
                {
                    DeleteOldFilesOnCompactation = true;
                    foreach (var sliceName in Directory.EnumerateFiles(directory, "*.col0.dat").Select(x => CombinedPersistentMultiDictionary.GetSliceInterval(Path.GetFileName(x))).OrderBy(x => (x.StartTime, x.EndTime, x.PruneId)))
                    {
                        if (sliceName.IsPruned) this.prunedSlices.Add(sliceName);
                        else
                        {
                            var s = OpenSlice(directory, sliceName, behavior, mandatory: false);
                            if (s == default) continue;
                            AddToCaches(s, this.slices.Count);
                            this.slices.Add(s);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                foreach (var slice in this.slices)
                {
                    slice.ReaderHandle.Dispose();
                }
                if (sliceNames != null && ex is FileNotFoundException fnf)
                    throw new Exception($"Slice not found: {fnf.FileName}, referenced by the latest checkpoint file.", ex);
                throw;
            }


            queue = new(sortedValues: behavior != PersistentDictionaryBehavior.PreserveOrder);
        }

        private void AddToCaches(TKey key, TValue value)
        {
            if (Caches == null) return;
            foreach (var cache in Caches)
            {
                cache.Add(key, value);
            }
        }

        private unsafe void AddToCaches(SliceInfo slice, int sliceIndex)
        {
            if (Caches == null) return;
            foreach (var cache in Caches)
            {
                if (cache.ShouldPersistCacheForSlice(slice))
                {

                    var cachePath = cache.GetCachePathForSlice(slice);
                    if (!cache.IsAlreadyMaterialized(cachePath))
                    {
                        Log("Materializing cache: " + cachePath);
                        cache.MaterializeCacheFile(slice, cachePath);
                    }

                    Log("Reading cache: " + cachePath);
                    cache.LoadCacheFile(slice, cachePath, sliceIndex);
                }
                else
                {
                    Log("Reading into cache (omit cache materialization): " + slice.Reader.PathPrefix + " [" + cache.Identifier + "]");
                    cache.LoadFromOriginalSlice(slice);
                }
            }




        }

        private SliceInfo OpenSlice(string directory, SliceName sliceName, PersistentDictionaryBehavior behavior, bool mandatory)
        {
            ImmutableMultiDictionaryReader<TKey, TValue> reader;
            try
            {
                reader = new ImmutableMultiDictionaryReader<TKey, TValue>(Path.Combine(directory, sliceName.BaseName), behavior, GetIoPreferenceForKeyFunc);
            }
            catch (FileNotFoundException) when (!mandatory)
            {
                var fullPath = Path.Combine(directory, sliceName.BaseName);
                Log("Partial slice: " + fullPath);
                File.Move(fullPath, fullPath + ".orphan-slice");
                return default;
            }

            return new SliceInfo(
                sliceName.StartTime,
                sliceName.EndTime,
                sliceName.PruneId,
                new(reader));
        }

        private MultiDictionary2<TKey, TValue> queue;
        public List<SliceInfo> slices;
        public List<SliceName> prunedSlices;


        public event EventHandler? BeforeFlush;
        public event EventHandler? AfterFlush;
        public event EventHandler<CancelEventArgs>? ShouldFlush;
        public event EventHandler? BeforeWrite;

        public MultiDictionary2<TKey, TValue> QueuedItems => queue;
        public record struct SliceInfo(DateTime StartTime, DateTime EndTime, long PruneId, ReferenceCountHandle<ImmutableMultiDictionaryReader<TKey, TValue>> ReaderHandle, long SizeInBytes)
        {
            public SliceInfo(DateTime startTime, DateTime endTime, long pruneId, ReferenceCountHandle<ImmutableMultiDictionaryReader<TKey, TValue>> readerHandle)
                 : this(startTime, endTime, pruneId, readerHandle, readerHandle.Value.SizeInBytes)
            {

            }
            public ImmutableMultiDictionaryReader<TKey, TValue> Reader => ReaderHandle.Value;

            public SliceName SliceName => new SliceName(this.StartTime, this.EndTime, this.PruneId);

            public override string ToString()
            {
                return SliceName + " (" + ToHumanBytes(SizeInBytes) + ")";
            }
        }

        public void DisposeNoFlush()
        {
            if (pendingCompactation != null) throw new Exception();

            if (Caches != null && ownsCaches)
            {
                foreach (var cache in Caches)
                {
                    cache.Dispose();
                }
            }

            foreach (var slice in slices)
            {
                slice.ReaderHandle.Dispose();
            }
        }

        public void Dispose() // Dispose() must also be called while holding the lock.
        {
            Flush(disposing: true);
            DisposeNoFlush();
        }

        private int onBeforeFlushNotificationInProgress;

        public void Flush(bool disposing)
        {
            try
            {
                FlushCore(disposing);
            }
            catch (Exception ex)
            {
                CombinedPersistentMultiDictionary.Abort(ex);
            }
        }

        private void FlushCore(bool disposing)
        {
            if (onBeforeFlushNotificationInProgress != 0) return;

            MaybeCommitPendingCompactation(forceWait: true);

            if (queue.GroupCount != 0)
            {
                try
                {
                    onBeforeFlushNotificationInProgress++;
                    BeforeFlush?.Invoke(this, EventArgs.Empty);
                }
                finally
                {
                    onBeforeFlushNotificationInProgress--;
                }

                var date = DateTime.UtcNow;

                if (date < prevSliceDate) date = prevSliceDate;
                if (date == prevSliceDate)
                {
                    date = date.AddTicks(1);
                }

                prevSliceDate = date;

                var groupCount = queue.GroupCount;
                var prefix = DirectoryPath + "/" + date.Ticks + "-" + date.Ticks;
                using var writer = new ImmutableMultiDictionaryWriter<TKey, TValue>(prefix, behavior);


                foreach (var group in queue.OrderBy(x => x.Key))
                {
                    var values = group.Values;
                    if (values.TryAsUnsortedSpan(out var span)) writer.AddPresorted(group.Key, span);
                    else writer.AddPresorted(group.Key, values.ValuesSorted);
                }
                var size = writer.CommitAndGetSize();
                OriginalWriteBytes += size;
                queue.Clear();
                LastFlushed = DateTime.UtcNow;
                slices.Add(new(date, date, 0, new(new ImmutableMultiDictionaryReader<TKey, TValue>(prefix, behavior, GetIoPreferenceForKeyFunc))));
                Log($"[{Path.GetFileName(DirectoryPath)}] Wrote {ToHumanBytes(size)}");

                if (!disposing)
                    NotifyCachesSliceAdded(slices.Count - 1);

                if (!disposing)
                    MaybeStartCompactation();


                AfterFlush?.Invoke(this, EventArgs.Empty);
            }
        }

        private void NotifyCachesSliceAdded(int insertedAt)
        {
            if (Caches != null)
            {
                foreach (var cache in Caches)
                {
                    cache.OnSliceAdded(insertedAt, this.slices[insertedAt]);
                }
            }
        }

        private void NotifyCachesSliceRemoved(int removedAt)
        {
            if (Caches != null)
            {
                foreach (var cache in Caches)
                {
                    cache.OnSliceRemoved(removedAt);
                }
            }
        }

        private DateTime prevSliceDate;


        public record CompactationCandidate(int Start, int Length, long ItemCount, long LargestComponent, double Score, double RatioOfLargestComponent)
        {
        }

        public List<CompactationCandidate> GetCompactationCandidates(int minLength)
        {
            var candidates = new List<CompactationCandidate>();
            for (int start = 0; start < slices.Count; start++)
            {
                long compactationBytes = 0;
                long largestComponentBytes = 0;
                for (int end = start + 1; end <= slices.Count; end++) // do not "optimize" this into a start + 2 (totalBytes accumulator)
                {
                    var componentBytes = slices[end - 1].SizeInBytes;
                    largestComponentBytes = Math.Max(largestComponentBytes, componentBytes);
                    compactationBytes += componentBytes;
                    var length = end - start;
                    if (length < minLength) continue;

                    var ratioOfLargestComponent = ((double)largestComponentBytes / compactationBytes);
                    if (ratioOfLargestComponent > GetMaximumRatioOfLargestSliceForCompactation(compactationBytes, slices.Count)) continue;
                    var z = slices.Slice(start, length);
                    Assert(z.Sum(x => x.SizeInBytes) == compactationBytes);
                    var score = (1 - ratioOfLargestComponent) * Math.Log(length) / Math.Log(compactationBytes);
                    candidates.Add(new CompactationCandidate(start, length, compactationBytes, largestComponentBytes, score, ratioOfLargestComponent));
                }
            }
            return candidates;
        }

        private static double GetMaximumRatioOfLargestSliceForCompactation(long totalSize, int totalSliceCount)
        {
            if (totalSliceCount >= 60)
            {
                return totalSliceCount * 0.01; // 40 slices -> 0.4; 100 slices: unacceptable, any ratio is ok (even 0.9999)
            }
            return 1 / Math.Max(3, Math.Log10(totalSize) - 3);
        }
        private const int TargetSliceCount = 15;
        private const int MinimumCompactationCount = 6;

        public void MaybeStartCompactation()
        {
            if (pendingCompactation != null) throw new InvalidOperationException();

            if (slices.Count <= TargetSliceCount) return;

            var minLength = (slices.Count - TargetSliceCount) + 1;

            var candidates = GetCompactationCandidates(Math.Max(minLength, MinimumCompactationCount));
            if (candidates.Count != 0)
            {
                var best = candidates.MaxBy(x => x.Score)!;
                StartCompactation(best.Start, best.Length, best);
            }

        }


        private void StartCompactation(int groupStart, int groupLength, CompactationCandidate compactationCandidate)
        {
            if (groupLength <= 1) return;
            var inputs = slices.Slice(groupStart, groupLength);
            for (int i = 1; i < slices.Count; i++)
            {
                if (slices[i - 1].StartTime >= slices[i].StartTime) throw new Exception();
            }
            var mergedStartTime = inputs[0].StartTime;
            var mergedEndTime = inputs[^1].EndTime;
            var mergedPruneId = inputs.Max(x => x.PruneId);
            var mergedPrefix = this.DirectoryPath + "/" + mergedStartTime.Ticks + "-" + mergedEndTime.Ticks;

            var writer = new ImmutableMultiDictionaryWriter<TKey, TValue>(mergedPrefix, behavior);


            var inputSlices = inputs.Select(x => x.Reader).ToArray();
            var sw = Stopwatch.StartNew();

            if (pendingCompactation != null) throw new Exception();

            var compactationThread = Task.Factory.StartNew(() =>
            {
                try
                {
                    CompactationSemaphore.Wait();
                    Compact(inputSlices, writer, OnCompactation);
                }
                catch (Exception ex)
                {
                    CombinedPersistentMultiDictionary.Abort(ex);
                }
                finally
                {
                    CompactationSemaphore.Release();
                }
                sw.Stop();

                return new Action(() =>
                {
                    // Here we are again inside the lock.
                    var size = writer.CommitAndGetSize(); // Writer disposal (*.dat.tmp -> *.dat) must happen inside the lock, otherwise old slice GC might see an unused slice and delete it. .tmp files are exempt (except at startup)
                    CompactationWriteBytes += size;
                    Log("Compact (" + sw.Elapsed + ") " + Path.GetFileName(DirectoryPath) + ": " + string.Join(" + ", inputs.Select(x => ToHumanBytes(x.SizeInBytes))) + " => " + ToHumanBytes(inputs.Sum(x => x.SizeInBytes)) + " -- largest: " + compactationCandidate.RatioOfLargestComponent.ToString("0.00"));

                    foreach (var input in inputs)
                    {
                        input.ReaderHandle.Dispose();
                        if (DeleteOldFilesOnCompactation)
                        {
                            for (int i = 0; i < input.Reader.ColumnCount; i++)
                            {
                                File.Delete(ToPhysicalPath(input.Reader.PathPrefix + ".col" + i + ".dat"));
                            }
                        }
                        NotifyCachesSliceRemoved(groupStart);
                    }

                    // Note: Flushes/slices.Add() can happen even during the compactation.
                    slices.RemoveRange(groupStart, groupLength);
                    slices.Insert(groupStart, new SliceInfo(mergedStartTime, mergedEndTime, mergedPruneId, new(new(mergedPrefix, behavior, GetIoPreferenceForKeyFunc))));
                    NotifyCachesSliceAdded(groupStart);

                    AfterCompactation?.Invoke(this, EventArgs.Empty);
                });
            }, TaskCreationOptions.LongRunning);

            pendingCompactation = compactationThread;


        }

        public bool TryGetLatestValue(TKey key, out TValue value)
        {
            foreach (var item in GetValuesChunkedLatestFirst(key))
            {
                value = item[item.Count - 1];
                return true;
            }
            value = default;
            return false;
        }

        public bool TryGetSingleValue(TKey key, out TValue value, MultiDictionaryIoPreference preference = default)
        {
            InitializeIoPreferenceForKey(key, ref preference);
            if (behavior == PersistentDictionaryBehavior.PreserveOrder) throw new InvalidOperationException();
            if (GetKeyProbabilisticCache()?.PossiblyContainsKey(key) == false)
            {
                value = default;
                return false;
            }
#if true
            foreach (var slice in slices)
            {
                var vals = slice.Reader.GetValues(key, preference);
                if (vals.Length != 0)
                {
                    value = vals[0];
                    return true;
                }
            }

            if (queue.TryGetValues(key, out var q))
            {
                value = q.First();
                return true;
            }
#else
            var f = GetValuesChunked(key).SingleOrDefault();

            if (!f.IsEmpty)
            {
                if (f.Count == 1)
                {
                    value = f[0];
                    return true;
                }
                else throw new Exception("Multiple values for key " + key);
            }
#endif
            value = default;
            return false;
        }

        public IEnumerable<DangerousHugeReadOnlyMemory<TValue>> GetValuesChunked(TKey key, TValue? minExclusive = null, TValue? maxExclusive = null, MultiDictionaryIoPreference preference = default)
        {
            InitializeIoPreferenceForKey(key, ref preference);
            if (behavior == PersistentDictionaryBehavior.PreserveOrder) throw new InvalidOperationException();
            DangerousHugeReadOnlyMemory<TValue> extraArr = default;
            if (queue.TryGetValues(key, out var extraStruct))
            {
                var extra = extraStruct.ValuesSorted;
                if (minExclusive != null)
                {
                    if (maxExclusive != null)
                    {
                        extraArr = ToNativeArray(extra.Where(x => minExclusive.Value.CompareTo(x) < 0 && x.CompareTo(maxExclusive.Value) < 0));
                    }
                    else
                    {
                        extraArr = ToNativeArray(extra.Where(x => minExclusive.Value.CompareTo(x) < 0));
                    }
                }
                else if (maxExclusive != null)
                {
                    extraArr = ToNativeArray(extra.Where(x => x.CompareTo(maxExclusive.Value) < 0));
                }
                else
                {
                    extraArr = ToNativeArray(extra);
                }
            }
            var z = slices.Select(slice => slice.Reader.GetValues(key, minExclusive: minExclusive, maxExclusive: maxExclusive, preference)).Where(x => x.Length != 0).Select(x => (DangerousHugeReadOnlyMemory<TValue>)x);
            if (extraArr.Count != 0)
                z = z.Append(extraArr);
            return z;
        }


        public IEnumerable<DangerousHugeReadOnlyMemory<TValue>> GetValuesChunkedLatestFirst(TKey key, bool omitQueue = false, MultiDictionaryIoPreference preference = default)
        {
            InitializeIoPreferenceForKey(key, ref preference);
            if (behavior == PersistentDictionaryBehavior.PreserveOrder) throw new InvalidOperationException();
            if (!omitQueue && queue.TryGetValues(key, out var extra))
            {
                yield return ToNativeArray(extra.ValuesUnsorted); // this will actually be sorted
            }
            for (int i = slices.Count - 1; i >= 0; i--)
            {
                var vals = slices[i].Reader.GetValues(key, preference);
                if (vals.Length != 0)
                    yield return vals;
            }
        }


        public bool TryGetPreserveOrderSpanAny(TKey key, out HugeReadOnlySpan<TValue> val, MultiDictionaryIoPreference preference = default)
        {
            InitializeIoPreferenceForKey(key, ref preference);
            if (behavior != PersistentDictionaryBehavior.PreserveOrder) throw new InvalidOperationException();
            foreach (var slice in slices)
            {
                var v = slice.Reader.GetValues(key, preference);
                if (v.Length != 0)
                {
                    val = v.Span;
                    return true;
                }

            }

            if (queue.TryGetValues(key, out var q))
            {
                val = ToSpan(q.ValuesUnsorted);
                return true;
            }
            val = default;
            return false;
        }
        public bool TryGetPreserveOrderSpanLatest(TKey key, out HugeReadOnlySpan<TValue> val)
        {
            if (behavior != PersistentDictionaryBehavior.PreserveOrder) throw new InvalidOperationException();
            if (queue.TryGetValues(key, out var extra))
            {
                val = ToSpan(extra.ValuesUnsorted);
                return true;
            }
            for (int i = slices.Count - 1; i >= 0; i--)
            {
                var v = slices[i].Reader.GetValues(key);
                if (v.Length != 0)
                {
                    val = v.Span;
                    return true;
                }
            }

            val = default;
            return false;

        }
        public IEnumerable<TValue> GetDistinctValuesSorted(TKey key)
        {
            return GetValuesSorted(key).DistinctAssumingOrderedInput();
        }

        public IEnumerable<TValue> GetValuesUnsorted(TKey key, TValue? minExclusive = null, TValue? maxExclusive = null, MultiDictionaryIoPreference preference = default)
        {
            InitializeIoPreferenceForKey(key, ref preference);
            if (minExclusive != null && maxExclusive == null)
            {
                var chunks = GetValuesChunked(key);
                return chunks.SelectMany(chunk => chunk.Reverse().TakeWhile(x => minExclusive.Value.CompareTo(x) < 0));
            }
            return GetValuesChunked(key, minExclusive, maxExclusive).SelectMany(x => x);
        }



        public IEnumerable<TValue> GetValuesSorted(TKey key, TValue? minExclusive = null)
        {
            var chunks = GetValuesChunked(key, minExclusive).ToArray();
            if (chunks.Length == 0) return [];
            if (chunks.Length == 1) return chunks[0].AsEnumerable();
            var chunksEnumerables = chunks.Select(x => x.AsEnumerable()).ToArray();
            return SimpleJoin.ConcatPresortedEnumerablesKeepOrdered(chunksEnumerables, x => x);
        }

        public IEnumerable<TValue> GetValuesSortedDescending(TKey key, TValue? minExclusive, TValue? maxExclusive)
        {
            if (minExclusive != null && maxExclusive == null)
                return GetValuesSortedDescending(key, null, null).TakeWhile(x => minExclusive.Value.CompareTo(x) < 0); // avoids binary searches
            var chunks = GetValuesChunked(key, minExclusive, maxExclusive).ToArray();
            if (chunks.Length == 0) return [];
            if (chunks.Length == 1) return chunks[0].Reverse();
            var chunksEnumerables = chunks.Select(x => x.Reverse()).ToArray();
            return SimpleJoin.ConcatPresortedEnumerablesKeepOrdered(chunksEnumerables, x => x, new ReverseComparer<TValue>());
        }

        public IEnumerable<TValue> GetValuesUnsorted(TKey key)
        {
            if (behavior == PersistentDictionaryBehavior.PreserveOrder)
            {
                var q = queue.TryGetValues(key);
                if (q.Count != 0)
                {
                    foreach (var item in queue.TryGetValues(key).ValuesUnsorted)
                    {
                        yield return item;
                    }
                }
                else
                {
                    for (int i = slices.Count - 1; i >= 0; i--)
                    {
                        var v = slices[i].Reader.GetValues(key);
                        if (v.Length != 0)
                        {
                            foreach (var item in v)
                            {
                                yield return item;
                            }
                            yield break;
                        }
                    }
                }
            }
            else
            {
                foreach (var slice in slices)
                {
                    var v = slice.Reader.GetValues(key);
                    for (int i = 0; i < v.Length; i++)
                    {
                        yield return v[i];
                    }
                }
                if (queue.TryGetValues(key, out var vals))
                {
                    foreach (var item in vals.ValuesUnsorted)
                    {
                        yield return item;
                    }
                }
            }
        }

        public bool Contains(TKey key, TValue value, MultiDictionaryIoPreference preference = default)
        {
            if (behavior == PersistentDictionaryBehavior.PreserveOrder) throw new InvalidOperationException();

            if (GetKeyValueProbabilisticCache()?.PossiblyContains(key, value) == false) return false;

            InitializeIoPreferenceForKey(key, ref preference);
            return slices.Any(slice => slice.Reader.Contains(key, value, preference)) || queue.TryGetValues(key).Contains(value);
        }
        public bool ContainsKey(TKey key, MultiDictionaryIoPreference preference = default)
        {
            if (GetKeyProbabilisticCache()?.PossiblyContainsKey(key) == false) return false;

            InitializeIoPreferenceForKey(key, ref preference);
            foreach (var slice in slices)
            {
                if (slice.Reader.ContainsKey(key, preference)) return true;
            }
            if (queue.ContainsKey(key)) return true;
            return false;
        }

        public void InitializeIoPreferenceForKey(TKey key, ref MultiDictionaryIoPreference preference)
        {
            preference |= GetIoPreferenceForKey(key);
        }

        public long GetValueCount(TKey key, MultiDictionaryIoPreference preference = default)
        {
            if (behavior == PersistentDictionaryBehavior.PreserveOrder) throw new InvalidOperationException();
            InitializeIoPreferenceForKey(key, ref preference);
            long num = 0;
            foreach (var slice in slices)
            {
                num += slice.Reader.GetValueCount(key, preference);
            }
            num += queue.TryGetValues(key).Count;
            return num;
        }

        public void AddIfMissing(TKey key, TValue value)
        {
            if (!Contains(key, value))
                Add(key, value);
        }

        public void Add(TKey key, TValue value)
        {
            OnBeforeWrite();
            AddToCaches(key, value);
            if (behavior == PersistentDictionaryBehavior.PreserveOrder) throw new InvalidOperationException();
            if (behavior == PersistentDictionaryBehavior.KeySetOnly)
            {
                if (!value.Equals(default)) throw new ArgumentException();
                queue.SetSingleton(key, default);
            }
            else if (behavior == PersistentDictionaryBehavior.SingleValue)
            {
                queue.SetSingleton(key, value);
            }
            else
            {
                queue.Add(key, value);
            }
            MaybeFlush();
        }



        private void OnBeforeWrite()
        {
            BeforeWrite?.Invoke(this, EventArgs.Empty);
        }

        public void AddRange(TKey key, ReadOnlySpan<TValue> values)
        {
            OnBeforeWrite();
            if (behavior == PersistentDictionaryBehavior.PreserveOrder)
            {
                if (values.Length == 0) throw new ArgumentException();
                queue.RemoveAll(key);
            }

            if (Caches != null)
            {
                foreach (var value in values)
                {
                    AddToCaches(key, value);
                }
            }

            queue.AddRange(key, values);
            MaybeFlush();
        }

        public override long KeyCount => queue.GroupCount + slices.Sum(x => x.Reader.KeyCount);
        public override long ValueCount => queue.ValueCount + slices.Sum(x => x.Reader.ValueCount);
        public override int SliceCount => slices.Count;

        public TimeSpan? MaximumInMemoryBufferDuration { get; set; }
        public TKey? MaximumKey
        {
            get
            {
                var maximumKey = slices.Max(x => (TKey?)x.Reader.MaximumKey);
                if (queue.GroupCount != 0)
                {
                    foreach (var key in queue.Keys)
                    {
                        if (maximumKey == null || key.CompareTo(maximumKey.Value) > 0)
                            maximumKey = key;
                    }
                }
                return maximumKey;
            }
        }

        private void MaybeFlush()
        {
            MaybeCommitPendingCompactation();
            if (pendingCompactation != null) return;

            if (InMemorySize >= WriteBufferSize || (MaximumInMemoryBufferDuration != null && lastFlushed != null && lastFlushed.Elapsed > MaximumInMemoryBufferDuration))
            {
                var shouldFlushArgs = new CancelEventArgs();
                ShouldFlush?.Invoke(this, shouldFlushArgs);
                if (shouldFlushArgs.Cancel) return;
                Flush(false);
            }
            lastFlushed ??= Stopwatch.StartNew();
        }

        public override long InMemorySize => queue.SizeInBytes;
        public override long OnDiskSize => slices.Sum(x => x.SizeInBytes);

        // Setting and reading this field requires a lock, but the underlying operation can finish at any time.
        private Task<Action>? pendingCompactation;

        public override Task? HasPendingCompactationNotReadyForCommitYet => pendingCompactation != null && !pendingCompactation.IsCompleted ? pendingCompactation : null;
        private void MaybeCommitPendingCompactation(bool forceWait = false)
        {
            try
            {
                if (pendingCompactation == null) return;
                if (forceWait)
                {
                    Log("Synchronously waiting for pending compactation to complete, because forceWait=true");
                    pendingCompactation.GetAwaiter().GetResult();
                }
                if (!pendingCompactation.IsCompleted)
                {
                    if (forceWait) throw new Exception();
                    return;
                }
                pendingCompactation.Result();
                pendingCompactation = null;
            }
            catch (Exception ex)
            {
                CombinedPersistentMultiDictionary.Abort(ex);
            }
        }

        private static void Compact(IReadOnlyList<ImmutableMultiDictionaryReader<TKey, TValue>> inputs, ImmutableMultiDictionaryWriter<TKey, TValue> output, Func<IEnumerable<TValue>, IEnumerable<TValue>>? onCompactation)
        {
            using var _ = new ThreadPriorityScope(System.Threading.ThreadPriority.Lowest);
            var concatenatedSlices = SimpleJoin.ConcatPresortedEnumerablesKeepOrdered(inputs.Select(x => x.Enumerate()).ToArray(), (x, i) => (x.Key, i));
            var groupedSlices = SimpleJoin.GroupAssumingOrderedInput(concatenatedSlices);

            if (output.behavior == PersistentDictionaryBehavior.PreserveOrder)
            {
                if (onCompactation != null) throw new ArgumentException();

                foreach (var group in groupedSlices)
                {
                    var mostRecent = group.Values[^1];
                    output.AddPresorted(group.Key, mostRecent.Span.AsSmallSpan());
                }

            }
            else
            {

                foreach (var group in groupedSlices)
                {
                    if (group.Values.Count == 1)
                    {
                        output.AddPresorted(group.Key, group.Values[0].Span.AsSmallSpan());
                    }
                    else
                    {
                        if (output.behavior is PersistentDictionaryBehavior.SingleValue or PersistentDictionaryBehavior.KeySetOnly)
                        {
                            var slices = group.Values;
                            var lastSlice = slices[slices.Count - 1];
                            if (lastSlice.Length != 1) throw new Exception();
                            var lastValue = lastSlice[0];
                            output.AddPresorted(group.Key, [lastValue]);
                        }
                        else
                        {
                            var enumerables = group.Values.Select(x => x.AsEnumerable()).ToArray();
                            var merged = SimpleJoin.ConcatPresortedEnumerablesKeepOrdered(enumerables, x => x).DistinctAssumingOrderedInput();
                            if (onCompactation != null)
                                merged = onCompactation(merged);

                            output.AddPresorted(group.Key, merged);
                        }
                    }
                }
            }
            // Don't commit yet (see caller)
        }


        public IEnumerable<(TKey Key, DangerousHugeReadOnlyMemory<TValue> Values)> EnumerateUnsortedGrouped()
        {
            foreach (var slice in slices)
            {
                foreach (var group in slice.Reader.Enumerate())
                {
                    yield return group;
                }
            }
            foreach (var q in queue.Groups)
            {
                yield return (q.Key, ToNativeArray(q.Value.ValuesUnsorted));
            }
        }

        public bool IsSingleValueOrKeySet => behavior is PersistentDictionaryBehavior.SingleValue or PersistentDictionaryBehavior.KeySetOnly;
        public bool HasOffsets => !IsSingleValueOrKeySet;
        public bool HasValues => behavior != PersistentDictionaryBehavior.KeySetOnly;


        public IEnumerable<(TKey Key, DangerousHugeReadOnlyMemory<TValue>[] ValueChunks)> EnumerateSortedGrouped()
        {
            if (IsSingleValueOrKeySet) throw new InvalidOperationException();
            var s = slices.Select(x => x.Reader.Enumerate().Select(x => (x.Key, Values: (DangerousHugeReadOnlyMemory<TValue>)x.Values)));
            if (queue.GroupCount != 0)
            {
                s = s.Append(queue.Select(x => (x.Key, Values: ToNativeArray(x.Values.ValuesUnsorted))));
            }
            return SimpleJoin
                .ConcatPresortedEnumerablesKeepOrdered(s.ToArray(), x => x.Key)
                .GroupAssumingOrderedInput(x => x.Key)
                .Select(x =>
                {
                    if (behavior == PersistentDictionaryBehavior.PreserveOrder)
                        return (x.Key, new[] { x.Values[x.Values.Count - 1].Values });
                    return (x.Key, x.Values.Select(x => x.Values).ToArray());
                });
        }

        public IEnumerable<(TKey Key, TValue Value)> EnumerateUnsorted()
        {
            foreach (var slice in slices)
            {
                foreach (var group in slice.Reader.Enumerate())
                {
                    var q = group.Values;
                    for (long i = 0; i < q.Length; i++)
                    {
                        yield return (group.Key, q[i]);
                    }
                }
            }
            foreach (var q in queue.AllEntries)
            {
                yield return (q.Key, q.Value);
            }
        }

        public IEnumerable<TKey> EnumerateKeysUnsorted()
        {
            return EnumerateKeyChunks().SelectMany(x => x);
        }
        public IEnumerable<DangerousHugeReadOnlyMemory<TKey>> EnumerateKeyChunks()
        {
            foreach (var slice in slices)
            {
                yield return (DangerousHugeReadOnlyMemory<TKey>)slice.Reader.Keys;
            }
            if (queue.GroupCount != 0)
            {
                yield return ToNativeArray(queue.Keys.Order());
            }
        }

        public IEnumerable<TKey> EnumerateKeysSortedDescending(TKey? maxExclusive)
        {
            var keyChunks = this.EnumerateKeyChunks();
            if (maxExclusive != null)
            {
                keyChunks = keyChunks
                    .Select(x =>
                    {
                        var index = x.AsSpan().BinarySearch(maxExclusive.Value);
                        if (index >= 0)
                        {
                            return x.Slice(0, index);
                        }
                        else
                        {
                            index = ~index;
                            return x.Slice(0, index);
                        }
                    })
                    .Where(x => x.Count != 0);
            }

            return SimpleJoin.ConcatPresortedEnumerablesKeepOrdered<TKey, TKey>(keyChunks.Select(x => x.Reverse()).ToArray(), x => x, new ReverseComparer<TKey>()).DistinctAssumingOrderedInput(skipCheck: true);
        }

        public IEnumerable<(TKey Key, DangerousHugeReadOnlyMemory<TValue> Values)> GetInRangeUnsorted(TKey min, TKey maxExclusive, MultiDictionaryIoPreference preference = default)
        {
            InitializeIoPreferenceForKey(min, ref preference);
            foreach (var q in queue)
            {
                if (IsInRange(q.Key, min, maxExclusive))
                    yield return (q.Key, ToNativeArray(q.Values.ValuesUnsorted));
            }

            foreach (var slice in slices)
            {
                var centerIndex = slice.Reader.BinarySearch(min);
                if (centerIndex < 0)
                {
                    centerIndex = ~centerIndex;
                }


                var keys = slice.Reader.Keys;
                for (long i = Math.Min(centerIndex, keys.Length - 1); i >= 0; i--)
                {
                    var key = keys.Span[i];
                    if (!IsInRange(key, min, maxExclusive)) break;
                    yield return (key, slice.Reader.GetValues(i, preference));
                }


                for (long i = centerIndex + 1; i < keys.Length; i++)
                {
                    var key = keys.Span[i];
                    if (!IsInRange(key, min, maxExclusive)) break;
                    yield return (key, slice.Reader.GetValues(i, preference));
                }

            }
        }

        private static bool IsInRange(TKey key, TKey min, TKey maxExclusive)
        {
            return
                key.CompareTo(min) >= 0 &&
                key.CompareTo(maxExclusive) < 0;
        }

        public (string TableName, SliceName[] ActiveSlices)[] GetActiveSlices()
        {
            return [(Path.GetFileName(this.DirectoryPath), slices.Select(x => new SliceName(x.StartTime, x.EndTime, x.PruneId)).Concat(prunedSlices).ToArray())];
        }

        private MultiDictionary2<TKey, TValue>? AcquireOldReplicaForRecycling()
        {
            var nextReplicaCanBeBuiltFrom = NextReplicaCanBeBuiltFrom;
            lock (nextReplicaCanBeBuiltFrom)
            {
                if (nextReplicaCanBeBuiltFrom.Count != 0)
                {
                    var q = nextReplicaCanBeBuiltFrom[^1];
                    nextReplicaCanBeBuiltFrom.RemoveAt(nextReplicaCanBeBuiltFrom.Count - 1);
                    return q;
                }
            }
            return null;
        }

        private bool ownsCaches = true;

        public CombinedPersistentMultiDictionary<TKey, TValue> CloneAsReadOnly()
        {
            var copy = new CombinedPersistentMultiDictionary<TKey, TValue>(DirectoryPath, this.slices.Select(x => new SliceInfo(x.StartTime, x.EndTime, x.PruneId, x.ReaderHandle.AddRef())).ToList(), behavior);
            copy.ReplicatedFrom = this;
            copy.GetIoPreferenceForKeyFunc = this.GetIoPreferenceForKeyFunc;
            copy.ownsCaches = false;

            if (this.Caches != null)
            {

                copy.Caches = this.Caches.Where(x => x.CanBeUsedByReplica).ToArray();
                if (copy.Caches.Length == 0)
                    copy.Caches = null;
            }

            // The root CloneAsReadOnly() is called from within a traditional lock.
            // Additionally, we hold a Read lock for the primary (so there can be no writers in the primary)
            // We CAN write NextReplicaCanBeBuiltFrom to the primary, even without holding the primary write lock (it's not used in reads).

            var q = AcquireOldReplicaForRecycling();
            if (q != null)
            {

                if (q.FirstVirtualSliceId == this.queue.virtualSlices[0].Id)
                {
                    copy.queue = q;
                    var alreadyExistingSlices = checked((int)(q.LastVirtualSliceIdExclusive - q.FirstVirtualSliceId));


                    var virtualSliceCount = this.queue.virtualSlices.Count;
                    var lastSliceIsEmpty = this.queue.currentVirtualSlice!.DirtyKeys.Count == 0;
                    if (lastSliceIsEmpty)
                        virtualSliceCount--;

                    HashSet<TKey> keysToCopy;
                    if (alreadyExistingSlices == virtualSliceCount - 1)
                    {
                        // fast path
                        keysToCopy = this.queue.virtualSlices[alreadyExistingSlices].DirtyKeys;
                        copy.queue.LastVirtualSliceIdExclusive++;
                    }
                    else
                    {

                        keysToCopy = new();
                        for (int i = alreadyExistingSlices; i < virtualSliceCount; i++)
                        {
                            var slice = this.queue.virtualSlices[i];
                            foreach (var key in slice.DirtyKeys)
                            {
                                keysToCopy.Add(key);
                            }
                            copy.queue.LastVirtualSliceIdExclusive++;
                        }
                    }

                    foreach (var key in keysToCopy)
                    {
                        var g = this.queue.Groups[key];
                        if (g._manyValuesSorted != null)
                        {
                            if (copy.queue.Groups.TryGetValue(key, out var preexisting))
                            {
                                if (preexisting.Count == g.Count) // if _manyValuesSorted didn't grow, we can keep the old version instead of cloning the whole SortedSet
                                {
                                    // Assert(preexisting._manyValuesSorted!.SequenceEqual(g._manyValuesSorted));
                                    continue;
                                }

                            }

                            if (g.Count >= 250)
                                Console.Error.WriteLine("Copying SortedSet of size " + g.Count);
                            // _manyValuesSorted is mutable (unlike _manyValuesPreserved), so we need to copy it.


                            // TODO: instead, each virtual slices contains List<KeyValuePair<TKey, TValue>>. After a replica is captured, we set that field to null. keep in mind when each obsolete for recycling is returned
                            g._manyValuesSorted = new SortedSet<TValue>(g._manyValuesSorted);
                        }
                        copy.queue.Groups[key] = g;
                    }

                    if (!lastSliceIsEmpty)
                        this.queue.CreateVirtualSlice(); // nobody else is writing to primary.

                    //Log("Queue copied incrementally.");
                }


            }

            if (copy.queue == null)
            {
                copy.queue = this.queue.CloneAndMaybeCreateNewVirtualSlice();
                //Log("Queued copied from scratch.");
            }
            copy.BeforeWrite += (_, _) => throw new InvalidOperationException("ReadOnly copy.");
            copy.BeforeFlush += (_, _) => throw new InvalidOperationException("ReadOnly copy.");
            return copy;
        }

        ICloneableAsReadOnly ICloneableAsReadOnly.CloneAsReadOnly() => CloneAsReadOnly();

        public override void ReturnQueueForNextReplica()
        {
            if (this.queue == null) return;
            if (this.ReplicatedFrom == null) return;

            var nextReplicaCanBeBuiltFrom = this.ReplicatedFrom.NextReplicaCanBeBuiltFrom;
            lock (nextReplicaCanBeBuiltFrom)
            {
                if (nextReplicaCanBeBuiltFrom.Contains(this.queue))
                    throw new InvalidOperationException();

                nextReplicaCanBeBuiltFrom.Add(this.queue);

                const int MaxVersionsToKeep = 3;
                while (nextReplicaCanBeBuiltFrom.Count > MaxVersionsToKeep)
                {
                    nextReplicaCanBeBuiltFrom.RemoveAt(0);
                }
            }
            this.queue = null!;
        }

        private List<MultiDictionary2<TKey, TValue>> NextReplicaCanBeBuiltFrom = new(); // only populated in primary
        private CombinedPersistentMultiDictionary<TKey, TValue>? ReplicatedFrom; // only populated in replica

        public TValue? GetValueWithPrefix(TKey key, TValue valueOrPrefix, Func<TValue, bool> hasDesiredPrefix)
        {
            foreach (var chunk in GetValuesChunkedLatestFirst(key))
            {
                var span = chunk.AsSpan();
                var index = span.IndexOfUsingBinarySearch(valueOrPrefix, hasDesiredPrefix);
                if (index != -1)
                    return span[index];
            }
            return null;
        }
        public TValue? GetValueWithPrefixLatest(TKey key, TValue valueOrPrefix, Func<TValue, bool> hasDesiredPrefix)
        {
            foreach (var chunk in GetValuesChunkedLatestFirst(key))
            {
                var span = chunk.AsSpan();
                var index = span.IndexOfUsingBinarySearchLatest(valueOrPrefix, hasDesiredPrefix);
                if (index != -1)
                    return span[index];
            }
            return null;
        }

        public override bool MaybePrune(Func<PruningContext> getPruningContext, long minSizeForPruning, TimeSpan pruningInterval)
        {
            if (pendingCompactation != null) return false;
            try
            {
                return MaybePruneCore(getPruningContext, minSizeForPruning, pruningInterval);
            }
            catch (Exception ex)
            {
                Environment.FailFast("Error during pruning.", ex);
                throw;
            }
        }

        public bool MaybePruneCore(Func<PruningContext> getPruningContext, long minSizeForPruning, TimeSpan pruningInterval)
        {
            if (ShouldPreserveKey == null && ShouldPreserveValue == null) return false;

            var pruneId = (DateTime.UtcNow.Ticks / 2) * 2; // Preserved slices have EVEN pruneIds. Pruned slices have ODD pruneIds.
            var now = DateTime.UtcNow;
            var prunedAnything = false;
            for (int sliceIdx = 0; sliceIdx < slices.Count; sliceIdx++)
            {
                var slice = slices[sliceIdx];

                if (slice.SizeInBytes < minSizeForPruning) continue;

                var sliceLastPruned = new DateTime(Math.Max(slice.EndTime.Ticks, slice.PruneId), DateTimeKind.Utc);
                if ((now - sliceLastPruned) < pruningInterval) continue;

                var pruningContext = getPruningContext();

                Log("  Pruning: " + slice.Reader.PathPrefix + ".col*.dat (" + ToHumanBytes(slice.SizeInBytes) + ")");
                var basePath = this.DirectoryPath + "/" + slice.StartTime.Ticks + "-" + slice.EndTime.Ticks + "-";

                var preservePruneId = pruneId++; // preservePruneId is EVEN
                var prunePruneId = pruneId++; // prunePruneId is ODD

                using var preservedWriter = new ImmutableMultiDictionaryWriter<TKey, TValue>(basePath + preservePruneId, behavior);
                using var prunedWriter = new ImmutableMultiDictionaryWriter<TKey, TValue>(basePath + prunePruneId, behavior);

                foreach (var group in slice.Reader.Enumerate())
                {
                    if (ShouldPreserveValue != null)
                    {
                        if (ShouldPreserveKey != null && !ShouldPreserveKey(pruningContext, group.Key))
                        {
                            prunedWriter.AddPresorted(group.Key, group.Values.Span.AsSmallSpan());
                        }
                        else
                        {
                            var preservedGroupCtx = preservedWriter.CreateAddContext(group.Key);
                            var prunedGroupCtx = prunedWriter.CreateAddContext(group.Key);

                            foreach (var value in group.Values)
                            {
                                if (ShouldPreserveValue(pruningContext, group.Key, value))
                                {
                                    preservedWriter.AddPresorted(ref preservedGroupCtx, value);
                                }
                                else
                                {
                                    prunedWriter.AddPresorted(ref prunedGroupCtx, value);
                                }
                            }

                            preservedWriter.FinishGroup(ref preservedGroupCtx);
                            prunedWriter.FinishGroup(ref prunedGroupCtx);
                        }
                    }
                    else
                    {
                        if (ShouldPreserveKey!(pruningContext, group.Key))
                        {
                            preservedWriter.AddPresorted(group.Key, group.Values.Span.AsSmallSpan());
                        }
                        else
                        {
                            prunedWriter.AddPresorted(group.Key, group.Values.Span.AsSmallSpan());
                        }
                    }
                }

                if (prunedWriter.KeyCount == 0)
                {
                    Log("    Nothing was pruned.");
                    prunedWriter.Dispose();
                    preservedWriter.Dispose();
                    continue;
                }
                prunedAnything = true;
                slice.ReaderHandle.Dispose();
                slices.RemoveAt(sliceIdx);

                var prunedBytes = prunedWriter.CommitAndGetSize();
                prunedSlices.Add(new SliceName(slice.StartTime, slice.EndTime, prunePruneId));

                if (preservedWriter.KeyCount != 0)
                {
                    var preservedBytes = preservedWriter.CommitAndGetSize();
                    Log($"    Pruned: {ToHumanBytes(slice.SizeInBytes)} -> {ToHumanBytes(prunedBytes)} (old) + {ToHumanBytes(preservedBytes)} (preserve)");
                    slices.Insert(sliceIdx, new SliceInfo(slice.StartTime, slice.EndTime, preservePruneId, new(new(basePath + preservePruneId, behavior, GetIoPreferenceForKeyFunc))));
                }
                else
                {
                    Log($"    Everything was pruned ({ToHumanBytes(slice.SizeInBytes)})");
                    preservedWriter.Dispose();
                    sliceIdx--;
                }

            }
            return prunedAnything;
        }

        public override string[] GetPotentiallyCorruptFiles()
        {
            return slices.SelectMany(x => x.Reader.GetPotentiallyCorruptFiles()).ToArray();
        }

        public override IEnumerable GetValuesSortedUntyped(object key, object? minExclusive)
        {
            return GetValuesSorted((TKey)key, (TValue?)minExclusive);
        }
        public override IEnumerable GetValuesPreserveOrderUntyped(object key)
        {
            return TryGetPreserveOrderSpanLatest((TKey)key, out var vals) ? vals.AsSmallSpan().ToArray() : [];
        }
        public override IEnumerable GetValuesSortedDescendingUntyped(object key, object? maxExclusive)
        {
            return GetValuesSortedDescending((TKey)key, null, (TValue?)maxExclusive);
        }
        public override IEnumerable EnumerateSortedDescendingUntyped(object? maxExclusive)
        {
            return EnumerateKeysSortedDescending((TKey?)maxExclusive);
        }

        public TCache? GetCache<TCache>() where TCache : CachedView
        {
            return Caches?.OfType<TCache>().SingleOrDefault();
        }

        public override IEnumerable<(string Name, object Value)> GetCounters()
        {
            return (Caches ?? []).Select(x =>
            {
                var typeName = x.GetType().Name;
                var quot = typeName.IndexOf('\u0060');
                if (quot != -1) typeName = typeName.Substring(0, quot);
                return (this.Name + "_" + typeName, x.GetCounters());
            }).Where(x => x.Item2 != null)!;
        }

        public KeyProbabilisticCache<TKey, TValue>? GetKeyProbabilisticCache() => this.GetCache<KeyProbabilisticCache<TKey, TValue>>();
        public KeyValueProbabilisticCache<TKey, TValue>? GetKeyValueProbabilisticCache() => this.GetCache<KeyValueProbabilisticCache<TKey, TValue>>();
        public DelegateProbabilisticCache<TKey, TValue, TProbabilistcKey>? GetDelegateProbabilisticCache<TProbabilistcKey>() where TProbabilistcKey: unmanaged => this.GetCache<DelegateProbabilisticCache<TKey, TValue, TProbabilistcKey>>();

        public abstract class CachedView : IDisposable
        {
            public abstract string Identifier { get; }

            public abstract void Add(TKey key, TValue value);

            public abstract bool ShouldPersistCacheForSlice(SliceInfo slice);

            public string GetCachePathForSlice(SliceInfo slice) => slice.Reader.PathPrefix + "." + Identifier + ".cache";

            public abstract void MaterializeCacheFile(SliceInfo slice, string destination);

            public abstract void LoadCacheFile(SliceInfo slice, string cachePath, int sliceIndex);

            public abstract void LoadFromOriginalSlice(SliceInfo slice);

            public virtual bool IsAlreadyMaterialized(string cachePath)
            {
                return File.Exists(cachePath);
            }

            public virtual void OnSliceAdded(int insertedAt, SliceInfo slice) { }
            public virtual void OnSliceRemoved(int removedAt) { }
            public virtual void Dispose() { }
            public abstract object? GetCounters();
            public abstract void EnsureSupportsSourceBehavior(PersistentDictionaryBehavior behavior);

            public abstract bool CanBeUsedByReplica { get; }
        }
    }


}

