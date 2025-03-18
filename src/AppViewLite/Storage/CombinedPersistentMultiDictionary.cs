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
using System.Runtime.InteropServices;

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
    }

    public abstract class CombinedPersistentMultiDictionary
    {
        public readonly string DirectoryPath;
        protected readonly PersistentDictionaryBehavior behavior;
        public readonly string Name;
        public DateTime LastFlushed;
        public long OriginalWriteBytes;
        public long CompactationWriteBytes;
        public abstract string[] GetPotentiallyCorruptFiles();

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
            var parts = baseName.Split('-');
            
            return new SliceName(new DateTime(long.Parse(parts[0]), DateTimeKind.Utc), new DateTime(long.Parse(parts[1]), DateTimeKind.Utc), parts.Length == 2 ? 0 : long.Parse(parts[2]));

        }
        public virtual long InMemorySize { get; }
        public virtual long OnDiskSize { get; }

        [DoesNotReturn]
        public static void Abort(string? message)
        {
            Abort(new Exception(message));
        }
        [DoesNotReturn]
        public static void Abort(Exception? ex)
        {
            Console.Error.WriteLine("Unexpected condition occurred. Aborting.");
            while (ex != null)
            {
                Console.Error.WriteLine(ex.Message);
                Console.Error.WriteLine(ex.StackTrace);
                ex = ex.InnerException;
            }
            Environment.FailFast("Unexpected condition occurred. Aborting.");
            throw ex;
        }

        public static void Assert(bool condition)
        {
            if (!condition)
                Abort(new Exception("Failed assertion."));
        }

        public abstract void ReturnQueueForNextReplica();

        public abstract bool MaybePrune(Func<PruningContext> getPruningContext, long minSizeForPruning, TimeSpan pruningInterval);

    }

    public class CombinedPersistentMultiDictionary<TKey, TValue> : CombinedPersistentMultiDictionary, IDisposable, IFlushable, ICheckpointable, ICloneableAsReadOnly where TKey: unmanaged, IComparable<TKey> where TValue: unmanaged, IComparable<TValue>, IEquatable<TValue>
    {
        public int WriteBufferSize = 128 * 1024;

        private Stopwatch? lastFlushed;
        public Func<IEnumerable<TValue>, IEnumerable<TValue>>? OnCompactation;
        public Func<PruningContext, TKey, bool>? ShouldPreserveKey;
        public Func<PruningContext, TKey, TValue, bool>? ShouldPreserveValue;

        public event EventHandler? AfterCompactation;
        private bool DeleteOldFilesOnCompactation;

        private CachedView[]? Caches;

#nullable disable
        private CombinedPersistentMultiDictionary(string directory, List<SliceInfo> slices, PersistentDictionaryBehavior behavior)
            : base(directory, behavior)
        {
            this.slices = slices;
        }
#nullable restore

        public CombinedPersistentMultiDictionary(string directory, SliceName[]? sliceNames, PersistentDictionaryBehavior behavior = PersistentDictionaryBehavior.SortedValues, CachedView[]? caches = null)
            : base(directory, behavior)
        {

            CompactStructCheck<TKey>.Check();
            CompactStructCheck<TValue>.Check();

            if (behavior == PersistentDictionaryBehavior.KeySetOnly && typeof(TValue) != typeof(byte))
                throw new ArgumentException("When behavior is KeySetOnly, the dummy TValue must be System.Byte.");

            System.IO.Directory.CreateDirectory(directory);

            this.slices = new();
            this.prunedSlices = new();
            if (caches != null && caches.Length == 0) caches = null;
            this.Caches = caches;
            if (caches != null)
            {
                if (behavior == PersistentDictionaryBehavior.PreserveOrder) throw new NotSupportedException();
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
                            AddToCaches(s);
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
                            AddToCaches(s);
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

        private unsafe void AddToCaches(SliceInfo slice)
        {
            if (Caches == null) return;
            foreach (var cache in Caches)
            {
                if (cache.ShouldPersistCacheForSlice(slice))
                {

                    var cachePath = cache.GetCachePathForSlice(slice);
                    if (!File.Exists(cachePath))
                    {
                        Console.Error.WriteLine("Materializing cache: " + cachePath);
                        cache.MaterializeCacheFile(slice, cachePath);
                    }

                    Console.Error.WriteLine("Reading cache: " + cachePath);
                    cache.LoadCacheFile(cachePath);
                }
                else
                {
                    Console.Error.WriteLine("Reading into cache (omit cache materialization): " + slice.Reader.PathPrefix + " [" + cache.Identifier + "]");
                    cache.LoadFromOriginalSlice(slice);
                }
            }                




        }

        private static SliceInfo OpenSlice(string directory, SliceName sliceName, PersistentDictionaryBehavior behavior, bool mandatory)
        {
            ImmutableMultiDictionaryReader<TKey, TValue> reader;
            try
            {
                reader = new ImmutableMultiDictionaryReader<TKey, TValue>(Path.Combine(directory, sliceName.BaseName), behavior);
            }
            catch (FileNotFoundException) when (!mandatory)
            {
                var fullPath = Path.Combine(directory, sliceName.BaseName);
                Console.Error.WriteLine("Partial slice: " + fullPath);
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
                 :this(startTime, endTime, pruneId, readerHandle, readerHandle.Value.SizeInBytes)
            { 
           
            }
            public ImmutableMultiDictionaryReader<TKey, TValue> Reader => ReaderHandle.Value;
        }

        public void DisposeNoFlush()
        {
            if (pendingCompactation != null) throw new Exception();

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
                slices.Add(new(date, date, 0, new(new ImmutableMultiDictionaryReader<TKey, TValue>(prefix, behavior))));
                Console.Error.WriteLine($"[{Path.GetFileName(DirectoryPath)}] Wrote {ToHumanBytes(size)}");

                if (!disposing)
                    MaybeStartCompactation();


                AfterFlush?.Invoke(this, EventArgs.Empty);
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
                long totalBytes = 0;
                long largestComponentBytes = 0;
                for (int end = start + 1; end <= slices.Count; end++)
                {
                    var componentBytes = slices[end - 1].SizeInBytes;
                    largestComponentBytes = Math.Max(largestComponentBytes, componentBytes);
                    totalBytes += componentBytes;
                    var length = end - start;
                    if (length < minLength) continue;

                    var ratioOfLargestComponent = ((double)largestComponentBytes / totalBytes);
                    if (ratioOfLargestComponent > GetMaximumRatioOfLargestSliceForCompactation(totalBytes)) continue;
                    var score = (1 - ratioOfLargestComponent) * Math.Log(length) / Math.Log(totalBytes);
                    candidates.Add(new CompactationCandidate(start, length, totalBytes, largestComponentBytes, score, ratioOfLargestComponent));
                }
            }
            return candidates;
        }

        private static double GetMaximumRatioOfLargestSliceForCompactation(long totalSize)
        {
            return 1 / Math.Max(3, Math.Log10(totalSize) - 3);
        }
        private const int TargetSliceCount = 15;
        private const int MinimumCompactationCount = 6;

        private void MaybeStartCompactation()
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
                    Compact(inputSlices, writer, OnCompactation);
                }
                catch (Exception ex)
                {
                    CombinedPersistentMultiDictionary.Abort(ex);
                }
                sw.Stop();

                return new Action(() => 
                {
                    // Here we are again inside the lock.
                    var size = writer.CommitAndGetSize(); // Writer disposal (*.dat.tmp -> *.dat) must happen inside the lock, otherwise old slice GC might see an unused slice and delete it. .tmp files are exempt (except at startup)
                    CompactationWriteBytes += size;
                    Console.Error.WriteLine("Compact (" + sw.Elapsed + ") " + Path.GetFileName(DirectoryPath) + ": " + string.Join(" + ", inputs.Select(x => ToHumanBytes(x.SizeInBytes))) + " => " + ToHumanBytes(inputs.Sum(x => x.SizeInBytes)) + " -- largest: " + compactationCandidate.RatioOfLargestComponent.ToString("0.00"));

                    foreach (var input in inputs)
                    {
                        input.ReaderHandle.Dispose();
                        if (DeleteOldFilesOnCompactation)
                        {
                            for (int i = 0; i < input.Reader.ColumnCount; i++)
                            {
                                File.Delete(input.Reader.PathPrefix + ".col" + i + ".dat");
                            }
                        }
                    }

                    // Note: Flushes/slices.Add() can happen even during the compactation.
                    slices.RemoveRange(groupStart, groupLength);
                    slices.Insert(groupStart, new SliceInfo(mergedStartTime, mergedEndTime, mergedPruneId, new(new(mergedPrefix, behavior))));

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

        public bool TryGetSingleValue(TKey key, out TValue value)
        {
            if (behavior == PersistentDictionaryBehavior.PreserveOrder) throw new InvalidOperationException();
#if true
            foreach (var slice in slices)
            {
                var vals = slice.Reader.GetValues(key);
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

        public IEnumerable<ManagedOrNativeArray<TValue>> GetValuesChunked(TKey key, TValue? minExclusive = null, TValue? maxExclusive = null)
        {
            if (behavior == PersistentDictionaryBehavior.PreserveOrder) throw new InvalidOperationException();
            ManagedOrNativeArray<TValue> extraArr = default;
            if (queue.TryGetValues(key, out var extraStruct))
            {
                var extra = extraStruct.ValuesSorted;
                if (minExclusive != null)
                {
                    if (maxExclusive != null)
                    {
                        extraArr = extra.Where(x => minExclusive.Value.CompareTo(x) < 0 && x.CompareTo(maxExclusive.Value) < 0).ToArray();
                    }
                    else
                    {
                        extraArr = extra.Where(x => minExclusive.Value.CompareTo(x) < 0).ToArray();
                    }
                }
                else if (maxExclusive != null)
                {
                    extraArr = extra.Where(x => x.CompareTo(maxExclusive.Value) < 0).ToArray();
                }
                else
                {
                    extraArr = extra.ToArray();
                }
            }
            var z = slices.Select(slice => slice.Reader.GetValues(key, minExclusive: minExclusive, maxExclusive: maxExclusive)).Where(x => x.Length != 0).Select(x => (ManagedOrNativeArray<TValue>)x);
            if (extraArr.Count != 0)
                z = z.Append(extraArr);
            return z;
        }
        public IEnumerable<ManagedOrNativeArray<TValue>> GetValuesChunkedLatestFirst(TKey key)
        {
            if (behavior == PersistentDictionaryBehavior.PreserveOrder) throw new InvalidOperationException();
            if (queue.TryGetValues(key, out var extra))
            {
                yield return extra.ValuesUnsortedArray; // this will actually be sorted
            }
            for (int i = slices.Count - 1; i >= 0; i--)
            {
                var vals = slices[i].Reader.GetValues(key);
                if (vals.Length != 0)
                    yield return vals;
            }
        }


        public bool TryGetPreserveOrderSpanAny(TKey key, out ManagedOrNativeArray<TValue> val)
        {
            if (behavior != PersistentDictionaryBehavior.PreserveOrder) throw new InvalidOperationException();
            foreach (var slice in slices)
            {
                var v = slice.Reader.GetValues(key);
                if (v.Length != 0)
                {
                    val = v;
                    return true;
                }

            }

            if (queue.TryGetValues(key, out var q))
            {
                val = q.ValuesUnsortedArray;
                return true;
            }
            val = default;
            return false;
        }
        public bool TryGetPreserveOrderSpanLatest(TKey key, out ManagedOrNativeArray<TValue> val)
        {
            if (behavior != PersistentDictionaryBehavior.PreserveOrder) throw new InvalidOperationException();
            if (queue.TryGetValues(key, out var extra))
            {
                val = extra.ValuesUnsortedArray;
                return true;
            }
            for (int i = slices.Count - 1; i >= 0; i--)
            {
                var v = slices[i].Reader.GetValues(key);
                if (v.Length != 0) 
                {
                    val = v;
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

        public IEnumerable<TValue> GetValuesUnsorted(TKey key, TValue? minExclusive = null, TValue? maxExclusive = null)
        {
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

        public bool Contains(TKey key, TValue value)
        {
            if (behavior == PersistentDictionaryBehavior.PreserveOrder) throw new InvalidOperationException();
            return slices.Any(slice => slice.Reader.Contains(key, value)) || queue.TryGetValues(key).Contains(value);
        }
        public bool ContainsKey(TKey key)
        {
            foreach (var slice in slices)
            {
                if (slice.Reader.ContainsKey(key)) return true;
            }
            if (queue.ContainsKey(key)) return true;
            return false;
        }

        public long GetValueCount(TKey key)
        {
            if (behavior == PersistentDictionaryBehavior.PreserveOrder) throw new InvalidOperationException();
            long num = 0;
            foreach (var slice in slices)
            {
                num += slice.Reader.GetValueCount(key);
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

        public long GroupCount => queue.GroupCount + slices.Sum(x => x.Reader.KeyCount);
        public long ValueCount => queue.ValueCount + slices.Sum(x => x.Reader.ValueCount);

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

        private void MaybeCommitPendingCompactation(bool forceWait = false)
        {
            try
            {
                if (pendingCompactation == null) return;
                if (forceWait)
                {
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
                    output.AddPresorted(group.Key, mostRecent.Span.AsSmallSpan);
                }

            }
            else
            {

                foreach (var group in groupedSlices)
                {
                    if (group.Values.Count == 1)
                    {
                        output.AddPresorted(group.Key, group.Values[0].Span.AsSmallSpan);
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


        public IEnumerable<(TKey Key, ManagedOrNativeArray<TValue> Values)> EnumerateUnsortedGrouped()
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
                yield return (q.Key, q.Value.ValuesUnsortedArray);
            }
        }

        public bool IsSingleValueOrKeySet => behavior is PersistentDictionaryBehavior.SingleValue or PersistentDictionaryBehavior.KeySetOnly;
        public bool HasOffsets => !IsSingleValueOrKeySet;
        public bool HasValues => behavior != PersistentDictionaryBehavior.KeySetOnly;


        public IEnumerable<(TKey Key, ManagedOrNativeArray<TValue>[] ValueChunks)> EnumerateSortedGrouped()
        {
            if (IsSingleValueOrKeySet) throw new InvalidOperationException();
            var s = slices.Select(x => x.Reader.Enumerate().Select(x => (x.Key, Values: (ManagedOrNativeArray<TValue>)x.Values)));
            if (queue.GroupCount != 0)
            {
                s = s.Append(queue.Select(x => (x.Key, Values: (ManagedOrNativeArray<TValue>)x.Values.ValuesSorted.ToArray())));
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
        public IEnumerable<ManagedOrNativeArray<TKey>> EnumerateKeyChunks()
        {
            foreach (var slice in slices)
            {
                yield return (DangerousHugeReadOnlyMemory<TKey>)slice.Reader.Keys;
            }
            if (queue.GroupCount != 0)
            {
                var keys = queue.Keys.ToArray();
                Array.Sort(keys);
                yield return keys;
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

        public IEnumerable<(TKey Key, ManagedOrNativeArray<TValue> Values)> GetInRangeUnsorted(TKey min, TKey maxExclusive)
        {
            foreach (var q in queue)
            {
                if (IsInRange(q.Key, min, maxExclusive))
                    yield return (q.Key, q.Values.ValuesUnsortedArray);
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
                    yield return (key, slice.Reader.GetValues(i));
                }
                

                for (long i = centerIndex + 1; i < keys.Length; i++)
                {
                    var key = keys.Span[i];
                    if (!IsInRange(key, min, maxExclusive)) break;
                    yield return (key, slice.Reader.GetValues(i));
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

        public CombinedPersistentMultiDictionary<TKey, TValue> CloneAsReadOnly()
        {
            var copy = new CombinedPersistentMultiDictionary<TKey, TValue>(DirectoryPath, this.slices.Select(x => new SliceInfo(x.StartTime, x.EndTime, x.PruneId, x.ReaderHandle.AddRef())).ToList(), behavior);
            copy.ReplicatedFrom = this;

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
                        copy.queue.Groups[key] = this.queue.Groups[key];
                    }

                    if (!lastSliceIsEmpty)
                        this.queue.CreateVirtualSlice(); // nobody else is writing to primary.

                    //Console.Error.WriteLine("Queue copied incrementally.");
                }

                
            }

            if (copy.queue == null)
            {
                copy.queue = this.queue.CloneAndMaybeCreateNewVirtualSlice();
                //Console.Error.WriteLine("Queued copied from scratch.");
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

                Console.Error.WriteLine("  Pruning: " + slice.Reader.PathPrefix + ".col*.dat (" + ToHumanBytes(slice.SizeInBytes) + ")");
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
                            prunedWriter.AddPresorted(group.Key, group.Values.Span.AsSmallSpan);
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
                            preservedWriter.AddPresorted(group.Key, group.Values.Span.AsSmallSpan);
                        }
                        else
                        {
                            prunedWriter.AddPresorted(group.Key, group.Values.Span.AsSmallSpan);
                        }
                    }
                }

                if (prunedWriter.KeyCount == 0)
                {
                    Console.Error.WriteLine("    Nothing was pruned.");
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
                    Console.Error.WriteLine($"    Pruned: {ToHumanBytes(slice.SizeInBytes)} -> {ToHumanBytes(prunedBytes)} (old) + {ToHumanBytes(preservedBytes)} (preserve)");
                    slices.Insert(sliceIdx, new SliceInfo(slice.StartTime, slice.EndTime, preservePruneId, new(new(basePath + preservePruneId, behavior))));
                }
                else
                {
                    Console.Error.WriteLine($"    Everything was pruned ({ToHumanBytes(slice.SizeInBytes)})");
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

        public abstract class CachedView
        {
            public abstract string Identifier { get; }

            public abstract void Add(TKey key, TValue value);

            public abstract bool ShouldPersistCacheForSlice(SliceInfo slice);

            public string GetCachePathForSlice(SliceInfo slice) => slice.Reader.PathPrefix + "." + Identifier + ".cache";

            public abstract void MaterializeCacheFile(SliceInfo slice, string destination);

            public abstract void LoadCacheFile(string cachePath);

            public abstract void LoadFromOriginalSlice(SliceInfo slice);
        }
    }






}

