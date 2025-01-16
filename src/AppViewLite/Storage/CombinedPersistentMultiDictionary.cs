using AppViewLite;
using AppViewLite.Storage;
using AppViewLite.Storage;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

namespace AppViewLite.Storage
{

    public enum PersistentDictionaryBehavior
    {
        SortedValues,
        SingleValue,
        PreserveOrder,
    }
    
    public class CombinedPersistentMultiDictionary<TKey, TValue> : IDisposable where TKey: unmanaged, IComparable<TKey> where TValue: unmanaged, IComparable<TValue>
    {
        private readonly string DirectoryPath;
        private readonly PersistentDictionaryBehavior behavior;
        public int ItemsToBuffer = 128 * 1024;

        private Stopwatch? lastFlushed;
        private bool IsSingleValue => behavior == PersistentDictionaryBehavior.SingleValue;
        public Func<IEnumerable<TValue>, IEnumerable<TValue>>? OnCompactation;
        public CombinedPersistentMultiDictionary(string directory, PersistentDictionaryBehavior behavior = PersistentDictionaryBehavior.SortedValues)
        {
            this.DirectoryPath = directory;
            this.behavior = behavior;

            System.IO.Directory.CreateDirectory(directory);

            slices = Directory.EnumerateFiles(directory, "*.col0.dat").Select(x =>
            {
                var fileName = Path.GetFileName(x);
                var baseName = fileName.Substring(0, fileName.LastIndexOf(".col", StringComparison.Ordinal));
                ImmutableMultiDictionaryReader<TKey, TValue> reader;
                try
                {
                    reader = new ImmutableMultiDictionaryReader<TKey, TValue>(Path.Combine(directory, baseName), behavior);
                }
                catch (FileNotFoundException)
                {
                    Console.Error.WriteLine("Partial slice: " + x);
                    File.Move(x, x + ".orphan-slice");
                    return default;
                }
                
                var dash = baseName.IndexOf('-');
                return new SliceInfo(new DateTime(long.Parse(baseName.Substring(0, dash)), DateTimeKind.Utc), long.Parse(baseName.Substring(dash + 1)) , reader);

            }).Where(x => x.Reader != null).ToList();
        }

        private MultiDictionary<TKey, TValue> queue = new();
        public List<SliceInfo> slices;

        public event EventHandler BeforeFlush;

        public MultiDictionary<TKey, TValue> QueuedItems => queue;
        public record struct SliceInfo(DateTime StartTime, long Count, ImmutableMultiDictionaryReader<TKey, TValue> Reader);

        
        public void Dispose() // Dispose() must also be called while holding the lock.
        {
            Flush(avoidNewCompactations: true);
            if (pendingCompactation != null) throw new Exception();

            foreach (var slice in slices)
            {
                slice.Reader.Dispose();
            }
        }

        public long GroupCount => slices.Sum(x => x.Count) + queue.GroupCount;

        public void Flush(bool avoidNewCompactations = false)
        {
            MaybeCommitPendingCompactation(forceWait: true);

            if (queue.GroupCount != 0)
            {
                BeforeFlush?.Invoke(this, EventArgs.Empty);

                var date = DateTime.UtcNow;

                if (date < prevSliceDate) date = prevSliceDate;
                if (date == prevSliceDate)
                {
                    date = date.AddTicks(1);
                }

                prevSliceDate = date;

                var groupCount = queue.GroupCount;
                var prefix = DirectoryPath + "/" + date.Ticks + "-" + groupCount;
                using var writer = new ImmutableMultiDictionaryWriter<TKey, TValue>(prefix, behavior);

                if (behavior == PersistentDictionaryBehavior.PreserveOrder)
                {
                    foreach (var key in queue.Groups.OrderBy(x => x.Key))
                    {
                        writer.AddPresorted(key.Key, CollectionsMarshal.AsSpan(key.Value));
                    }
                    writer.Commit();
                }
                else
                {
                    writer.AddAllAndCommit(queue.AllEntries.Select(x => (Key: x.Key, Value: x.Value)));
                }
                queue.Clear();
                slices.Add(new(date, groupCount, new ImmutableMultiDictionaryReader<TKey, TValue>(prefix, behavior)));
                

                if (!avoidNewCompactations)
                    MaybeCompact();
                lastFlushed = null;
            }
        }


        private DateTime prevSliceDate;

        private void MaybeCompactOnce()
        {
            if (pendingCompactation != null) throw new InvalidOperationException();

            var desiredGroupSize = 4;


            if (slices.Count < desiredGroupSize) return;

            var last = slices[^desiredGroupSize..];

            var total = last.Sum(x => x.Count);
            var max = last.Max(x => x.Count);
            var secondMax = last.OrderByDescending(x => x.Count).ElementAt(1).Count;

            var a = secondMax * 2 > max;
            var b = total >= max * (desiredGroupSize - 1);

            for (int i = slices.Count - desiredGroupSize - 1; i >= 0; i--)
            {
                var extra = slices[i];
                if (extra.Count < max)
                {
                    last.Insert(0, extra);
                    total += extra.Count;
                }
                else
                    break;
            }


            if ((a && b) || last.Count >= 6)
            {
                Compact(slices.Count - last.Count, last.Count);
            }

            
  


        }

        public void MaybeCompact()
        {
            while (true)
            {
                var prevSlices = slices.Count;
                MaybeCompactOnce();
                if (slices.Count == prevSlices) return;
            }
        }

        private void Compact(int groupStart, int groupLength)
        {
            if (groupLength <= 1) return;
            var inputs = slices.Slice(groupStart, groupLength);
            var mergedDate = inputs.Min(x => x.StartTime);
            var mergedCount = inputs.Sum(x => x.Count);
            var mergedPrefix = this.DirectoryPath + "/" + mergedDate.Ticks + "-" + mergedCount;

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
                finally
                {
                    writer.Dispose();
                }
                sw.Stop();

                return new Action(() => 
                {
                    // Here we are again inside the lock.
                    Console.Error.WriteLine("Compact (" + sw.Elapsed + ") " + Path.GetFileName(DirectoryPath) + ": " + string.Join(" + ", inputs.Select(x => x.Count)) + " => " + inputs.Sum(x => x.Count));

                    foreach (var input in inputs)
                    {
                        input.Reader.Dispose();
                        for (int i = 0; i < (IsSingleValue ? 2 : 3); i++)
                        {
                            File.Delete(input.Reader.PathPrefix + ".col" + i + ".dat");
                        }
                    }
                    slices.RemoveRange(groupStart, groupLength);
                    slices.Insert(groupStart, new SliceInfo(mergedDate, mergedCount, new(mergedPrefix, behavior)));
                });
            }, TaskCreationOptions.LongRunning);

            pendingCompactation = compactationThread;


        }

        public bool TryGetSingleValue(TKey key, out TValue value)
        {
            if (behavior == PersistentDictionaryBehavior.PreserveOrder) throw new InvalidOperationException();
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
            value = default;
            return false;
        }

        public IEnumerable<ManagedOrNativeArray<TValue>> GetValuesChunked(TKey key, TValue? minExclusive = null)
        {
            if (behavior == PersistentDictionaryBehavior.PreserveOrder) throw new InvalidOperationException();
            ManagedOrNativeArray<TValue> extraArr = default;
            if (queue.TryGetValues(key, out var extra))
            {
                if (minExclusive != null)
                {
                    extraArr = extra.Where(x => minExclusive.Value.CompareTo(x) < 0).Order().ToArray();
                }
                else
                {
                    extraArr = extra.Order().ToArray();
                }
            }
            var z = slices.Select(slice => slice.Reader.GetValues(key, minExclusive: minExclusive)).Where(x => x.Length != 0).Select(x => (ManagedOrNativeArray<TValue>)x);
            if (extraArr.Count != 0)
                z = z.Append(extraArr);
            return z;
        }
        public IEnumerable<ManagedOrNativeArray<TValue>> GetValuesChunkedLatestFirst(TKey key)
        {
            if (behavior == PersistentDictionaryBehavior.PreserveOrder) throw new InvalidOperationException();
            var extra = queue.TryGetValues(key);
            if (extra.Count != 0) yield return extra.ToArray();
            for (int i = slices.Count - 1; i >= 0; i--)
            {
                var vals = slices[i].Reader.GetValues(key);
                if (vals.Length != 0)
                    yield return vals;
            }
        }


        public bool TryGetPreserveOrderSpan(TKey key, out ManagedOrNativeArray<TValue> val)
        {
            if (behavior != PersistentDictionaryBehavior.PreserveOrder) throw new InvalidOperationException();
            if (queue.TryGetValues(key, out var extra))
            {
                val = extra.ToArray();
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

        public IEnumerable<TValue> GetValuesSorted(TKey key, TValue? minExclusive = null)
        {
            var chunks = GetValuesChunked(key, minExclusive).ToArray();
            if (chunks.Length == 0) return [];
            if (chunks.Length == 1) return chunks[0].AsEnumerable();
            var chunksEnumerables = chunks.Select(x => x.AsEnumerable()).ToArray();
            return SimpleJoin.ConcatPresortedEnumerablesKeepOrdered(chunksEnumerables, x => x);
        }

        public IEnumerable<TValue> GetValuesUnsorted(TKey key)
        {
            if (behavior == PersistentDictionaryBehavior.PreserveOrder)
            {
                var q = queue.TryGetValues(key);
                if (q.Count != 0)
                {
                    foreach (var item in queue.TryGetValues(key))
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
                foreach (var item in queue.TryGetValues(key))
                {
                    yield return item;
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
            if (behavior == PersistentDictionaryBehavior.PreserveOrder) throw new InvalidOperationException();
            queue.Add(key, value);
            MaybeFlush();
        }
        public void AddRange(TKey key, ReadOnlySpan<TValue> values)
        {
            if (behavior == PersistentDictionaryBehavior.PreserveOrder)
            {
                if (values.Length == 0) throw new ArgumentException();
                queue.RemoveAll(key);
            }
            queue.AddRange(key, values);
            MaybeFlush();
        }


        public TimeSpan MaximumInMemoryBufferDuration { get; set; } = TimeSpan.FromMinutes(5);

        private void MaybeFlush()
        {
            MaybeCommitPendingCompactation();
            if (pendingCompactation != null) return;
            
            if (queue.GroupCount >= ItemsToBuffer || (lastFlushed != null && lastFlushed.Elapsed > MaximumInMemoryBufferDuration))
            {
                Flush();
            }
            lastFlushed ??= Stopwatch.StartNew();
        }


        // Setting and reading this field requires a lock, but the underlying operation can finish at any time.
        private Task<Action>? pendingCompactation;

        private void MaybeCommitPendingCompactation(bool forceWait = false)
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

        private static void Compact(IReadOnlyList<ImmutableMultiDictionaryReader<TKey, TValue>> inputs, ImmutableMultiDictionaryWriter<TKey, TValue> output, Func<IEnumerable<TValue>, IEnumerable<TValue>>? onCompactation)
        {
            if (output.behavior == PersistentDictionaryBehavior.PreserveOrder)
            {
                if (onCompactation != null) throw new ArgumentException();
                var concatenatedSlices = SimpleJoin.ConcatPresortedEnumerablesKeepOrdered(inputs.Select(x => x.Enumerate()).ToArray(), (x, i) => (x.Key, i)).ToArray();
                var groupedSlices = SimpleJoin.GroupAssumingOrderedInput(concatenatedSlices).ToArray();

                foreach (var group in groupedSlices)
                {
                    if(group.Values.Count != 1) { }
                    var mostRecent = group.Values[^1];
                    output.AddPresorted(group.Key, mostRecent);
                }

            }
            else
            {
                var concatenatedSlices = SimpleJoin.ConcatPresortedEnumerablesKeepOrdered(inputs.Select(x => x.Enumerate()).ToArray(), x => x.Key);
                var groupedSlices = SimpleJoin.GroupAssumingOrderedInput(concatenatedSlices);

                foreach (var group in groupedSlices)
                {
                    if (group.Values.Count == 1)
                    {
                        output.AddPresorted(group.Key, group.Values[0].Span.AsSmallSpan);
                    }
                    else
                    {
                        var enumerables = group.Values.Select(x => x.AsEnumerable()).ToArray();
                        var merged = SimpleJoin.ConcatPresortedEnumerablesKeepOrdered(enumerables, x => x);
                        if (onCompactation != null)
                            merged = onCompactation(merged);

                        output.AddPresorted(group.Key, merged);
                    }
                }
            }
            output.Commit();
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

        public IEnumerable<(TKey Key, ManagedOrNativeArray<TValue> Values)> GetInRangeUnsorted(TKey min, TKey maxExclusive)
        {
            foreach (var q in queue)
            {
                if (IsInRange(q.Key, min, maxExclusive))
                    yield return (q.Key, q.Values.ToArray());
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
    }

}

