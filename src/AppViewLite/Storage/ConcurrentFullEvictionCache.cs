using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace AppViewLite
{
    public class ConcurrentFullEvictionCache<TKey, TValue> where TKey : notnull
    {
        private ConcurrentDictionary<TKey, TValue> dict;
        private int capacity;
        private int approximateCount; // Approximate because dict = new() and approximateCount = 0 don't happen atomically.
        public ConcurrentFullEvictionCache(int capacity)
        {
            this.capacity = capacity;
            this.dict = new(-1, capacity);
        }

        public ConcurrentDictionary<TKey, TValue> Dictionary => dict;

        public readonly HitMissCounter HitMissCounter = new();

        public bool TryGetValue(TKey key, [MaybeNullWhen(false)] out TValue value)
        {
            if (dict.TryGetValue(key, out value))
            {
                HitMissCounter.OnHit();
                return true;
            }
            else
            {
                HitMissCounter.OnMiss();
                return false;
            }
        }

        public object GetCounters() => new { Count = Count, HitRatio = HitMissCounter.HitRatio, LastResetAgo = Stopwatch.GetElapsedTime(LastReset) };

        public int Count => dict.Count;
        public int ApproximateCount => approximateCount;
        public TValue this[TKey key]
        {
            set
            {
                Add(key, value);
            }
        }
        public void Add(TKey key, TValue value)
        {
            var wasAdded = true;
            dict.AddOrUpdate(key, value, (key, old) =>
            {
                wasAdded = false;
                return value;
            });
            if (wasAdded)
            {
                var incremented = Interlocked.Increment(ref approximateCount);
                if (incremented >= capacity)
                {
                    // It's ok to have races here. Count is only approximate.
                    dict = new();
                    approximateCount = 0;
                    LastReset = Stopwatch.GetTimestamp();
                }
            }
        }

        private long LastReset = Stopwatch.GetTimestamp();

        public void Remove(TKey key)
        {
            if (dict.TryRemove(key, out _))
                Interlocked.Decrement(ref approximateCount);
        }
    }
}

