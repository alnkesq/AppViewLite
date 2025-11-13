using AppViewLite.Storage;
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
        public ConcurrentFullEvictionCache(int capacity, HitMissCounter? hitMissCounter = null)
        {
            this.capacity = capacity;
            this.dict = new(-1, capacity);
            this.HitMissCounter = hitMissCounter ?? new();
        }

        public ConcurrentDictionary<TKey, TValue> Dictionary => dict;

        public readonly HitMissCounter HitMissCounter;
        public event Action? AfterReset;
        public event Action<TValue>? ValueAdded;

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

        public object GetCounters() => new { Count, ApproximateCount, HitRatio = HitMissCounter.HitRatio, LastResetAgo = Stopwatch.GetElapsedTime(LastReset) };

        public int Count => dict.Count;
        public int ApproximateCount => approximateCount;
        public TValue this[TKey key]
        {
            set
            {
                Add(key, value);
            }
        }

        public TValue GetOrAdd(TKey key, Func<TValue> factory)
        {
            var wasAdded = false;
            var value = dict.GetOrAdd(key, (key) =>
            {
                wasAdded = true; // racy but doesn't matter, approximation only
                return factory();
            });
            if (wasAdded)
            {
                HitMissCounter.OnMiss();
                IncrementApproximateCountAndMaybeReset(value);
            }
            else
            {
                HitMissCounter.OnHit();
            }
            return value;
        }
        public unsafe TValue GetOrAdd<TArg>(TKey key, Func<TKey, TArg, TValue> factory, TArg arg)
        {
            var wasAdded = false;
            var value = dict.GetOrAdd(key, static (key, argg) =>
            {
                argg.WasAddedPtr.AsRef = true; // racy but doesn't matter, approximation only
                return argg.factory(key, argg.arg);
            }, (factory, arg, WasAddedPtr: (UnsafePointer<bool>)(&wasAdded)));
            if (wasAdded)
            {
                HitMissCounter.OnMiss();
                IncrementApproximateCountAndMaybeReset(value);
            }
            else
            {
                HitMissCounter.OnHit();
            }
            return value;
        }
        public bool Add(TKey key, TValue value)
        {
            var wasAdded = true;
            dict.AddOrUpdate(key, value, (key, old) =>
            {
                wasAdded = false; // Updating, so we must NOT increment approx count
                return value;
            });
            if (wasAdded)
            {
                IncrementApproximateCountAndMaybeReset(value);
            }
            return wasAdded;
        }

        private void IncrementApproximateCountAndMaybeReset(TValue value)
        {
            ValueAdded?.Invoke(value);
            var incremented = Interlocked.Increment(ref approximateCount);
            if (incremented >= capacity)
            {
                Reset();
            }
        }

        public void Reset()
        {
            // It's ok to have races here. Count is only approximate.
            dict = new();
            approximateCount = 0;
            LastReset = Stopwatch.GetTimestamp();
            AfterReset?.Invoke();
        }

        private long LastReset = Stopwatch.GetTimestamp();

        public void Remove(TKey key)
        {
            if (dict.TryRemove(key, out _))
                Interlocked.Decrement(ref approximateCount);
        }

        public void UpdateIfExists(TKey key, TValue updatedValue)
        {
            if (dict.ContainsKey(key))
                dict[key] = updatedValue;
        }
    }

    public class ConcurrentFullEvictionSetCache<TKey> where TKey : notnull
    {
        private ConcurrentFullEvictionCache<TKey, byte> inner;
        public ConcurrentFullEvictionSetCache(int capacity)
        {
            this.inner = new(capacity);
        }

        public bool Add(TKey key)
        {
            return inner.Add(key, 0);
        }

        public bool Contains(TKey key) => inner.Dictionary.ContainsKey(key);
    }
}

