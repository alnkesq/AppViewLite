using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
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

        public bool TryGetValue(TKey key, [MaybeNullWhen(false)] out TValue value)
        {
            return dict.TryGetValue(key, out value);
        }

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
            if (dict.TryAdd(key, value))
            {
                var incremented = Interlocked.Increment(ref approximateCount);
                if (incremented >= capacity)
                {
                    // It's ok to have races here. Count is only approximate.
                    dict = new();
                    approximateCount = 0;
                }
            }
        }

        public void Remove(TKey key)
        {
            if (dict.TryRemove(key, out _))
                Interlocked.Decrement(ref approximateCount);
        }
    }
}

