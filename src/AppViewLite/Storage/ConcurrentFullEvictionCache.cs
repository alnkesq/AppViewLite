using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite
{
    public class ConcurrentFullEvictionCache<TKey, TValue> where TKey : notnull
    {
        private ConcurrentDictionary<TKey, TValue> dict;
        private int capacity;
        public ConcurrentFullEvictionCache(int capacity)
        {
            this.capacity = capacity;
            this.dict = new(-1, capacity);
        }

        public bool TryGetValue(TKey key, [MaybeNullWhen(false)] out TValue value)
        {
            return dict.TryGetValue(key, out value);
        }


        public TValue this[TKey key]
        {
            set
            {
                Add(key, value);
            }
        }
        public void Add(TKey key, TValue value)
        {
            if (dict.Count == capacity)
            {
                // Console.Error.WriteLine("Evict everything.");
                dict = new(-1, capacity);
            }
            if (!dict.ContainsKey(key))
                dict[key] = value;
        }

        public void Remove(TKey key)
        {
            dict.TryRemove(key, out _);
        }
    }
}

