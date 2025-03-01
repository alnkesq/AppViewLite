using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite
{
    public class LruCache<TKey, TValue> where TKey: notnull
    {
        private readonly Dictionary<TKey, LinkedListNode<KeyValuePair<TKey, TValue>>> dict = new();
        private readonly LinkedList<KeyValuePair<TKey, TValue>> lru = new();
        private readonly int capacity;
        public LruCache(int capacity)
        {
            this.capacity = capacity;
        }

        public bool TryGetValue(TKey key, [MaybeNullWhen(false)] out TValue value)
        {
            if (dict.TryGetValue(key, out var existing))
            {
                lru.Remove(existing);
                lru.AddFirst(existing);
                value = existing.Value.Value;
                return true;
            }
            value = default!;
            return false;
        }

        public void Add(TKey key, TValue value)
        {
            if (dict.TryGetValue(key, out var existing))
            {
                existing.Value = new(key, value);
                lru.Remove(existing);
                lru.AddFirst(existing);
                return;
            }
            while (dict.Count >= capacity)
            {
                var last = lru.Last;
                lru.RemoveLast();
                dict.Remove(last!.Value.Key);
            }
            var linkedListNode = new LinkedListNode<KeyValuePair<TKey, TValue>>(new(key, value));
            dict[key] = linkedListNode;
            lru.AddFirst(linkedListNode);
        }

    }
}

