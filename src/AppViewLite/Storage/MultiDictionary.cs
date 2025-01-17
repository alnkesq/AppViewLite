using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace AppViewLite.Storage
{
    public class MultiDictionary<TKey, TValue> : IEnumerable<(TKey Key, IReadOnlyList<TValue> Values)>
    {
        private Dictionary<TKey, List<TValue>> dict = new();

        public int GroupCount => dict.Count;

        public void RemoveAll(TKey key) => dict.Remove(key);
        public void Add(TKey key, TValue value)
        {
            if (!dict.TryGetValue(key, out var list))
            {
                list = new();
                dict.Add(key, list);
            }
            list.Add(value);
        }

        public void AddRange(TKey key, ReadOnlySpan<TValue> values)
        {
            if (!dict.TryGetValue(key, out var list))
            {
                list = new();
                dict.Add(key, list);
            }
            list.AddRange(values);
        }

        public bool ContainsKey(TKey key) => dict.ContainsKey(key);

        public bool Contains(TKey key, TValue value)
        {
            if (TryGetValues(key, out var values))
                return values.Contains(value);
            return false;
        }

        public IEnumerator<(TKey Key, IReadOnlyList<TValue> Values)> GetEnumerator()
        {
            return dict.Select(x => (x.Key, (IReadOnlyList<TValue>)x.Value)).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public bool TryGetValues(TKey key, out List<TValue> values)
        {
            return dict.TryGetValue(key, out values);
        }

        public IReadOnlyList<TValue> TryGetValues(TKey key)
        {
            if (TryGetValues(key, out var vals)) return vals;
            return [];
        }

        public void Clear() => dict.Clear();

        public Dictionary<TKey, List<TValue>> Groups => dict;

        public IEnumerable<(TKey Key, TValue Value)> AllEntries => dict.SelectMany(x => x.Value, (group, list) => (group.Key, list));
    }
}
