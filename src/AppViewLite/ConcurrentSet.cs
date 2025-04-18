using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite
{
    public class ConcurrentSet<T> : IEnumerable<T> where T : notnull
    {
        private ConcurrentDictionary<T, bool> dict = new();
        public int Count => dict.Count;
        public bool TryAdd(T value)
        {
            return dict.TryAdd(value, false);
        }

        public void Remove(T value)
        {
            dict.TryRemove(value, out _);
        }

        public bool Contains(T value) => dict.ContainsKey(value);

        public void Clear()
        {
            dict.Clear();
        }

        public IEnumerator<T> GetEnumerator()
        {
            return dict.Keys.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}

