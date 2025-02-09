using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite
{
    internal class ConcurrentSet<T> where T : notnull
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
    }
}

