using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace AppViewLite
{
    public class ConcurrentSet<T> : IReadOnlySet<T> where T :notnull
    {
        public ConcurrentSet()
        {
            inner = new();
        }

        public ConcurrentSet(IEnumerable<T> source)
            : this()
        {
            foreach (var item in source)
            {
                Add(item);
            }
        }

        private readonly ConcurrentDictionary<T, byte> inner;

        public IEnumerator<T> GetEnumerator()
        {
            return inner.Keys.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public bool Add(T item) => TryAdd(item);
        public bool TryAdd(T item) => inner.TryAdd(item, 0);


        public void Remove(T value)
        {
            inner.TryRemove(value, out _);
        }
        public void Clear()
        {
            inner.Clear();
        }

        public bool Contains(T item)
        {
            return inner.ContainsKey(item);
        }

        public bool IsProperSubsetOf(IEnumerable<T> other)
        {
            throw new NotImplementedException();
        }

        public bool IsProperSupersetOf(IEnumerable<T> other)
        {
            throw new NotImplementedException();
        }

        public bool IsSubsetOf(IEnumerable<T> other)
        {
            throw new NotImplementedException();
        }

        public bool IsSupersetOf(IEnumerable<T> other)
        {
            throw new NotImplementedException();
        }

        public bool Overlaps(IEnumerable<T> other)
        {
            throw new NotImplementedException();
        }

        public bool SetEquals(IEnumerable<T> other)
        {
            throw new NotImplementedException();
        }

        public int Count => inner.Count;
    }
}

