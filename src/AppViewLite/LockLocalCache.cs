using AppViewLite.Models;
using AppViewLite.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite
{
    public class LockLocalCache<TKey, TValue> where TKey: notnull
    {
        private readonly Dictionary<TKey, TValue> _cache = new();

        public TValue GetOrFetch(TKey key, Func<TValue> fetch)
        {
            if (_cache.TryGetValue(key, out var v)) return v;
            _cache[key] = v = fetch();
            return v;
        }
        public int Count => _cache.Count;
    }

    public class LockLocalCaches
    {
        public readonly LockLocalCache<Relationship, DangerousHugeReadOnlyMemory<ListEntry>[]> ListMembers = new();
    }

}

