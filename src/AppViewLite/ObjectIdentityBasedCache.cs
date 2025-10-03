using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite
{
    public class ObjectIdentityBasedCache<TSource, TCache> where TSource : class
    {
        public required TSource Source;
        public required TCache Cache;
    }

    public static class ObjectIdentityBasedCache
    {
        public static TCache GetOrCreateCache<TSource, TCache>(TSource source, ref ObjectIdentityBasedCache<TSource, TCache>? cache, Func<TSource, TCache> cacheFactory) where TSource : class
        {
            var existing = cache;
            if (existing != null && Object.ReferenceEquals(existing.Source, source)) return existing.Cache;

            var result = cacheFactory(source);
            existing = new()
            {
                Source = source,
                Cache = result,
            };
            cache = existing;
            return result;
        }
    }
}

