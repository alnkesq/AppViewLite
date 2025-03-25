using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite
{
    public static unsafe class MemoryInstrumentation
    {
#if MEMORY_INSTRUMENTATION
        public readonly static bool IsEnabled = true;
#else
        public readonly static bool IsEnabled = false;
#endif

        const int PageSize = 4096;
        public static bool ShouldSample() => ((ulong)System.Diagnostics.Stopwatch.GetTimestamp()) % 128 == 0;

        [Conditional("MEMORY_INSTRUMENTATION")]
        public static void MaybeOnAccess<T>(ref readonly T addr)
        {
            if (ShouldSample())
                OnAccess(in addr);
        }

        [Conditional("MEMORY_INSTRUMENTATION")]
        public static void OnAccess<T>(ref readonly T item)
        {
            OnAccess(Unsafe.AsPointer(ref Unsafe.AsRef(in item)));
        }

        [Conditional("MEMORY_INSTRUMENTATION")]
        public static void OnAccess(void* ptr)
        {
            var page = ((nuint)ptr) & ~(nuint)(PageSize - 1);

            lock (LruCache)
            {
                if (LruCache.TryGetValue(page, out var _)) return;
                var now = Stopwatch.GetTimestamp();
                LruCache.Add(page, now);

                CollectionsMarshal.GetValueRefOrAddDefault(CacheMisses, page, out _)++;
            }
            //if (!LastPageAccesses.TryGetValue(page, out var prevAccessed))
            //    prevAccessed = 0;

            //var timeSincePreviousAccess = now - prevAccessed;
            //if (timeSincePreviousAccess > )
            //{

            //}
            ////Console.Error.WriteLine(timeSincePreviousAccess);
            //LastPageAccesses[page] = now;
        }

        public static LruCache<nuint, long> LruCache = new((512 * 1024 * 1024) / PageSize);
        //public static ConcurrentDictionary<nuint, long> LastPageAccesses = new();
        //public static ConcurrentDictionary<nuint, long> CacheMisses = new();
        public static Dictionary<nuint, long> CacheMisses = new();

        public static IReadOnlyList<(string TableName, long CacheMisses)> GetStats(Func<nuint, string> pageToSection)
        {
            lock (LruCache)
            {
                return CacheMisses
                    .Select(x =>
                    {
                        var section = pageToSection(x.Key);

                        if (section == null)
                        {
                            section = "UNKNOWN";
                        }
                        else
                        {
                            section = Path.GetFileName(Path.GetDirectoryName(section))
                                + (section switch
                                {
                                    _ when section.EndsWith(".col0.dat", StringComparison.Ordinal) => "_KEYS",
                                    _ when section.EndsWith(".col1.dat", StringComparison.Ordinal) => "_VALUES",
                                    _ when section.EndsWith(".col2.dat", StringComparison.Ordinal) => "_OFFSETS",
                                    _ when section.EndsWith(".cache", StringComparison.Ordinal) => "_CACHE",
                                    _ => "_UNKNOWN",
                                });
                        }


                        return (Table: section, CacheMisses: x.Value);
                    })
                    .GroupBy(x => x.Table)
                    .Select(x => (Table: x.Key, CacheMisses: x.Sum(x => x.CacheMisses)))
                    .ToArray();

            }
        }


    }
}

