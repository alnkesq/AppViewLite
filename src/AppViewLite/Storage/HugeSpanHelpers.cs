using AppViewLite.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite
{
    // Adapted from https://github.com/dotnet/runtime/blob/f3d43efa514a87f04aa994749409297e8f3e94e2/src/libraries/System.Private.CoreLib/src/System/SpanHelpers.BinarySearch.cs#L4
    public static partial class HugeSpanHelpers
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long BinarySearch<T, TComparable>(
            HugeReadOnlySpan<T> span, TComparable comparable)
            where TComparable : IComparable<T>, allows ref struct
        {
            if (comparable == null)
                throw new ArgumentNullException();

            return BinarySearch(ref Unsafe.AsRef(in span.GetReference()), span.Length, comparable);
        }

        public static long BinarySearch<T, TComparable>(
            ref T spanStart, long length, TComparable comparable)
            where TComparable : IComparable<T>, allows ref struct
        {
            long lo = 0;
            long hi = length - 1;
            // If length == 0, hi == -1, and loop will not be entered
            while (lo <= hi)
            {
                // PERF: `lo` or `hi` will never be negative inside the loop,
                //       so computing median using uints is safe since we know
                //       `length <= int.MaxValue`, and indices are >= 0
                //       and thus cannot overflow an uint.
                //       Saves one subtraction per loop compared to
                //       `int i = lo + ((hi - lo) >> 1);`
                long i = (long)(((ulong)hi + (ulong)lo) >> 1);

                // MemoryInstrumentation.OnAccess(ref Unsafe.Add(ref spanStart, (nint)i));
                long c = comparable.CompareTo(Unsafe.Add(ref spanStart, (nint)i));
                if (c == 0)
                {
                    return i;
                }
                else if (c > 0)
                {
                    lo = i + 1;
                }
                else
                {
                    hi = i - 1;
                }
            }
            // If none found, then a negative number that is the bitwise complement
            // of the index of the next element that is larger than or, if there is
            // no larger element, the bitwise complement of `length`, which
            // is `lo` at this point.
            return ~lo;
        }

        // Helper to allow sharing all code via IComparable<T> inlineable
        internal readonly ref struct ComparerComparable<T, TComparer> : IComparable<T>
            where TComparer : IComparer<T>, allows ref struct
        {
            private readonly T _value;
            private readonly TComparer _comparer;

            public ComparerComparable(T value, TComparer comparer)
            {
                _value = value;
                _comparer = comparer;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int CompareTo(T? other) => _comparer.Compare(_value, other);
        }

        public static int IndexOfUsingBinarySearch<T>(this Span<T> span, T needle, Func<T, bool> hasDesiredPrefix) where T : IComparable<T>
        {
            return IndexOfUsingBinarySearch((ReadOnlySpan<T>)span, needle, hasDesiredPrefix);
        }
        public static int IndexOfUsingBinarySearchLatest<T>(this Span<T> span, T needle, Func<T, bool> hasDesiredPrefix) where T : IComparable<T>
        {
            return IndexOfUsingBinarySearchLatest((ReadOnlySpan<T>)span, needle, hasDesiredPrefix);
        }

        public static int IndexOfUsingBinarySearch<T>(this ReadOnlySpan<T> span, T needle, Func<T, bool> hasDesiredPrefix) where T : IComparable<T>
        {
            var index = span.BinarySearch(needle);
            if (index >= 0) return index;

            index = ~index;
            if (index == span.Length) return -1;
            var item = span[index];
            if (hasDesiredPrefix(item)) return index;
            return -1;
        }


        public static long IndexOfUsingBinarySearch<T>(this HugeReadOnlySpan<T> span, T needleValueOrPrefix, Func<T, bool> hasDesiredPrefix) where T : IComparable<T>
        {
            var index = span.BinarySearch(needleValueOrPrefix);
            if (index >= 0) return index;

            index = ~index;
            if (index == span.Length) return -1;
            var item = span[index];
            if (hasDesiredPrefix(item)) return index;
            return -1;
        }


        public static int IndexOfUsingBinarySearchLatest<T>(this ReadOnlySpan<T> span, T needle, Func<T, bool> hasDesiredPrefix) where T : IComparable<T>
        {
            var index = span.BinarySearch(needle);

            if (index < 0)
            {
                index = ~index;
                if (index == span.Length) return -1;
                if (!hasDesiredPrefix(span[index])) return -1;
            }

            while (true)
            {
                var indexNext = index + 1;
                if (indexNext < span.Length && hasDesiredPrefix(span[indexNext]))
                {
                    index++;
                }
                else break;
            }

            return index;
        }


        public static long IndexOfUsingBinarySearchLatest<T>(this HugeReadOnlySpan<T> span, T needle, Func<T, bool> hasDesiredPrefix) where T : IComparable<T>
        {
            var index = span.BinarySearch(needle);

            if (index < 0)
            {
                index = ~index;
                if (index == span.Length) return -1;
                if (!hasDesiredPrefix(span[index])) return -1;
            }

            while (true)
            {
                var indexNext = index + 1;
                if (indexNext < span.Length && hasDesiredPrefix(span[indexNext]))
                {
                    index++;
                }
                else break;
            }

            return index;
        }


    }
}

