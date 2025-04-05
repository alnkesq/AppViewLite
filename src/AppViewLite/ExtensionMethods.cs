using AngleSharp.Dom;
using AppViewLite.Models;
using AppViewLite.Storage;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

namespace AppViewLite
{
    public static class ExtensionMethods
    {
        public static void FireAndForget(this Task task)
        {
            task.ContinueWith(t =>
            {
                if (t.IsFaulted)
                {
                    if (t.Exception.InnerException is OperationCanceledException)
                    {
                        return;
                    }
                    LoggableBase.LogNonCriticalException(t.Exception);
                }
            }, TaskContinuationOptions.OnlyOnFaulted);
        }
        public static void FireAndForgetLowImportance(this Task task)
        {
            task.ContinueWith(t =>
            {
                if (t.IsFaulted)
                {
                    if (t.Exception.InnerException is OperationCanceledException)
                    {
                        return;
                    }
                    LoggableBase.LogLowImportanceException(t.Exception);
                }
            }, TaskContinuationOptions.OnlyOnFaulted);
        }

        public static Queue<T> ToQueue<T>(this IEnumerable<T> items) => new(items);
        public static QueueWithOwner<T> ToQueueWithOwner<T>(this IEnumerable<T> items, Plc owner)
        {
            var q = new QueueWithOwner<T>()
            {
                Owner = owner
            };
            foreach (var item in items)
            {
                q.Enqueue(item);
            }
            return q;
        }
        public static void EnqueueRange<T>(this Queue<T> queue, IEnumerable<T> items)
        {
            foreach (var item in items)
            {
                queue.Enqueue(item);
            }
        }

        public static T ReadUnmanaged<T>(this BinaryReader reader) where T : unmanaged
        {
            var bytes = reader.ReadBytes(Unsafe.SizeOf<T>());
            return MemoryMarshal.Cast<byte, T>(bytes)[0];
        }
        public static void WriteUnmanaged<T>(this BinaryWriter writer, T item) where T : unmanaged
        {
            writer.Write(MemoryMarshal.AsBytes([item]));
        }

        internal static void MakeUtc(ref DateTime date)
        {
            date = new DateTime(date.Ticks, DateTimeKind.Utc);
        }
        internal static void MakeUtc(ref DateTime? date)
        {
            if (date != null)
                date = new DateTime(date.Value.Ticks, DateTimeKind.Utc);
        }

        public static INode? PreviousNonWhiteSpaceSibling(this INode node)
        {
            var sibling = node.PreviousSibling;
            while (sibling != null && sibling is IText { TextContent: { } t } && string.IsNullOrWhiteSpace(t))
            {
                sibling = sibling.PreviousSibling;
            }
            return sibling;
        }

        public static IEnumerable<T> WhereNonNull<T>(this IEnumerable<T?> items) where T : class
        {
            return items.Where(x => x != null)!;
        }

        private static long OffsetBinarySearchResult(long offset, long index)
        {
            if (index < 0) return ~(offset + ~index);
            return offset + index;
        }

        public static long BinarySearchRightBiased<T>(this HugeReadOnlySpan<T> span, T needle) where T : IComparable<T>
        {
#if true
            return span.BinarySearchRightBiasedCore(needle);
#else
            var a = span.BinarySearchRightBiasedCore(needle);
            var b = span.BinarySearch(needle);
            if (a != b)
            {
                if (a >= 0 && b >= 0 && span[a].Equals(span[b]))
                {
                    // actually ok (duplicates)
                }
                else
                {
                    BlueskyRelationships.ThrowFatalError("BinarySearchRightBiased mismatch");
                }
            }
            return a;
#endif
        }
        public static long BinarySearchRightBiasedCore<T>(this HugeReadOnlySpan<T> span, T needle) where T : IComparable<T>
        {
            long increment = 4;
            while (true)
            {
                var index = span.Length - increment;

                if (index < 0) return span.BinarySearch(needle);

                var val = span[index];
                var cmp = needle.CompareTo(val);
                if (cmp > 0)
                {
                    var offset = index + 1;
                    var result = span.Slice(offset).BinarySearch(needle);
                    return OffsetBinarySearchResult(offset, result);
                }
                else if (cmp < 0)
                {
                    span = span.Slice(0, index);
                }
                else
                {
                    return index;
                }


                increment <<= 2;
            }

        }

        public static IEnumerable<T> EnumerateFrom<T>(this DangerousHugeReadOnlyMemory<T> source, T inclusiveStartingPoint) where T : unmanaged, IComparable<T>
        {
            var index = HugeSpanHelpers.BinarySearch<T, T>(source, inclusiveStartingPoint);
            if (index < 0)
            {
                index = ~index;
            }
            return source.Slice(index);
        }

        public static IEnumerable<T> EnumerateFromReverse<T>(this DangerousHugeReadOnlyMemory<T> source, T startingPointInclusive) where T : unmanaged, IComparable<T>
        {
            var index = HugeSpanHelpers.BinarySearch<T, T>(source, startingPointInclusive);
            if (index < 0)
            {
                index = ~index;
            }
            else
            {
                index++;
            }
            return source.Slice(0, index).Reverse();
        }



        public static IEnumerable<T> EnumerateFromRightBiased<T>(this DangerousHugeReadOnlyMemory<T> source, T inclusiveStartingPoint) where T : unmanaged, IComparable<T>
        {
            var index = source.AsSpan().BinarySearchRightBiased(inclusiveStartingPoint);
            if (index < 0)
            {
                index = ~index;
            }
            return source.Slice(index);
        }

        public static IEnumerable<T> EnumerateFromReverseRightBiased<T>(this DangerousHugeReadOnlyMemory<T> source, T inclusiveStartingPoint) where T : unmanaged, IComparable<T>
        {
            var index = source.AsSpan().BinarySearchRightBiased(inclusiveStartingPoint);
            if (index < 0)
            {
                index = ~index;
            }
            else
            {
                index++;
            }
            return source.Slice(0, index).Reverse();
        }

        public static bool AnyInnerException(this Exception ex, Func<Exception, bool> condition)
        {
            while (ex != null)
            {
                if (condition(ex))
                    return true;
                ex = ex.InnerException!;
            }
            return false;
        }

        public static Dictionary<TKey, TValue> ToDictionaryIgnoreDuplicates<T, TKey, TValue>(this IEnumerable<T> items, Func<T, TKey> getKey, Func<T, TValue> getValue) where TKey : notnull
        {
            var dict = new Dictionary<TKey, TValue>();
            foreach (var item in items)
            {
                dict[getKey(item)] = getValue(item);
            }
            return dict;
        }

        public static bool ShouldPreservePost(this PruningContext ctx, PostId postId) => ((AppViewLitePruningContext)ctx).ShouldPreservePost(postId);
        public static bool ShouldPreserveUser(this PruningContext ctx, Plc user) => ((AppViewLitePruningContext)ctx).ShouldPreserveUser(user);

        public static IEnumerable<T> TrySelect<TSource, T>(this IEnumerable<TSource> items, Func<TSource, T> selector)
        {
            foreach (var item in items)
            {
                T result;
                try
                {
                    result = selector(item);
                }
                catch (Exception ex)
                {
                    LoggableBase.LogLowImportanceException(ex);
                    continue;
                }

                yield return result;
            }
        }
    }


}

