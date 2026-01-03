using AppViewLite;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Storage
{
    public static partial class SimpleJoin
    {
        public static IEnumerable<(TKey Key, List<TSource> Values)> GroupAssumingOrderedInput<TSource, TKey>(this IEnumerable<TSource> sortedInput, Func<TSource, TKey> keySelector, bool skipCheck = false, IEqualityComparer<TKey>? keyEqualityComparer = null)
        {
            return sortedInput.Select(x => (keySelector(x), x)).GroupAssumingOrderedInput(skipCheck, keyEqualityComparer);
        }
        public static GroupAssumingOrderedInputStreamedEnumerator<TKey, TSource> GroupAssumingOrderedInputStreamed<TSource, TKey>(this IEnumerable<TSource> sortedInput, Func<TSource, TKey> keySelector, bool skipCheck = false, IEqualityComparer<TKey>? keyEqualityComparer = null)
        {
            return sortedInput.Select(x => (keySelector(x), x)).GroupAssumingOrderedInputStreamed(skipCheck, keyEqualityComparer);
        }

        public static IEnumerable<T> DistinctByAssumingOrderedInput<TKey, T>(this IEnumerable<T> sortedInput, Func<T, TKey> keyFunc, bool skipCheck = false, IEqualityComparer<TKey>? keyEqualityComparer = null)
        {
            keyEqualityComparer ??= GetEqualityComparer<TKey>();
            if (keyEqualityComparer != GetEqualityComparer<TKey>())
                skipCheck = true;
            if (!skipCheck) sortedInput = sortedInput.AssertOrderedAllowDuplicates(x => keyFunc(x), keyEqualityComparer as IComparer<TKey>);

            TKey prevKey = default!;
            bool hasPrev = false;

            foreach (var item in sortedInput)
            {
                var key = keyFunc(item);
                if (hasPrev && keyEqualityComparer.Equals(key, prevKey))
                    continue;
                prevKey = key;
                hasPrev = true;
                yield return item;
            }
        }


        public static IEnumerable<T> DistinctByAssumingOrderedInputLatest<TKey, T>(this IEnumerable<T> sortedInput, Func<T, TKey> keyFunc, bool skipCheck = false, IEqualityComparer<TKey>? keyEqualityComparer = null)
        {
            keyEqualityComparer ??= GetEqualityComparer<TKey>();
            if (keyEqualityComparer != GetEqualityComparer<TKey>())
                skipCheck = true;
            if (!skipCheck) sortedInput = sortedInput.AssertOrderedAllowDuplicates(x => keyFunc(x), keyEqualityComparer as IComparer<TKey>);

            TKey prevKey = default!;
            T prevValue = default!;
            bool hasPrev = false;

            foreach (var item in sortedInput)
            {
                var key = keyFunc(item);
                if (hasPrev && !keyEqualityComparer.Equals(key, prevKey))
                {
                    yield return prevValue;
                }
                prevKey = key;
                hasPrev = true;
                prevValue = item;
            }
            if (hasPrev)
                yield return prevValue;
        }

        public static IEnumerable<T> DistinctAssumingOrderedInput<T>(this IEnumerable<T> sortedInput, bool skipCheck = false, IEqualityComparer<T>? equalityComparer = null)
        {
            return DistinctByAssumingOrderedInput(sortedInput, x => x, skipCheck: skipCheck, keyEqualityComparer: equalityComparer);
        }
        public static IEnumerable<T> DistinctAssumingOrderedInputLatest<T>(this IEnumerable<T> sortedInput, bool skipCheck = false, IEqualityComparer<T>? equalityComparer = null)
        {
            return DistinctByAssumingOrderedInputLatest(sortedInput, x => x, skipCheck: skipCheck, keyEqualityComparer: equalityComparer);
        }


        public static IEnumerable<(TKey Key, List<TValue> Values)> GroupAssumingOrderedInput<TKey, TValue>(this IEnumerable<(TKey Key, TValue Value)> sortedInput, bool skipCheck = false, IEqualityComparer<TKey>? keyEqualityComparer = null)
        {
            keyEqualityComparer ??= GetEqualityComparer<TKey>();
            if (keyEqualityComparer != GetEqualityComparer<TKey>())
                skipCheck = true;
            if (!skipCheck) sortedInput = sortedInput.AssertOrderedAllowDuplicates(x => x.Key, keyEqualityComparer as IComparer<TKey>);

            bool any = false;
            TKey currentKey = default!;
            var currentList = new List<TValue>();
            foreach (var row in sortedInput)
            {
                if (!any)
                {
                    currentKey = row.Key;
                    currentList.Add(row.Value);
                    any = true;
                }
                else
                {
                    if (!keyEqualityComparer.Equals(currentKey, row.Key))
                    {
                        yield return (currentKey, currentList.ToList());
                        currentList.Clear();
                        currentKey = row.Key;
                    }
                    currentList.Add(row.Value);
                }
            }
            if (any)
            {
                yield return (currentKey, currentList.ToList());
            }
        }

        public class GroupAssumingOrderedInputStreamedEnumerator<TKey, T> : IEnumerable<GroupAssumingOrderedInputStreamedGroupEnumerator<TKey, T>>, IEnumerator<GroupAssumingOrderedInputStreamedGroupEnumerator<TKey, T>>
        {
            internal IEnumerator<(TKey Key, T Value)>? Source;
            internal IEqualityComparer<TKey> KeyEqualityComparer = null!;
            internal TKey CurrentKey = default!;
            internal bool ReadyForNextGroup = true;
            private bool didCallGetEnumerator;

            internal bool _hasPeekedRow;
            internal (TKey Key, T Value) _peekedRow;
            internal bool disposed;
            internal int currentToken;
            GroupAssumingOrderedInputStreamedGroupEnumerator<TKey, T> _current;

            internal void ConsumePending()
            {
                if (!_hasPeekedRow) throw new Exception();
                _hasPeekedRow = false;
            }
            internal bool TryGetPending(out (TKey Key, T Value) result)
            {
                if (!_hasPeekedRow)
                {
                    if (Source != null && Source.MoveNext())
                    {
                        _peekedRow = Source.Current;
                        _hasPeekedRow = true;
                        result = _peekedRow;
                        return true;
                    }
                    else
                    {
                        Source?.Dispose();
                        Source = null!;
                        result = default;
                        return false;
                    }
                }
                result = _peekedRow;
                return true;
            }

            public GroupAssumingOrderedInputStreamedGroupEnumerator<TKey, T> Current
            {
                get
                {
                    if (disposed) throw new InvalidOperationException();
                    return _current;
                }
            }
            public bool MoveNext()
            {
                if (disposed) throw new InvalidOperationException();
                if (!ReadyForNextGroup) throw new NotSupportedException();

                if (TryGetPending(out var row))
                {
                    CurrentKey = row.Key;
                    ReadyForNextGroup = false;
                    currentToken++;
                    _current = new GroupAssumingOrderedInputStreamedGroupEnumerator<TKey, T>
                    {
                        Owner = this,
                        Token = currentToken,
                    };
                    return true;
                }
                else
                {
                    this.disposed = true;
                    return false;
                }

            }

            public GroupAssumingOrderedInputStreamedEnumerator<TKey, T> GetEnumerator()
            {
                if (didCallGetEnumerator) throw new NotSupportedException();
                didCallGetEnumerator = true;
                return this;
            }
            object IEnumerator.Current => this.Current;
            public void Dispose() { }
            IEnumerator<GroupAssumingOrderedInputStreamedGroupEnumerator<TKey, T>> IEnumerable<GroupAssumingOrderedInputStreamedGroupEnumerator<TKey, T>>.GetEnumerator() => GetEnumerator();
            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
            void IEnumerator.Reset() => throw new NotSupportedException();
        }
        public struct GroupAssumingOrderedInputStreamedGroupEnumerator<TKey, T> : IEnumerable<T>, IEnumerator<T>
        {
            private bool didCallGetEnumerator;

            internal int Token;
            internal GroupAssumingOrderedInputStreamedEnumerator<TKey, T> Owner;
            private bool isCompleted;
            private void CheckToken()
            {
                if (Token != Owner.currentToken) throw new NotSupportedException();
            }
            public TKey Key
            {
                get
                {
                    CheckToken();
                    return Owner.CurrentKey;
                }
            }
            public T Current
            {
                get
                {
                    CheckToken();
                    return Owner._peekedRow.Value;
                }
            }
            public bool MoveNext()
            {
                if (isCompleted) return false;

                CheckToken();
                if (Owner.TryGetPending(out var row))
                {
                    if (Owner.KeyEqualityComparer.Equals(row.Key, Owner.CurrentKey))
                    {
                        Owner.ConsumePending();
                        return true;
                    }
                }

                isCompleted = true;
                Owner.ReadyForNextGroup = true;
                return false;

            }

            object IEnumerator.Current => Current!;
            public void Dispose()
            {
                Token = -1;
            }
            public GroupAssumingOrderedInputStreamedGroupEnumerator<TKey, T> GetEnumerator()
            {
                if (didCallGetEnumerator) throw new NotSupportedException();
                didCallGetEnumerator = true;
                return this;
            }
            IEnumerator<T> IEnumerable<T>.GetEnumerator() => GetEnumerator();
            public void Reset() => throw new NotSupportedException();
            IEnumerator IEnumerable.GetEnumerator() => this.GetEnumerator();
        }
        public static GroupAssumingOrderedInputStreamedEnumerator<TKey, TValue> GroupAssumingOrderedInputStreamed<TKey, TValue>(this IEnumerable<(TKey Key, TValue Value)> sortedInput, bool skipCheck = false, IEqualityComparer<TKey>? keyEqualityComparer = null)
        {
            keyEqualityComparer ??= GetEqualityComparer<TKey>();
            if (keyEqualityComparer != GetEqualityComparer<TKey>())
                skipCheck = true;
            if (!skipCheck) sortedInput = sortedInput.AssertOrderedAllowDuplicates(x => x.Key, keyEqualityComparer as IComparer<TKey>);
            return new GroupAssumingOrderedInputStreamedEnumerator<TKey, TValue>
            {
                KeyEqualityComparer = keyEqualityComparer,
                Source = sortedInput.GetEnumerator(),
            };
        }

        public static IEnumerable<TAccumulate> AggregatePresortedEnumerables<T, TAccumulate, TKey>(IReadOnlyList<IEnumerable<T>> sources, Func<T, TKey> getSortKey, Func<TAccumulate> makeSeed, Func<TAccumulate, T, TAccumulate> merge, IComparer<TKey>? comparer = null)
        {
            comparer ??= GetComparer<TKey>();
            var sorted = ConcatPresortedEnumerablesKeepOrdered(sources, getSortKey, comparer: comparer);
            TAccumulate current = default!;
            TKey currentKey = default!;
            var first = true;
            foreach (var item in sorted)
            {
                var key = getSortKey(item);
                if (first || comparer.Compare(currentKey, key) != 0)
                {
                    if (!first) yield return current;
                    current = makeSeed();
                    currentKey = key;
                    first = false;
                }
                current = merge(current, item);
            }
            if (!first)
                yield return current;
        }


        public static IEnumerable<T> AggregatePresortedEnumerables<T, TKey>(IReadOnlyList<IEnumerable<T>> sources, Func<T, TKey> getSortKey, Func<T, T, T> merge, IComparer<TKey>? comparer = null)
        {
            comparer ??= GetComparer<TKey>();
            var sorted = ConcatPresortedEnumerablesKeepOrdered(sources, getSortKey, comparer: comparer);
            T current = default!;
            TKey currentKey = default!;
            var first = true;
            foreach (var item in sorted)
            {
                var key = getSortKey(item);
                if (first || comparer.Compare(currentKey, key) != 0)
                {
                    if (!first) yield return current;
                    current = item;
                    currentKey = key;
                    first = false;
                }
                else
                {
                    current = merge(current, item);
                }
            }
            if (!first)
                yield return current;
        }

        public static IEnumerable<T> ConcatPresortedEnumerablesKeepOrdered<T, TKey>(IReadOnlyList<IEnumerable<T>> sources, Func<T, TKey> getSortKey, IComparer<TKey>? comparer = null)
        {
            return ConcatPresortedEnumerablesKeepOrdered<T, TKey>(sources, (x, _) => getSortKey(x), comparer);
        }

        public static IEnumerable<T> ConcatPresortedEnumerablesKeepOrdered<T, TKey>(IReadOnlyList<IEnumerable<T>> sources, Func<T, int, TKey> getSortKey, IComparer<TKey>? comparer = null)
        {
            return sources.Count switch
            {
                0 => [],
                1 => sources[0],
                _ => ConcatPresortedEnumerablesKeepOrderedCore(sources, getSortKey, comparer)
            };
        }

        private static IEnumerable<T> ConcatPresortedEnumerablesKeepOrderedCore<T, TKey>(IReadOnlyList<IEnumerable<T>> sources, Func<T, int, TKey> getSortKey, IComparer<TKey>? comparer)
        {

            comparer ??= GetComparer<TKey>();

            var enumerators = new PriorityQueue<(IEnumerator<T> Enumerator, int SourceIndex), TKey>(comparer);
            try
            {
                var sourceIndex = 0;
                foreach (var source in sources)
                {
                    var enu = source.GetEnumerator();
                    if (enu.MoveNext())
                        enumerators.Enqueue((enu, sourceIndex), getSortKey(enu.Current, sourceIndex));
                    sourceIndex++;
                }
                if (enumerators.Count == 0) yield break;

                while (enumerators.TryDequeue(out var minEnumeratorTuple, out _))
                {
                    var minEnumerator = minEnumeratorTuple.Enumerator;
                    var minItem = minEnumerator.Current;
                    yield return minItem;
                    if (minEnumerator.MoveNext())
                    {
                        enumerators.Enqueue(minEnumeratorTuple, getSortKey(minEnumerator.Current, minEnumeratorTuple.SourceIndex));
                    }
                    else
                    {
                        minEnumerator.Dispose();
                    }
                }
            }
            finally
            {
                while (enumerators.TryDequeue(out var todispose, out _))
                {
                    todispose.Enumerator.Dispose();
                }
            }
        }
        public static IEnumerable<T> AssertOrderedAllowDuplicates<T, TKey>(this IEnumerable<T> items, Func<T, TKey> getKey, IComparer<TKey>? comparer = null)
        {
            comparer ??= GetComparer<TKey>();
            TKey prevKey = default!;
            var hasFirstKey = false;
            foreach (var item in items)
            {
                var key = getKey(item);
                if (hasFirstKey)
                {
                    var comparison = comparer.Compare(prevKey, key);
                    if (comparison > 0) throw new Exception("Input was not sorted. T=" + typeof(T).FullName);
                }
                prevKey = key;
                hasFirstKey = true;
                yield return item;
            }
        }
        public static IEnumerable<T> AssertOrderedAndUnique<T, TKey>(this IEnumerable<T> items, Func<T, TKey> getKey, IComparer<TKey>? comparer = null)
        {
            comparer ??= GetComparer<TKey>();
            TKey prevKey = default!;
            var hasFirstKey = false;
            foreach (var item in items)
            {
                var key = getKey(item);
                if (hasFirstKey)
                {
                    var comparison = comparer.Compare(prevKey, key);
                    if (comparison < 0) { /*ok*/ }
                    else if (comparison == 0) throw new Exception($"Input contained duplicate key '{key}'. T={typeof(T).FullName}");
                    else throw new Exception("Input was not sorted. T=" + typeof(T).FullName);
                }
                prevKey = key;
                hasFirstKey = true;
                yield return item;
            }
        }

        public static IEqualityComparer<T> GetEqualityComparer<T>() => EqualityComparer<T>.Default;
        public static IComparer<T> GetComparer<T>() => Comparer<T>.Default;
    }
}

