using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Storage
{
    public static partial class SimpleJoin
    {

        public static IEnumerable<(TKey Key, T1? Left, T2? Center, T3? Right)> JoinPresortedAndUnique<T1, T2, T3, TKey>(
            IEnumerable<T1> left, Func<T1, TKey> leftGetKey,
            IEnumerable<T2> center, Func<T2, TKey> centerGetKey,
            IEnumerable<T3> right, Func<T3, TKey> rightGetKey,
            IComparer<TKey>? keyComparer = null,
            bool skipCheck = false)
        {
            var partial = JoinPresortedAndUnique(left, leftGetKey, center, centerGetKey, keyComparer, skipCheck);

            return JoinPresortedAndUnique(partial, x => x.Key, right, rightGetKey, keyComparer, skipCheck)
                .Select(x => (x.Key, x.Left.Left, x.Left.Right, x.Right));
        }

        public static IEnumerable<(T1? Item1, T2? Item2, T3? Item3, T4? Item4, TKey Key)> JoinPresortedAndUnique<T1, T2, T3, T4, TKey>(
            IEnumerable<T1> items1, Func<T1, TKey> item1GetKey,
            IEnumerable<T2> items2, Func<T2, TKey> item2GetKey,
            IEnumerable<T3> items3, Func<T3, TKey> item3GetKey,
            IEnumerable<T4> items4, Func<T4, TKey> item4GetKey,
            IComparer<TKey>? keyComparer = null,
            bool skipCheck = false)
        {
            var partial1 = JoinPresortedAndUnique(items1, item1GetKey, items2, item2GetKey, keyComparer, skipCheck);
            var partial2 = JoinPresortedAndUnique(items3, item3GetKey, items4, item4GetKey, keyComparer, skipCheck);

            return JoinPresortedAndUnique(partial1, x => x.Key, partial2, x => x.Key, keyComparer, skipCheck: true /* Only inputs are from JoinPresortedAndUnique */)
                .Select(x => (x.Left.Left, x.Left.Right, x.Right.Left, x.Right.Right, x.Key));
        }

        public static IEnumerable<(T1? Item1, T2? Item2, T3? Item3, T4? Item4, T5? Item5, TKey Key)> JoinPresortedAndUnique<T1, T2, T3, T4, T5, TKey>(
            IEnumerable<T1> items1, Func<T1, TKey> item1GetKey,
            IEnumerable<T2> items2, Func<T2, TKey> item2GetKey,
            IEnumerable<T3> items3, Func<T3, TKey> item3GetKey,
            IEnumerable<T4> items4, Func<T4, TKey> item4GetKey,
            IEnumerable<T5> items5, Func<T5, TKey> item5GetKey,
            IComparer<TKey>? keyComparer = null,
            bool skipCheck = false)
        {
            var partial1 = JoinPresortedAndUnique(items1, item1GetKey, items2, item2GetKey, keyComparer, skipCheck);
            var partial2 = JoinPresortedAndUnique(items3, item3GetKey, items4, item4GetKey, keyComparer, skipCheck);

            var partial3 = JoinPresortedAndUnique(partial1, x => x.Key, partial2, x => x.Key, keyComparer, skipCheck: true /* Only inputs are from JoinPresortedAndUnique */)
                .Select(x => (x.Left.Left, x.Left.Right, x.Right.Left, x.Right.Right, x.Key));
            return JoinPresortedAndUnique(partial3, x => x.Key, items5, item5GetKey, keyComparer, skipCheck)
                .Select(x => (x.Left.Item1, x.Left.Item2, x.Left.Item3, x.Left.Item4, x.Right, x.Key));
        }


        public static IEnumerable<(T1? Item1, T2? Item2, T3? Item3, T4? Item4, T5? Item5, T6? Item6, TKey Key)> JoinPresortedAndUnique<T1, T2, T3, T4, T5, T6, TKey>(
            IEnumerable<T1> items1, Func<T1, TKey> item1GetKey,
            IEnumerable<T2> items2, Func<T2, TKey> item2GetKey,
            IEnumerable<T3> items3, Func<T3, TKey> item3GetKey,
            IEnumerable<T4> items4, Func<T4, TKey> item4GetKey,
            IEnumerable<T5> items5, Func<T5, TKey> item5GetKey,
            IEnumerable<T6> items6, Func<T6, TKey> item6GetKey,
            IComparer<TKey>? keyComparer = null,
            bool skipCheck = false)
        {
            var partial1 = JoinPresortedAndUnique(items1, item1GetKey, items2, item2GetKey, keyComparer, skipCheck);
            var partial2 = JoinPresortedAndUnique(items3, item3GetKey, items4, item4GetKey, keyComparer, skipCheck);
            var partial3 = JoinPresortedAndUnique(items5, item5GetKey, items6, item6GetKey, keyComparer, skipCheck);

            var result = JoinPresortedAndUnique(partial1, x => x.Key, partial2, x => x.Key, partial3, x => x.Key, keyComparer, skipCheck: true /* Only inputs are from JoinPresortedAndUnique */);
            return result.Select(x => (x.Left.Left, x.Left.Right, x.Center.Left, x.Center.Right, x.Right.Left, x.Right.Right, x.Key));
        }

        public static IEnumerable<(T1? Item1, T2? Item2, T3? Item3, T4? Item4, T5? Item5, T6? Item6, T7? Item7, TKey Key)> JoinPresortedAndUnique<T1, T2, T3, T4, T5, T6, T7, TKey>(
            IEnumerable<T1> items1, Func<T1, TKey> item1GetKey,
            IEnumerable<T2> items2, Func<T2, TKey> item2GetKey,
            IEnumerable<T3> items3, Func<T3, TKey> item3GetKey,
            IEnumerable<T4> items4, Func<T4, TKey> item4GetKey,
            IEnumerable<T5> items5, Func<T5, TKey> item5GetKey,
            IEnumerable<T6> items6, Func<T6, TKey> item6GetKey,
            IEnumerable<T7> items7, Func<T7, TKey> item7GetKey,
            IComparer<TKey>? keyComparer = null,
            bool skipCheck = false)
        {
            var partial1 = JoinPresortedAndUnique(items1, item1GetKey, items2, item2GetKey, keyComparer, skipCheck);
            var partial2 = JoinPresortedAndUnique(items3, item3GetKey, items4, item4GetKey, keyComparer, skipCheck);
            var partial3 = JoinPresortedAndUnique(items5, item5GetKey, items6, item6GetKey, items7, item7GetKey, keyComparer, skipCheck);

            var result = JoinPresortedAndUnique(partial1, x => x.Key, partial2, x => x.Key, partial3, x => x.Key, keyComparer, skipCheck: true /* Only inputs are from JoinPresortedAndUnique */);
            return result.Select(x => (x.Left.Left, x.Left.Right, x.Center.Left, x.Center.Right, x.Right.Left, x.Right.Center, x.Right.Right, x.Key));
        }

        public static IEnumerable<(TKey Key, T1? Left, T2? Right)> JoinPresortedAndUnique<T1, T2, TKey>(IEnumerable<T1> left, Func<T1, TKey> leftGetKey, IEnumerable<T2> right, Func<T2, TKey> rightGetKey, IComparer<TKey>? keyComparer = null, bool skipCheck = false)
        {
            keyComparer ??= GetComparer<TKey>();
            if (!skipCheck)
            {
                left = left.AssertOrderedAndUnique(leftGetKey, keyComparer);
                right = right.AssertOrderedAndUnique(rightGetKey, keyComparer);
            }
            using var enuLeft = left.GetEnumerator();
            using var enuRight = right.GetEnumerator();
            var hasLeftPrefetched = false;
            var hasRightPrefetched = false;
            var isLeftExhausted = false;
            var isRightExhausted = false;
            T1 leftPrefeched = default!;
            T2 rightPrefetched = default!;
            while (true)
            {
                if (!hasLeftPrefetched && !isLeftExhausted)
                {
                    if (enuLeft.MoveNext())
                    {
                        leftPrefeched = enuLeft.Current;
                        hasLeftPrefetched = true;
                    }
                    else
                    {
                        isLeftExhausted = true;
                    }
                }


                if (!hasRightPrefetched && !isRightExhausted)
                {
                    if (enuRight.MoveNext())
                    {
                        rightPrefetched = enuRight.Current;
                        hasRightPrefetched = true;
                    }
                    else
                    {
                        isRightExhausted = true;
                    }
                }

                if (hasLeftPrefetched && hasRightPrefetched)
                {
                    var leftKey = leftGetKey(leftPrefeched);
                    var rightKey = rightGetKey(rightPrefetched);

                    var comparison = keyComparer.Compare(leftKey, rightKey);
                    if (comparison == 0)
                    {
                        yield return (leftKey, leftPrefeched, rightPrefetched);
                        hasLeftPrefetched = false;
                        hasRightPrefetched = false;
                    }
                    else if (comparison < 0)
                    {
                        yield return (leftKey, leftPrefeched, default);
                        hasLeftPrefetched = false;
                    }
                    else
                    {
                        yield return (rightKey, default, rightPrefetched);
                        hasRightPrefetched = false;
                    }
                }
                else if (hasLeftPrefetched && !hasRightPrefetched)
                {
                    yield return (leftGetKey(leftPrefeched), leftPrefeched, default);
                    hasLeftPrefetched = false;
                }
                else if (!hasLeftPrefetched && hasRightPrefetched)
                {
                    yield return (rightGetKey(rightPrefetched), default, rightPrefetched);
                    hasRightPrefetched = false;
                }
                else yield break;

            }
        }

    }
}

