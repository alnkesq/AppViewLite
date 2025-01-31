using AppViewLite.Storage;
using System;
using System.Collections.Generic;
using AppViewLite;
using AppViewLite.Numerics;

namespace AppViewLite.Storage
{
    public class ImmutableMultiDictionaryReader<TKey, TValue> : IDisposable where TKey: unmanaged, IComparable<TKey> where TValue: unmanaged, IComparable<TValue>
    {
        private readonly SimpleColumnarReader columnarReader;
        public string PathPrefix;
        private readonly PersistentDictionaryBehavior behavior;
        private bool IsSingleValue => behavior == PersistentDictionaryBehavior.SingleValue;
        private TKey MinimumKey;
        private TKey MaximumKey;
        public ImmutableMultiDictionaryReader(string pathPrefix, PersistentDictionaryBehavior behavior)
        {
            this.PathPrefix = pathPrefix;
            this.behavior = behavior;
            this.columnarReader = new SimpleColumnarReader(pathPrefix, IsSingleValue ? 2 : 3);
            this.MinimumKey = Keys[0];
            this.MaximumKey = Keys[Keys.Length - 1];
        }



        public HugeReadOnlyMemory<TKey> Keys => columnarReader.GetColumnHugeMemory<TKey>(0);
        public DangerousHugeReadOnlyMemory<TValue> Values => columnarReader.GetColumnDangerousHugeMemory<TValue>(1);
        public HugeReadOnlyMemory<UInt48> Offsets => IsSingleValue ? throw new InvalidOperationException() : columnarReader.GetColumnHugeMemory<UInt48>(2);




        public long Count => Offsets.Length;

        public void Dispose()
        {
            columnarReader.Dispose();
        }

        public long GetIndex(TKey key)
        {
            var z = BinarySearch(key);
            return z < 0 ? -1 : z;
        }
        public bool ContainsKey(TKey key)
        {
            return GetIndex(key) != -1;
        }

        public int GetValueCount(long index)
        {
            if (index == -1) return 0;
            if (IsSingleValue)
                return 1;
            var offsets = this.Offsets;
            var startOffset = offsets[index];
            var endOffset = index == offsets.Length - 1 ? Values.Length : offsets[index + 1];
            return checked((int)(endOffset - startOffset));
        }

        public int GetValueCount(TKey key) => GetValueCount(GetIndex(key));

        public bool Contains(TKey key, TValue value)
        {
            var keyIndex = GetIndex(key);
            if (keyIndex == -1) return false;
            var vals = GetValues(keyIndex);
            var index = vals.Span.BinarySearch(value);
            return index >= 0;
        }

        public DangerousHugeReadOnlyMemory<TValue> GetValues(TKey key) => GetValues(GetIndex(key));
        public DangerousHugeReadOnlyMemory<TValue> GetValues(TKey key, TValue? minExclusive, TValue? maxExclusive) => GetValues(GetIndex(key), minExclusive, maxExclusive);
        public DangerousHugeReadOnlyMemory<TValue> GetValues(long index, TValue? minExclusive, TValue? maxExclusive = null)
        {
            var vals = GetValues(index);
            if (vals.Length == 0) return vals;


            if (minExclusive != null)
            {
                var z = vals.Span.BinarySearch(minExclusive.Value);
                if (z >= 0)
                {
                    vals = vals.Slice(z + 1);
                }
                else
                {
                    z = ~z;
                    vals = vals.Slice(z);
                }
                if (vals.Length == 0) return vals;
            }

            if (maxExclusive != null)
            {
                var z = vals.Span.BinarySearch(maxExclusive.Value);
                if (z >= 0)
                {
                    vals = vals.Slice(0, z);
                }
                else
                {
                    z = ~z;
                    vals = vals.Slice(0, z);
                }
            }


            return vals;
        }
        public DangerousHugeReadOnlyMemory<TValue> GetValues(long index)
        {
            if (index == -1) return default;
            if (IsSingleValue) return Values.Slice(index, 1);
            return Values.Slice(Offsets[index], GetValueCount(index));
        }

        public IEnumerable<(TKey Key, TValue Value)> EnumerateSingleValues()
        {
            if (!IsSingleValue) throw new InvalidOperationException();
            var allValues = this.Values;
            var keys = this.Keys;
            var count = keys.Length;

            for (long i = 0; i < count; i++)
            {
                yield return (keys[i], allValues[i]);
            }
        }

        public IEnumerable<(TKey Key, DangerousHugeReadOnlyMemory<TValue> Values)> Enumerate()
        {
            var allValues = this.Values;
            var keys = this.Keys;
            var count = keys.Length;

            if (IsSingleValue)
            {
                for (long i = 0; i < count; i++)
                {
                    yield return (keys[i], allValues.Slice(i, 1));
                }
            }
            else
            {

                var offsets = this.Offsets;
                for (long i = 0; i < count; i++)
                {
                    var values = allValues.Slice(offsets[i], GetValueCount(i));
                    yield return (keys[i], values);
                }
            }
        }

        public long BinarySearch<TComparable>(TComparable comparable) where TComparable : IComparable<TKey>, allows ref struct
        {
            if (comparable.CompareTo(MinimumKey) < 0) return ~0;
            if (comparable.CompareTo(MaximumKey) > 0) return ~this.Keys.Length;
            return HugeSpanHelpers.BinarySearch(this.Keys.Span, comparable);
        }
    }
}

