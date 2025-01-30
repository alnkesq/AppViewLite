using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Runtime.InteropServices;

namespace AppViewLite
{
    public class MultiDictionary2<TKey, TValue> : IEnumerable<(TKey Key, MultiDictionary2<TKey, TValue>.ValueGroup Values)> where TValue : IComparable<TValue>, IEquatable<TValue>
    {
        private readonly static SortedSet<TValue> EmptyValueSet = new();

        private readonly bool preserveOrder;
        public MultiDictionary2(bool sortedValues)
        {
            preserveOrder = !sortedValues;
        }


        public struct ValueGroup
        {

            public readonly int Count
            {
                get
                {
                    if (_manyValuesSorted != null) return _manyValuesSorted.Count;
                    if (_manyValuesPreserved != null) return _manyValuesPreserved.Length;
                    return 1;
                }
            }

            internal SortedSet<TValue> _manyValuesSorted;
            internal TValue[] _manyValuesPreserved;
            internal TValue _singleValue;

            public readonly IEnumerable<TValue> ValuesSorted
            {
                get
                {
                    if (_manyValuesSorted != null) return _manyValuesSorted;
                    if (_manyValuesPreserved != null) throw new InvalidOperationException();
                    return [_singleValue];
                }
            }
            public readonly IEnumerable<TValue> ValuesUnsorted
            {
                get
                {
                    if (_manyValuesSorted != null) return _manyValuesSorted;
                    if (_manyValuesPreserved != null) return _manyValuesPreserved;
                    return [_singleValue];
                }
            }

            public readonly TValue[] ValuesUnsortedArray
            {
                get
                {
                    if (_manyValuesSorted != null) return _manyValuesSorted.ToArray();
                    if (_manyValuesPreserved != null) return _manyValuesPreserved;
                    return [_singleValue];
                }
            }

            public readonly TValue First()
            {
                if (_manyValuesSorted != null) return _manyValuesSorted.First();
                if (_manyValuesPreserved != null) return _manyValuesPreserved[0];
                return _singleValue;
            }
            
            public readonly ReadOnlySpan<TValue> AsUnsortedSpan()
            {
                if (_manyValuesPreserved != null) return _manyValuesPreserved;
                throw new NotSupportedException();
            }

            public readonly bool Contains(TValue value)
            {
                if (_manyValuesSorted != null) return _manyValuesSorted.Contains(value);
                if (_manyValuesPreserved != null) return _manyValuesPreserved.Contains(value);
                return _singleValue.Equals(value);
            }
        }

        private Dictionary<TKey, ValueGroup> dict = new();

        public int GroupCount => dict.Count;

        public IEnumerable<TKey> Keys => dict.Keys;

        public void RemoveAll(TKey key) => dict.Remove(key);

        public void SetSingleton(TKey key, TValue value)
        {
            if (preserveOrder) throw new NotSupportedException();
            ref var slot = ref CollectionsMarshal.GetValueRefOrAddDefault(dict, key, out var exists);
            slot = new ValueGroup { _singleValue = value };
        }

        public void Add(TKey key, TValue value)
        {
            if (preserveOrder) throw new NotSupportedException();

            ref var slot = ref CollectionsMarshal.GetValueRefOrAddDefault(dict, key, out var exists);
            if (!exists)
            {
                slot = new ValueGroup { _singleValue = value };
            }
            else if (slot._manyValuesSorted != null)
            {
                if (slot._manyValuesSorted == EmptyValueSet) throw new Exception();
                slot._manyValuesSorted.Add(value);
            }
            else 
            {
                var prev = slot._singleValue;
                if (prev.Equals(value)) return;
                slot._manyValuesSorted = [prev, value];
                slot._singleValue = default!;
            }

        }

        public void AddRange(TKey key, ReadOnlySpan<TValue> values)
        {
            if (!preserveOrder) throw new NotSupportedException();
            if (values.IsEmpty) throw new NotSupportedException();
            ref var slot = ref CollectionsMarshal.GetValueRefOrAddDefault(dict, key, out var exists);
            if (!exists)
            {
                slot = new ValueGroup { _manyValuesPreserved = values.ToArray() };
            }
            else throw new InvalidOperationException();
        }

        public bool ContainsKey(TKey key) => dict.ContainsKey(key);

        public bool Contains(TKey key, TValue value)
        {
            if (preserveOrder) throw new NotSupportedException();
            if (TryGetValues(key, out var values))
                return values.Contains(value);
            return false;
        }

        public IEnumerator<(TKey Key, ValueGroup Values)> GetEnumerator()
        {
            return dict.Select(x => (x.Key, x.Value)).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public bool TryGetValues(TKey key, out ValueGroup values)
        {
            return dict.TryGetValue(key, out values);
        }

        public ValueGroup TryGetValues(TKey key)
        {
            if (TryGetValues(key, out var vals)) return vals;
            return new ValueGroup { _manyValuesSorted = EmptyValueSet };
        }

        public void Clear() => dict.Clear();

        public Dictionary<TKey, ValueGroup> Groups => dict;

        public IEnumerable<(TKey Key, TValue Value)> AllEntries => dict.SelectMany(x => x.Value.ValuesUnsorted, (group, list) => (group.Key, list));
    }
}

