using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace AppViewLite
{
    public class MultiDictionary2<TKey, TValue> : IEnumerable<(TKey Key, MultiDictionary2<TKey, TValue>.ValueGroup Values)> where TValue : IComparable<TValue>, IEquatable<TValue> where TKey: notnull
    {
        private readonly static SortedSet<TValue> EmptyValueSet = new();

        private readonly static int PerKeySize = Unsafe.SizeOf<TKey>() + Unsafe.SizeOf<MultiDictionary2<TKey, TValue>.ValueGroup>();
        private readonly static int PerValueSize = Unsafe.SizeOf<TValue>();

        public long SizeInBytes => _sizeInBytes;
        private long _sizeInBytes;
        private void AddMemoryPressure(long byteCount)
        {
            _sizeInBytes += byteCount;
        }

        private readonly bool preserveOrder;
        public MultiDictionary2(bool sortedValues)
        {
            preserveOrder = !sortedValues;
            CreateVirtualSlice();
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

            public SortedSet<TValue>? _manyValuesSorted;
            internal TValue[]? _manyValuesPreserved; // Contents of this array must NOT change.
            internal TValue _singleValue;

            public ValueGroup Clone()
            {
                return new()
                {
                    _singleValue = _singleValue,
                    _manyValuesPreserved = _manyValuesPreserved,
                    _manyValuesSorted = _manyValuesSorted != null ? new SortedSet<TValue>(_manyValuesSorted) : null
                };
            }

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

            public long ExtraSizeBytes
            {
                get
                {
                    if (_manyValuesSorted != null) return _manyValuesSorted.Count * PerValueSize;
                    if (_manyValuesPreserved != null) return _manyValuesPreserved.Length * PerValueSize;
                    return 0;
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


            [UnscopedRef]
            public bool TryAsUnsortedSpan(out ReadOnlySpan<TValue> span)
            {
                if (_manyValuesPreserved != null)
                {
                    span = _manyValuesPreserved;
                    return true;
                }
                else if (_manyValuesSorted == null)
                {
                    span = new ReadOnlySpan<TValue>(in _singleValue);
                    return true;
                }
                span = default;
                return false;
            }

            public readonly bool Contains(TValue value)
            {
                if (_manyValuesSorted != null) return _manyValuesSorted.Contains(value);
                if (_manyValuesPreserved != null) return _manyValuesPreserved.Contains(value);
                return _singleValue.Equals(value);
            }
        }

        private Dictionary<TKey, ValueGroup> dict = new();
        public List<MultiDictionary2VirtualSlice> virtualSlices = new();
        public MultiDictionary2VirtualSlice? currentVirtualSlice;

        public long FirstVirtualSliceId;
        public long LastVirtualSliceIdExclusive;

        public int GroupCount => dict.Count;
        public long ValueCount => dict.Values.Sum(x => (long)x.Count);

        public IEnumerable<TKey> Keys => dict.Keys;

        public void RemoveAll(TKey key)
        {
            if (dict.Remove(key, out var old))
            {
                AddMemoryPressure(-PerKeySize - old.ExtraSizeBytes);
            }
        }

        public void SetSingleton(TKey key, TValue value)
        {
            if (preserveOrder) throw new NotSupportedException();
            ref var slot = ref CollectionsMarshal.GetValueRefOrAddDefault(dict, key, out var exists);
            if (!exists) 
                AddMemoryPressure(PerKeySize);
            slot = new ValueGroup { _singleValue = value };
            MarkKeyAsDirty(key);
        }

        public void Add(TKey key, TValue value)
        {
            if (preserveOrder) throw new NotSupportedException();

            ref var slot = ref CollectionsMarshal.GetValueRefOrAddDefault(dict, key, out var exists);
            if (!exists)
            {
                slot = new ValueGroup { _singleValue = value };
                AddMemoryPressure(PerKeySize);
            }
            else if (slot._manyValuesSorted != null)
            {
                if (slot._manyValuesSorted == EmptyValueSet) throw new Exception();
                if (slot._manyValuesSorted.Add(value))
                    AddMemoryPressure(PerValueSize);
            }
            else 
            {
                var prev = slot._singleValue;
                if (prev.Equals(value)) return;
                slot._manyValuesSorted = [prev, value];
                slot._singleValue = default!;
                AddMemoryPressure(PerValueSize * 2);
            }

            MarkKeyAsDirty(key);
        }

        public void AddRange(TKey key, ReadOnlySpan<TValue> values)
        {
            if (!preserveOrder) throw new NotSupportedException();
            if (values.IsEmpty) throw new NotSupportedException();
            ref var slot = ref CollectionsMarshal.GetValueRefOrAddDefault(dict, key, out var exists);
            if (!exists)
            {
                slot = new ValueGroup { _manyValuesPreserved = values.ToArray() };
                AddMemoryPressure(PerKeySize);
                AddMemoryPressure(PerValueSize * values.Length);
                MarkKeyAsDirty(key);
            }
            else throw new InvalidOperationException();
        }

        private void MarkKeyAsDirty(TKey key)
        {
            currentVirtualSlice!.DirtyKeys.Add(key);
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

        public void Clear()
        {
            _sizeInBytes = 0;
            dict.Clear();
            virtualSlices.Clear();
            CreateVirtualSlice();
        }

        public void CreateVirtualSlice()
        {
            currentVirtualSlice = new() { Id = ++LastVirtualSliceId };
            virtualSlices.Add(currentVirtualSlice);
        }

        internal long LastVirtualSliceId;

        public Dictionary<TKey, ValueGroup> Groups => dict;

        public IEnumerable<(TKey Key, TValue Value)> AllEntries => dict.SelectMany(x => x.Value.ValuesUnsorted, (group, list) => (group.Key, list));

        public MultiDictionary2<TKey, TValue> CloneAndMaybeCreateNewVirtualSlice()
        {
            var d = new Dictionary<TKey, ValueGroup>(this.dict.Capacity);
            foreach (var item in this.dict)
            {
                var v = item.Value;
                d.Add(item.Key, item.Value.Clone());
            }
            var copy = new MultiDictionary2<TKey, TValue>(sortedValues: !preserveOrder);
            copy.dict = d;
            copy.FirstVirtualSliceId = this.virtualSlices[0].Id;
            copy.LastVirtualSliceIdExclusive = this.virtualSlices[^1].Id + 1;
            if (this.currentVirtualSlice!.DirtyKeys.Count != 0)
            {
                this.CreateVirtualSlice();
            } 
            else
            {
                copy.LastVirtualSliceIdExclusive--;
            }
            return copy;
        }


        public class MultiDictionary2VirtualSlice
        {
            public required long Id;
            public HashSet<TKey> DirtyKeys = new();

            public override string ToString()
            {
                return $"#{Id} ({DirtyKeys.Count} keys)";
            }
        }
    }


}

