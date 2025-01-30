using AppViewLite.Numerics;
using System;
using System.Collections.Generic;
using System.Linq;

namespace AppViewLite.Storage
{
    public class ImmutableMultiDictionaryWriter<TKey, TValue> : IDisposable where TKey: unmanaged, IComparable<TKey> where TValue: unmanaged, IComparable<TValue>
    {
        private readonly SimpleColumnarWriter writer;
        internal PersistentDictionaryBehavior behavior;
        private bool IsSingleValue => behavior == PersistentDictionaryBehavior.SingleValue;
        public ImmutableMultiDictionaryWriter(string destinationPrefix, PersistentDictionaryBehavior behavior)
        {
            this.behavior = behavior;
            this.writer = new SimpleColumnarWriter(destinationPrefix, IsSingleValue ? 2 : 3);
        }

        private long currentValueOffset;
        public void AddPresorted(TKey key, IEnumerable<TValue> sortedValues)
        {

            var startOffset = currentValueOffset;
            TValue? prev = default;
            foreach (var value in sortedValues)
            {
                if (behavior == PersistentDictionaryBehavior.SortedValues && prev != null && value.CompareTo(prev.Value) < 0) throw new ArgumentException();
                prev = value;

                writer.WriteElement(1, value);

                currentValueOffset++;
            }

            

            if (startOffset != currentValueOffset)
            {
                writer.WriteElement(0, key);
                WriteStartOffset(startOffset);
            }

        }

        public void AddPresorted(TKey key, ReadOnlySpan<TValue> sortedValues)
        {
            var startOffset = currentValueOffset;
            TValue? prev = default;

            foreach (var value in sortedValues)
            {
                if (behavior == PersistentDictionaryBehavior.SortedValues && prev != null && value.CompareTo(prev.Value) < 0) throw new ArgumentException();
                prev = value;


                writer.WriteElement(1, value);
                currentValueOffset++;
            }

            if (startOffset != currentValueOffset)
            {

                writer.WriteElement(0, key);
                WriteStartOffset(startOffset);
            }

        }


        private void WriteStartOffset(long startOffset)
        {
            if (!IsSingleValue)
            {
                if ((ulong)startOffset > UInt48.MaxValueAsUInt64) throw new Exception();
                writer.WriteElement(2, (UInt48)startOffset);
            }
        }


        public void Commit()
        {
            writer.Commit();
        }

        public void Dispose()
        {
            writer.Dispose();
        }
    }
}

