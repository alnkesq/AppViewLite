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

        public long KeyCount { get; private set; }
        public long ValueCount => currentValueOffset;



        public struct AddContext
        {
            internal TKey key;
            internal long startOffset;
            internal TValue? prev;
        }
        public AddContext CreateAddContext(TKey key)
        {
            return new AddContext
            {
                key = key,
                startOffset = currentValueOffset,
            };
        }

        public void AddPresorted(ref AddContext ctx, TValue value)
        {
            if (behavior == PersistentDictionaryBehavior.SortedValues && ctx.prev != null && value.CompareTo(ctx.prev.Value) < 0) throw new ArgumentException();
            ctx.prev = value;

            writer.WriteElement(1, value);

            currentValueOffset++;
        }

        public void FinishGroup(ref AddContext ctx)
        {
            if (ctx.startOffset != currentValueOffset)
            {
                if (behavior == PersistentDictionaryBehavior.SingleValue && (currentValueOffset - ctx.startOffset) != 1) throw new Exception();
                writer.WriteElement(0, ctx.key);
                WriteStartOffset(ctx.startOffset);
                KeyCount++;
            }
        }

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
                if (behavior == PersistentDictionaryBehavior.SingleValue && (currentValueOffset - startOffset) != 1) throw new Exception();
                writer.WriteElement(0, key);
                WriteStartOffset(startOffset);
                KeyCount++;
            }

        }

        public void AddPresorted(TKey key, ReadOnlySpan<TValue> sortedValues)
        {
            var startOffset = currentValueOffset;

            if (behavior == PersistentDictionaryBehavior.PreserveOrder)
            {
                if (sortedValues.IsEmpty) return;
                writer.WriteElementRange(1, sortedValues);
                currentValueOffset += sortedValues.Length;

                writer.WriteElement(0, key);
                WriteStartOffset(startOffset);
                KeyCount++;
                return;
            }


            if (behavior == PersistentDictionaryBehavior.SingleValue && sortedValues.Length > 1) throw new ArgumentException();

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
                KeyCount++;
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


        public long CommitAndGetSize()
        {
            return writer.CommitAndGetSize();
        }

        public void Dispose()
        {
            writer.Dispose();
        }
    }
}

