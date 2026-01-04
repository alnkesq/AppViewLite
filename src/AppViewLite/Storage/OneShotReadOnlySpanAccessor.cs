using AppViewLite.Storage;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace AppViewLite
{
    public readonly struct OneShotReadOnlySpanAccessor<T> : IEnumerable<T> where T : unmanaged
    {
        private readonly uint fingerprint;
        private readonly Func<uint, DangerousHugeReadOnlyMemory<T>> getItems;

        public OneShotReadOnlySpanAccessor(uint fingerprint, Func<uint, DangerousHugeReadOnlyMemory<T>> getItems)
        {
            this.fingerprint = fingerprint;
            this.getItems = getItems;
        }
        public IEnumerator<T> GetEnumerator()
        {
            return DangerousSpan.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public ReadOnlySpan<T> AsSmallSpan() => Span.AsSmallSpan();
        public HugeReadOnlySpan<T> Span => DangerousSpan.Span;
        public DangerousHugeReadOnlyMemory<T> DangerousSpan => getItems(fingerprint);
        public long Length => DangerousSpan.Length;

        public T this[long index] => DangerousSpan[index];
    }
}

