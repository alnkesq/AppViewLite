using AppViewLite.Storage;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Storage
{
    [DebuggerDisplay("Count = {Count}")]
    public readonly struct ManagedOrNativeArray<T> : IEnumerable<T> where T : unmanaged
    {
        [DebuggerBrowsable(DebuggerBrowsableState.Never)]
        private readonly DangerousHugeReadOnlyMemory<T> native;
        [DebuggerBrowsable(DebuggerBrowsableState.Never)]
        private readonly T[]? managed;

        public ManagedOrNativeArray(DangerousHugeReadOnlyMemory<T> native)
        {
            this.native = native;
        }
        public ManagedOrNativeArray(T[]? managed)
        {
            this.managed = managed;
        }

        [DebuggerBrowsable(DebuggerBrowsableState.Never)]
        public long Count => managed != null ? managed.LongLength : native.Length;

        [DebuggerBrowsable(DebuggerBrowsableState.Never)]
        public bool IsEmpty => Count == 0;

        public ref readonly T this[long index] => ref managed != null ? ref managed[index] : ref native[index];

        public override string ToString()
        {
            return $"Count = {Count}";
        }

        public ManagedOrNativeArray<T> Slice(long start, long length)
        {
            if (managed != null) return managed.AsSpan(checked((int)start), checked((int)length)).ToArray();
            return native.Slice(start, length);
        }
        public ManagedOrNativeArray<T> Slice(long start)
        {
            if (managed != null) return managed.AsSpan(checked((int)start)).ToArray();
            return native.Slice(start);
        }

        public IEnumerator<T> GetEnumerator()
        {
            if (managed != null) return ((IEnumerable<T>)managed).GetEnumerator();
            return native.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public ReadOnlySpan<T> AsSpan() => this;

        [DebuggerBrowsable(DebuggerBrowsableState.RootHidden)]
        private ReadOnlySpan<T> AsSpanForDebugger => AsSpan();

        public static implicit operator ManagedOrNativeArray<T>(T[]? arr) => new(arr);
        public static implicit operator ManagedOrNativeArray<T>(DangerousHugeReadOnlyMemory<T> arr) => new(arr);

        public static implicit operator ReadOnlySpan<T>(ManagedOrNativeArray<T> arr) => arr.managed != null ? arr.managed : arr.native.Span.AsSmallSpan;
    }
}

