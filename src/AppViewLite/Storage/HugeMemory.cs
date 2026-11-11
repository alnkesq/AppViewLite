using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace AppViewLite.Storage
{
    public sealed class HugeMemory<T> : IDisposable where T : unmanaged
    {
        private T* ptr;
        private readonly long length;
        private readonly bool ownsMemory;
        public HugeSpan<T> Span
        {
            get
            {
                if (ptr == null) throw new ObjectDisposedException(null);
                return unsafe(new HugeSpan<T>(ref Unsafe.AsRef<T>((void*)ptr), length));
            }
        }
        public unsafe HugeMemory(T* ptr, long length)
        {
            this.ptr = ptr;
            this.length = length;
            this.ownsMemory = false;
        }
        public HugeMemory(long length)
        {
            this.ptr = (T*)NativeMemory.AllocZeroed((nuint)length, (nuint)Marshal.SizeOf<T>());
            this.length = length;
            this.ownsMemory = true;
        }
        public long Length => length;

        public void Dispose()
        {
            if (ptr == null) return;
            if (ownsMemory) NativeMemory.Free(ptr);
            ptr = null;
        }

        public HugeReadOnlyMemory<T> AsReadOnly() => unsafe(new HugeReadOnlyMemory<T>(ptr, length));
    }




    public sealed class HugeReadOnlyMemory<T> : IDisposable where T : unmanaged
    {
        public T* Pointer => ptr;
        private T* ptr;
        private readonly long length;
        public HugeReadOnlySpan<T> Span
        {
            get
            {
                if (ptr == null) throw new ObjectDisposedException(null);
                return unsafe(new HugeReadOnlySpan<T>(in Unsafe.AsRef<T>((void*)ptr), length));
            }
        }
        public unsafe HugeReadOnlyMemory(T* ptr, long length)
        {
            this.ptr = ptr;
            this.length = length;
        }

        public long Length => length;

        public void Dispose()
        {
            ptr = null;
        }
        public static explicit operator DangerousHugeReadOnlyMemory<T>(HugeReadOnlyMemory<T> mem) => unsafe(new(mem.ptr, mem.length));

        public unsafe ref readonly T this[long index] => ref unsafe(Unsafe.AsRef(in ptr[index]));

    }

    [DebuggerDisplay("Length = {Length}")]
    public struct DangerousHugeReadOnlyMemory<T> : IDisposable, IEnumerable<T> where T : unmanaged
    {
        private T* ptr;
        private readonly long length;
        public readonly T* Pointer => ptr;
        public readonly HugeReadOnlySpan<T> Span
        {
            get
            {
                if (ptr == null)
                {
                    if (length == 0) return default;
                    throw new ObjectDisposedException(null);
                }
                return unsafe(new HugeReadOnlySpan<T>(in Unsafe.AsRef<T>((void*)ptr), length));
            }
        }

        public static implicit operator HugeReadOnlySpan<T>(DangerousHugeReadOnlyMemory<T> memory) => memory.Span;

        public unsafe DangerousHugeReadOnlyMemory(T* ptr, long length)
        {
            this.ptr = ptr;
            this.length = length;
        }

        public readonly long Length => length;

        [EditorBrowsable(EditorBrowsableState.Never)]
        public readonly long Count => length;

        public void Dispose()
        {
            ptr = null;
        }
        public static explicit operator HugeReadOnlyMemory<T>(DangerousHugeReadOnlyMemory<T> mem) => unsafe(new(mem.ptr, mem.length));


        public readonly DangerousHugeReadOnlyMemory<T> Slice(long start, long length)
        {
            if ((ulong)start > (ulong)this.length) throw new ArgumentOutOfRangeException();
            if ((ulong)(start + length) > (ulong)this.length) throw new ArgumentOutOfRangeException();
            return unsafe(new DangerousHugeReadOnlyMemory<T>(ptr + (nint)start, length));
        }
        public readonly DangerousHugeReadOnlyMemory<T> Slice(long start)
        {
            if ((ulong)start > (ulong)this.length) throw new ArgumentOutOfRangeException();
            return unsafe(new DangerousHugeReadOnlyMemory<T>(ptr + (nint)start, this.length - start));
        }

        public readonly Enumerator GetEnumerator() => new Enumerator { ptr = this.ptr - 1, Remaining = this.length };

        readonly IEnumerator<T> IEnumerable<T>.GetEnumerator() => GetEnumerator();

        readonly IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        public safe readonly IEnumerable<T> Reverse()
        {
            for (long i = this.length - 1; i >= 0; i--)
            {
                T val;
                unsafe { val = this[i]; }
                yield return val;
            }
        }

        [EditorBrowsable(EditorBrowsableState.Never)]
        public readonly HugeReadOnlySpan<T> AsSpan() => this.Span;

        public readonly ReadOnlySpan<T> AsSmallSpan() => this.Span.AsSmallSpan();

        public unsafe readonly ref readonly T this[long index] => ref unsafe (Unsafe.AsRef(in this.ptr[index]));

        public struct Enumerator : IEnumerator<T>
        {
            internal T* ptr;
            internal long Remaining;

            public readonly T Current => unsafe(*ptr);

            readonly object IEnumerator.Current => this.Current;

            public readonly void Dispose()
            {
            }

            public bool MoveNext()
            {
                if (Remaining == 0) return false;
                ptr++;
                Remaining--;
                return true;
            }

            public readonly void Reset()
            {
                throw new NotSupportedException();
            }
        }
    }

    public readonly ref struct HugeSpan<T>
    {
        public unsafe HugeSpan(void* ptr, long length)
            : this(ref Unsafe.AsRef<T>(ptr), length)
        {
        }
        public unsafe HugeSpan(ref T ptr, long length)
        {
            this.ptr = ref ptr;
            this.length = length;
        }
        public HugeSpan(Span<T> span)
        {
            this.ptr = ref unsafe(MemoryMarshal.GetReference(span));
            this.length = span.Length;
        }
        private readonly ref T ptr;
        private readonly long length;
        public ref T this[long index]
        {
            get
            {
                if ((ulong)index >= (ulong)length) throw new IndexOutOfRangeException();
                return ref unsafe(Unsafe.Add(ref ptr, (nuint)index));
            }
        }
        public long Length => length;
        public bool IsEmpty => Length == 0;
        public HugeSpan<T> Slice(long start)
        {
            if ((ulong)start > (ulong)length) throw new ArgumentOutOfRangeException();
            return unsafe(new HugeSpan<T>(ref Unsafe.Add(ref ptr, (nint)start), length - start));
        }
        public HugeSpan<T> Slice(long start, long length)
        {
            if ((ulong)start > (ulong)this.length) throw new ArgumentOutOfRangeException();
            if ((ulong)(start + length) > (ulong)this.length) throw new ArgumentOutOfRangeException();
            return unsafe(new HugeSpan<T>(ref Unsafe.Add(ref ptr, (nint)start), length));
        }

        public Span<T> AsSmallSpan() => unsafe(MemoryMarshal.CreateSpan(ref ptr, checked((int)length)));

        public static implicit operator HugeReadOnlySpan<T>(HugeSpan<T> span) => span.AsReadOnly();
        public HugeReadOnlySpan<T> AsReadOnly() => unsafe(new HugeReadOnlySpan<T>(in ptr, length));
    }



    public readonly ref struct HugeReadOnlySpan<T>
    {
        public unsafe HugeReadOnlySpan(void* ptr, long length)
            : this(in Unsafe.AsRef<T>(ptr), length)
        {
        }
        public unsafe HugeReadOnlySpan(in T ptr, long length)
        {
            this.ptr = ref ptr;
            this.length = length;
        }
        private readonly ref readonly T ptr;
        private readonly long length;

        public ref readonly T GetReference() => ref ptr;

        public ref readonly T this[long index]
        {
            get
            {
                if ((ulong)index >= (ulong)length) throw new IndexOutOfRangeException();
                return ref unsafe(Unsafe.Add(ref Unsafe.AsRef(in ptr), (nuint)index));
            }
        }
        public long Length => length;

        public HugeReadOnlySpan<T> Slice(long start)
        {
            if ((ulong)start > (ulong)length) throw new ArgumentOutOfRangeException();
            return unsafe(new HugeReadOnlySpan<T>(in Unsafe.Add(ref Unsafe.AsRef(in ptr), (nint)start), length - start));
        }
        public HugeReadOnlySpan<T> Slice(long start, long length)
        {
            if ((ulong)start > (ulong)this.length) throw new ArgumentOutOfRangeException();
            if ((ulong)(start + length) > (ulong)this.length) throw new ArgumentOutOfRangeException();
            return unsafe(new HugeReadOnlySpan<T>(in Unsafe.Add(ref Unsafe.AsRef(in ptr), (nint)start), length));
        }

        public ReadOnlySpan<T> AsSmallSpan() => unsafe(MemoryMarshal.CreateReadOnlySpan(ref Unsafe.AsRef(in ptr), checked((int)length)));

        public static implicit operator HugeReadOnlySpan<T>(T[]? array)
        {
            if (array == null || array.Length == 0) return default;
            return unsafe(new HugeReadOnlySpan<T>(in array[0], array.Length));
        }

        public bool IsEmpty => Length == 0;
    }


}

