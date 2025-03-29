using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Storage
{
    public unsafe class HugeMemory<T> : IDisposable where T : unmanaged
    {
        private T* ptr;
        private readonly long length;
        private readonly bool ownsMemory;
        public HugeSpan<T> Span
        {
            get
            {
                if (ptr == null) throw new ObjectDisposedException(null);
                return new HugeSpan<T>(ref Unsafe.AsRef<T>((void*)ptr), length);
            }
        }
        public HugeMemory(T* ptr, long length)
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

        public HugeReadOnlyMemory<T> AsReadOnly() => new HugeReadOnlyMemory<T>(ptr, length);
    }




    public unsafe class HugeReadOnlyMemory<T> : IDisposable where T : unmanaged
    {
        private T* ptr;
        private readonly long length;
        public HugeReadOnlySpan<T> Span
        {
            get
            {
                if (ptr == null) throw new ObjectDisposedException(null);
                return new HugeReadOnlySpan<T>(in Unsafe.AsRef<T>((void*)ptr), length);
            }
        }
        public HugeReadOnlyMemory(T* ptr, long length)
        {
            this.ptr = ptr;
            this.length = length;
        }

        public long Length => length;

        public void Dispose()
        {
            ptr = null;
        }
        public static explicit operator DangerousHugeReadOnlyMemory<T>(HugeReadOnlyMemory<T> mem) => new(mem.ptr, mem.length);

        public ref readonly T this[long index] => ref Unsafe.AsRef(in ptr[index]);

    }

    [DebuggerDisplay("Length = {Length}")]
    public unsafe struct DangerousHugeReadOnlyMemory<T> : IDisposable, IEnumerable<T> where T : unmanaged
    {
        private T* ptr;
        private readonly long length;
        public T* Pointer => ptr;
        public HugeReadOnlySpan<T> Span
        {
            get
            {
                if (ptr == null)
                {
                    if (length == 0) return default;
                    throw new ObjectDisposedException(null);
                }
                return new HugeReadOnlySpan<T>(in Unsafe.AsRef<T>((void*)ptr), length);
            }
        }

        public static implicit operator HugeReadOnlySpan<T>(DangerousHugeReadOnlyMemory<T> memory) => memory.Span;

        public DangerousHugeReadOnlyMemory(T* ptr, long length)
        {
            this.ptr = ptr;
            this.length = length;
        }

        public long Length => length;

        [EditorBrowsable(EditorBrowsableState.Never)]
        public long Count => length;

        public void Dispose()
        {
            ptr = null;
        }
        public static explicit operator HugeReadOnlyMemory<T>(DangerousHugeReadOnlyMemory<T> mem) => new(mem.ptr, mem.length);


        public DangerousHugeReadOnlyMemory<T> Slice(long start, long length)
        {
            if ((ulong)start > (ulong)this.length) throw new ArgumentOutOfRangeException();
            if ((ulong)(start + length) > (ulong)this.length) throw new ArgumentOutOfRangeException();
            return new DangerousHugeReadOnlyMemory<T>(ptr + (nint)start, length);
        }
        public DangerousHugeReadOnlyMemory<T> Slice(long start)
        {
            if ((ulong)start > (ulong)this.length) throw new ArgumentOutOfRangeException();
            return new DangerousHugeReadOnlyMemory<T>(ptr + (nint)start, this.length - start);
        }

        public Enumerator GetEnumerator() => new Enumerator { ptr = this.ptr - 1, Remaining = this.length };

        IEnumerator<T> IEnumerable<T>.GetEnumerator() => GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        public IEnumerable<T> Reverse()
        {
            for (long i = this.length - 1; i >= 0; i--)
            {
                yield return this[i];
            }
        }

        [EditorBrowsable(EditorBrowsableState.Never)]
        public HugeReadOnlySpan<T> AsSpan() => this.Span;
        
        public ReadOnlySpan<T> AsSmallSpan() => this.Span.AsSmallSpan();

        public ref readonly T this[long index] => ref Unsafe.AsRef(in this.ptr[index]);
        public unsafe struct Enumerator : IEnumerator<T>
        {
            internal T* ptr;
            internal long Remaining;

            public T Current => *ptr;

            object IEnumerator.Current => this.Current;

            public void Dispose()
            {
            }

            public bool MoveNext()
            {
                if (Remaining == 0) return false;
                ptr++;
                Remaining--;
                return true;
            }

            public void Reset()
            {
                throw new NotSupportedException();
            }
        }
    }

    public readonly unsafe ref struct HugeSpan<T>
    {
        public HugeSpan(void* ptr, long length)
            : this(ref Unsafe.AsRef<T>(ptr), length)
        {
        }
        public HugeSpan(ref T ptr, long length)
        {
            this.ptr = ref ptr;
            this.length = length;
        }
        public HugeSpan(Span<T> span)
        {
            this.ptr = ref MemoryMarshal.GetReference(span);
            this.length = span.Length;
        }
        private readonly ref T ptr;
        private readonly long length;
        public ref T this[long index]
        {
            get
            {
                if ((ulong)index >= (ulong)length) throw new IndexOutOfRangeException();
                return ref Unsafe.Add(ref ptr, (nuint)index);
            }
        }
        public long Length => length;
        public bool IsEmpty => Length == 0;
        public HugeSpan<T> Slice(long start)
        {
            if ((ulong)start > (ulong)length) throw new ArgumentOutOfRangeException();
            return new HugeSpan<T>(ref Unsafe.Add(ref ptr, (nint)start), length - start);
        }
        public HugeSpan<T> Slice(long start, long length)
        {
            if ((ulong)start > (ulong)this.length) throw new ArgumentOutOfRangeException();
            if ((ulong)(start + length) > (ulong)this.length) throw new ArgumentOutOfRangeException();
            return new HugeSpan<T>(ref Unsafe.Add(ref ptr, (nint)start), length);
        }

        public Span<T> AsSmallSpan() => MemoryMarshal.CreateSpan(ref ptr, checked((int) length));

        public static implicit operator HugeReadOnlySpan<T>(HugeSpan<T> span) => span.AsReadOnly();
        public HugeReadOnlySpan<T> AsReadOnly() => new HugeReadOnlySpan<T>(in ptr, length);
    }



    public readonly unsafe ref struct HugeReadOnlySpan<T>
    {
        public HugeReadOnlySpan(void* ptr, long length)
            : this(in Unsafe.AsRef<T>(ptr), length)
        {
        }
        public HugeReadOnlySpan(in T ptr, long length)
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
                return ref Unsafe.Add(ref Unsafe.AsRef(in ptr), (nuint)index);
            }
        }
        public long Length => length;

        public HugeReadOnlySpan<T> Slice(long start)
        {
            if ((ulong)start > (ulong)length) throw new ArgumentOutOfRangeException();
            return new HugeReadOnlySpan<T>(in Unsafe.Add(ref Unsafe.AsRef(in ptr), (nint)start), length - start);
        }
        public HugeReadOnlySpan<T> Slice(long start, long length)
        {
            if ((ulong)start > (ulong)this.length) throw new ArgumentOutOfRangeException();
            if ((ulong)(start + length) > (ulong)this.length) throw new ArgumentOutOfRangeException();
            return new HugeReadOnlySpan<T>(in Unsafe.Add(ref Unsafe.AsRef(in ptr), (nint)start), length);
        }

        public ReadOnlySpan<T> AsSmallSpan() => MemoryMarshal.CreateReadOnlySpan(ref Unsafe.AsRef(in ptr), checked((int) length));

        public static implicit operator HugeReadOnlySpan<T>(T[]? array)
        {
            if (array == null || array.Length == 0) return default;
            return new HugeReadOnlySpan<T>(in array[0], array.Length);
        }

        public bool IsEmpty => Length == 0;
    }


}

