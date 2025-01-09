using Microsoft.Win32.SafeHandles;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace AppViewLite.Storage
{
    public unsafe class MemoryMappedFileSlim : IDisposable
    {
        public MemoryMappedFileSlim(string path)
        {
            using var fileStream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read);
            _length = fileStream.Length;
            mmap = MemoryMappedFile.CreateFromFile(fileStream, null, 0, MemoryMappedFileAccess.Read, HandleInheritability.None, false);
            var stream = mmap.CreateViewStream(0, 0, MemoryMappedFileAccess.Read);
            this.handle = stream.SafeMemoryMappedViewHandle;
            handle.AcquirePointer(ref ptr);
        }



        public MemoryMappedFileSlim(FileStream fileStream)
            : this(fileStream.Name)
        {
        }

        internal readonly byte* ptr;
        internal SafeMemoryMappedViewHandle handle;
        private MemoryMappedFile mmap;
        private int disposed;

        public MemoryMappedMemory Memory => new MemoryMappedMemory(this, ptr, Length);
        public byte* Pointer => ptr;
        private long _length;
        public long Length => _length;
        public void Dispose()
        {
            if (Interlocked.Increment(ref disposed) == 1) 
            {
                handle.ReleasePointer();
                handle.Dispose();
                mmap.Dispose();
            }
        }

        public void EnsureValid()
        {
            if (handle.IsClosed) ThrowDisposed();
        }

        private static void ThrowDisposed()
        {
            throw new ObjectDisposedException(nameof(SafeHandle));
        }
    }

    public unsafe struct MemoryMappedMemory
    {
        public MemoryMappedMemory(MemoryMappedFileSlim handle, byte* basePtr, long length)
        {
            this.handle = handle;
            this.basePtr = basePtr;
            this.length = length;
        }
        internal MemoryMappedFileSlim handle;
        internal byte* basePtr;
        internal long length;
        public readonly long Length => length;

        public void EnsureValid()
        {
            handle.EnsureValid();
        }

        public readonly MemoryMappedMemory Slice(long offset)
        {
            return Slice(offset, length - offset);
        }
        public readonly MemoryMappedMemory Slice(long offset, long length)
        {
            var finalOffset = this.basePtr + offset;
            if (length < 0 || offset < 0 || offset > this.length) throw new IndexOutOfRangeException();
            return new MemoryMappedMemory(handle, finalOffset, length);
        }
        public byte ReadByte() => Read<byte>();
        public uint ReadUInt32() => Read<uint>();
        public int ReadInt32() => Read<int>();
        public ulong ReadUInt64() => Read<ulong>();
        public long ReadInt64() => Read<long>();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T Read<T>() where T : unmanaged
        {
            if (length < sizeof(T)) ThrowEndOfStream();
            var val = Unsafe.ReadUnaligned<T>(basePtr);
            basePtr += sizeof(T);
            length -= sizeof(T);
            return val;
        }

        private static void ThrowEndOfStream()
        {
            throw new System.IO.EndOfStreamException();
        }
        
    }

}

