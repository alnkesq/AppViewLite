using Microsoft.Win32.SafeHandles;
using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
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
        public static ConcurrentDictionary<MemoryMappedFileSlim, byte> Sections = new();
        private string Path;
        public MemoryMappedFileSlim(string path, bool randomAccess = false)
            : this(path, writable: false, FileShare.Read, randomAccess: randomAccess)
        { 
        
        }
        public MemoryMappedFileSlim(string path, bool writable, FileShare fileShare = FileShare.None, bool randomAccess = false)
        {
            var access = writable ? MemoryMappedFileAccess.ReadWrite : MemoryMappedFileAccess.Read;

            using var fileStream = new FileStream(path, FileMode.Open, writable ? FileAccess.ReadWrite : FileAccess.Read, fileShare, 4096, randomAccess ? FileOptions.RandomAccess : FileOptions.None);
            _length = fileStream.Length;
            mmap = MemoryMappedFile.CreateFromFile(fileStream, null, 0, access, HandleInheritability.None, false);
            var stream = mmap.CreateViewStream(0, 0, access);
            this.handle = stream.SafeMemoryMappedViewHandle;
            handle.AcquirePointer(ref ptr);
            Path = path;
            Sections[this] = 0;
        }



        public unsafe static Func<nuint, string?> GetPageToSectionFunc()
        {
            var sorted = Sections.Keys
                .Select(x => (x.Path, Start: (nuint)x.Pointer, x.Length))
                .Where(x => x.Length != 0)
                .OrderBy(x => x.Start)
                .ToArray();
            var lookup = sorted.Select(x => x.Start).ToArray();

            return addr =>
            {
                var index = lookup.AsSpan().BinarySearch(addr);
                if (index < 0)
                {
                    index = ~index;
                    if (index == 0) return null;
                    index--;
                    var section = sorted[index];
                    if (addr >= section.Start)
                    {
                        if (addr < section.Start + (nuint)section.Length)
                            return section.Path;
                    }
                    return null;
                }
                return sorted[index].Path;
            };

        }

        public MemoryMappedFileSlim(FileStream fileStream)
            : this(fileStream.Name)
        {
        }

        internal readonly byte* ptr;
        internal SafeMemoryMappedViewHandle handle;
        private MemoryMappedFile mmap;
        private int disposed;

        [Obsolete]
        public MemoryMappedMemory Memory => new MemoryMappedMemory(this, ptr, Length);
        public byte* Pointer => ptr;
        private long _length;
        public long Length => _length;
        public void Dispose()
        {
            if (Interlocked.Increment(ref disposed) == 1) 
            {
                Sections.TryRemove(this, out _);
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
    [Obsolete]
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

