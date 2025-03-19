using Microsoft.Win32.SafeHandles;
using System;
using System.Collections.Concurrent;
using System.ComponentModel;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace AppViewLite.Storage
{
    public unsafe class MemoryMappedFileSlim : IDisposable
    {
        public static ConcurrentDictionary<MemoryMappedFileSlim, byte> Sections = new();
        public string Path { get; private set; }
        public MemoryMappedFileSlim(string path, bool randomAccess = false)
            : this(path, writable: false, FileShare.Read, randomAccess: randomAccess)
        { 
        
        }

        private const uint GENERIC_READ = 0x80000000;
        private const uint FILE_SHARE_READ = 0x00000001;
        private const uint OPEN_EXISTING = 3;
        private const uint FILE_FLAG_NO_BUFFERING = 0x20000000;

        [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Auto)]
        private static extern SafeFileHandle CreateFile(
            string lpFileName,
            uint dwDesiredAccess,
            FileShare dwShareMode,
            IntPtr lpSecurityAttributes,
            uint dwCreationDisposition,
            uint dwFlagsAndAttributes,
            IntPtr hTemplateFile
        );

        [DllImport("libc.so.6", SetLastError = true)]
        private static extern int open(string pathname, OpenFlags flags);

        [Flags]
        internal enum OpenFlags
        {
            // Access modes (mutually exclusive)
            O_RDONLY = 0x0000,
            O_WRONLY = 0x0001,
            O_RDWR = 0x0002,

            // Flags (combinable)
            O_CLOEXEC = 0x0010,
            O_CREAT = 0x0020,
            O_EXCL = 0x0040,
            O_TRUNC = 0x0080,
            O_SYNC = 0x0100,
            O_NOFOLLOW = 0x0200,

            O_DIRECT = 16384,
        }


        internal enum FileAdvice : int
        {
            POSIX_FADV_NORMAL = 0,    /* no special advice, the default value */
            POSIX_FADV_RANDOM = 1,    /* random I/O access */
            POSIX_FADV_SEQUENTIAL = 2,    /* sequential I/O access */
            POSIX_FADV_WILLNEED = 3,    /* will need specified pages */
            POSIX_FADV_DONTNEED = 4,    /* don't need the specified pages */
            POSIX_FADV_NOREUSE = 5,    /* data will only be accessed once */
        }

        [DllImport("libc.so.6", SetLastError = true)]
        private static extern int posix_fadvise(int fd, long offset, long length, FileAdvice advice);


        public MemoryMappedFileSlim(string path, bool writable, FileShare fileShare = FileShare.None, bool randomAccess = false)
        {
            var access = writable ? MemoryMappedFileAccess.ReadWrite : MemoryMappedFileAccess.Read;

            SafeFileHandle safeFileHandle;
            if (false)
            {

                safeFileHandle = File.OpenHandle(path, FileMode.Open, writable ? FileAccess.ReadWrite : FileAccess.Read, fileShare, randomAccess ? FileOptions.RandomAccess : FileOptions.None);
            }
            else
            {
                if (OperatingSystem.IsWindows())
                {
                    safeFileHandle = CreateFile(path, GENERIC_READ, fileShare, IntPtr.Zero, OPEN_EXISTING, FILE_FLAG_NO_BUFFERING, IntPtr.Zero);
                }
                else
                {
                    var fd = open(path, OpenFlags.O_RDONLY | OpenFlags.O_DIRECT);
                    if (fd < 0)
                    {
                        throw new Win32Exception();
                    }

                    safeFileHandle = new SafeFileHandle(fd, ownsHandle: true);
                    var errno = posix_fadvise(fd, 0, 0, FileAdvice.POSIX_FADV_RANDOM);
                    if (errno != 0)
                    {
                        safeFileHandle.Dispose();
                        throw new Win32Exception(errno);
                    }


                }
                
            }
            var fileStream = new FileStream(safeFileHandle, FileAccess.Read, 4096, false);
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

