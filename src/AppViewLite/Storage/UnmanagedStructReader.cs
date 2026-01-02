using System;
using System.Buffers.Binary;
using System.IO;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace AppViewLite.Storage
{
    public class UnmanagedStructReader<T> : IDisposable where T: unmanaged
    {
        private readonly Stream stream;
        //private byte[]? buffer;
        private T[] buffer;
        private int bufferOffsetStructs;
        private long remainingStructs;
        private readonly int bufferSizeStructs;
        private readonly int bufferSizeBytes;
        public long TotalStructCount { get; }
        public long RemainingStructs => remainingStructs;
        public UnmanagedStructReader(Stream stream, int bufferSizeBytes = 32 * 1024)
        {
            var bufferSizeStructs = (int)CombinedPersistentMultiDictionary.DivideWithCeiling(bufferSizeBytes, Unsafe.SizeOf<T>());
            bufferSizeBytes = bufferSizeStructs * Unsafe.SizeOf<T>();
            this.bufferSizeStructs = bufferSizeStructs;
            this.bufferSizeBytes = bufferSizeBytes;
            //buffer = ArrayPool<byte>.Shared.Rent();
            buffer = new T[bufferSizeStructs];
            bufferOffsetStructs = bufferSizeStructs;
            remainingStructs = stream.Length / Unsafe.SizeOf<T>();
            TotalStructCount = remainingStructs;
            this.stream = stream;
        }


        public T Read()
        {
            if (remainingStructs == 0) throw new EndOfStreamException();
            if (bufferOffsetStructs == bufferSizeStructs)
            {
                var bufferBytes = MemoryMarshal.AsBytes(buffer.AsSpan(0, bufferSizeStructs));
                stream.ReadAtLeast(bufferBytes, bufferSizeBytes, throwOnEndOfStream: false);
                bufferOffsetStructs = 0;
            }
            remainingStructs--;
            //return MemoryMarshal.Cast<byte, T>(buffer)[bufferOffsetStructs++];
            return buffer[bufferOffsetStructs++];
        }


        public void Dispose()
        {
            stream.Dispose();
            //var b = Interlocked.Exchange(ref buffer, null);
            //if (b != null)
            //    ArrayPool<byte>.Shared.Return(b);
        }
    }
}

