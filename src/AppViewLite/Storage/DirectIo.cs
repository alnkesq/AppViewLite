using Microsoft.Win32.SafeHandles;
using System;
using System.IO;

namespace AppViewLite.Storage
{
    public static class DirectIo
    {

        public unsafe static NativeMemoryRange ReadAligned(SafeFileHandle handle, long fileOffset, int length, AlignedNativeArena arena)
        {
            var ptr = arena.Allocate((nuint)length);
            var span = new Span<byte>(ptr, length);
            var read = RandomAccess.Read(handle, span, fileOffset);
            if (read != length)
                throw new EndOfStreamException();
            return new NativeMemoryRange((nuint)ptr, (nuint)length);
        }

        public unsafe static NativeMemoryRange ReadUnaligned(SafeFileHandle handle, long fileOffset, int length, AlignedNativeArena arena, DirectIoReadCache? readCache = null)
        {
            var blockSize = (int)arena.Alignment;
            var offsetWithinBlock = fileOffset % blockSize;
            var alignedFileOffset = fileOffset - offsetWithinBlock;
            var end = fileOffset + length;


            var offsetWithinEndBlock = end % blockSize;
            long alignedFileOffsetEnd;
            if (offsetWithinEndBlock == 0)
                alignedFileOffsetEnd = end;
            else
                alignedFileOffsetEnd = end - offsetWithinEndBlock + blockSize;

            var alignedLength = (int)(alignedFileOffsetEnd - alignedFileOffset);

            if (readCache != null)
            {

                var blockCount = alignedLength / blockSize;
                var blockStartFileOffset = alignedFileOffset;
                var result = readCache.AllocUnaligned(length);
                var resultSpan = new Span<byte>((void*)result.Pointer, length);
                var resultSpanRemaining = resultSpan;
                if (blockCount != 1) { }
                for (int blockIndex = 0; blockIndex < blockCount; blockIndex++)
                {
                    var blockSpan = readCache.GetOrAdd(blockStartFileOffset, () => 
                    {
                        var block = new byte[blockSize];
                        if (RandomAccess.Read(handle, block, blockStartFileOffset) != blockSize)
                        {
                            throw new EndOfStreamException();
                        }
                        return block;
                    }).AsSpan();

                    if (blockIndex == 0)
                    {
                        blockSpan = blockSpan.Slice((int)offsetWithinBlock);
                    }

                    if (resultSpanRemaining.Length < blockSpan.Length)
                    {
                        blockSpan = blockSpan.Slice(0, resultSpanRemaining.Length);
                    }



                    blockSpan.CopyTo(resultSpanRemaining);
                    resultSpanRemaining = resultSpanRemaining.Slice(blockSpan.Length);


                    blockStartFileOffset += blockSize;
                }

#if false
                var alignedSpan = ReadAligned(handle, alignedFileOffset, alignedLength, arena);
                var unalignedSpan = new NativeMemoryRange(alignedSpan.Pointer + (nuint)offsetWithinBlock, (nuint)length);
                if (!new Span<byte>((void*)unalignedSpan.Pointer, (int)unalignedSpan.Length).SequenceEqual(resultSpan))
                    Environment.FailFast("Inconsistent result for cached/uncached ReadUnaligned implementation");
#endif
                return result;
            }
            else
            {
                var alignedSpan = ReadAligned(handle, alignedFileOffset, alignedLength, arena);
                var unalignedSpan = new NativeMemoryRange(alignedSpan.Pointer + (nuint)offsetWithinBlock, (nuint)length);
                return unalignedSpan;
            }

        }

    }

    public record struct NativeMemoryRange(nuint Pointer, nuint Length)
    {
    }

    public enum IoMethodPreference
    {
        Unspecified = 0,
        Mmap = 1,
        DirectIo = 2,
    }

    public class DirectIoReadCache
    {
        public DirectIoReadCache(Func<long, Func<byte[]>, byte[]> getOrAdd, Func<int, NativeMemoryRange> allocUnaligned)
        {
            GetOrAdd = getOrAdd;
            AllocUnaligned = allocUnaligned;
        }
        internal readonly Func<long, Func<byte[]>, byte[]> GetOrAdd;
        internal readonly Func<int, NativeMemoryRange> AllocUnaligned;
    }
}

