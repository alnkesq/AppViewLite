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
            // "read != length" can happen when we read near the end of a file (last block might not be complete)
            // Just make sure we read at least one byte.
            if (read == 0)
            {
                throw new EndOfStreamException();
            } 
            return new NativeMemoryRange((nuint)ptr, (nuint)length);
        }

        [ThreadStatic] private static AlignedNativeArena? AlignedNativeArenaForCurrentThreadCacheReads;

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
                var resultSpan = result.AsSpan();
                var resultSpanRemaining = resultSpan;
                if (blockCount >= 3)
                {
                    var cached = readCache.GetOrAddMultiblock((fileOffset, length, handle), () =>
                    {
                        
                        var cacheReadArena = GetArenaForAlignedCacheReads(blockSize);
                        var alignedBuffer = ReadAligned(handle, alignedFileOffset, alignedLength, cacheReadArena).AsReadOnlySpan();
                        var result = alignedBuffer.Slice((int)offsetWithinBlock, length).ToArray(); 
                        ResetArenaForAlignedCacheReads(cacheReadArena);
                        return result;
                    });
                    cached.CopyTo(resultSpan);
                }
                else
                {
                    for (int blockIndex = 0; blockIndex < blockCount; blockIndex++)
                    {
                        var blockSpan = readCache.GetOrAddSingleBlock((blockStartFileOffset, handle), () =>
                        {
                            var cacheReadArena = GetArenaForAlignedCacheReads(blockSize);
                            var result = ReadAligned(handle, blockStartFileOffset, blockSize, cacheReadArena).AsReadOnlySpan().ToArray();
                            ResetArenaForAlignedCacheReads(cacheReadArena);
                            return result;
                            
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

        private static void ResetArenaForAlignedCacheReads(AlignedNativeArena cacheReadArena)
        {
            cacheReadArena.Reset();
            
            if (cacheReadArena.TotalAllocatedSize > ArenaForCacheReadsInitialBlockCount * (nint)cacheReadArena.Alignment)
            {
                if (cacheReadArena != AlignedNativeArenaForCurrentThreadCacheReads) throw new ArgumentException();
                cacheReadArena.Dispose();
                AlignedNativeArenaForCurrentThreadCacheReads = null;
            }
        }

        private const int ArenaForCacheReadsInitialBlockCount = 256;
        private static unsafe AlignedNativeArena GetArenaForAlignedCacheReads(int blockSize)
        {
            var alignedArenaForCacheReads = AlignedNativeArenaForCurrentThreadCacheReads;
            if (alignedArenaForCacheReads == null)
            {
                alignedArenaForCacheReads = new(blockSize, (nuint)blockSize * ArenaForCacheReadsInitialBlockCount);
                AlignedNativeArenaForCurrentThreadCacheReads = alignedArenaForCacheReads;
            }

            return alignedArenaForCacheReads;
        }
    }

    public record struct NativeMemoryRange(nuint Pointer, nuint Length)
    {
        public unsafe Span<byte> AsSpan() => new Span<byte>((void*)Pointer, checked((int)Length));
        public unsafe ReadOnlySpan<byte> AsReadOnlySpan() => AsSpan();
    }

    public enum IoMethodPreference
    {
        Unspecified = 0,
        Mmap = 1,
        DirectIo = 2,
    }

    public class DirectIoReadCache
    {
        public DirectIoReadCache(Func<(long BlockIndex, SafeFileHandle SourceFile), Func<byte[]>, byte[]> getOrAddSingleBlock, Func<(long FileOffset, int Length, SafeFileHandle SourceFile), Func<byte[]>, byte[]> getOrAddMultiBlock, Func<int, NativeMemoryRange> allocUnaligned, Func<object> getCounters)
        {
            GetOrAddSingleBlock = getOrAddSingleBlock;
            AllocUnaligned = allocUnaligned;
            GetOrAddMultiblock = getOrAddMultiBlock;
            GetCounters = getCounters;
        }
        internal readonly Func<(long BlockIndex, SafeFileHandle SourceFile), Func<byte[]>, byte[]> GetOrAddSingleBlock;
        internal readonly Func<(long FileOffset, int Length, SafeFileHandle SourceFile), Func<byte[]>, byte[]> GetOrAddMultiblock;
        internal readonly Func<int, NativeMemoryRange> AllocUnaligned;

        public readonly Func<object> GetCounters;
    }
}

