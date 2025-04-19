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

        public unsafe static NativeMemoryRange ReadUnaligned(SafeFileHandle handle, long fileOffset, int length, AlignedNativeArena arena)
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
            var alignedSpan = ReadAligned(handle, alignedFileOffset, alignedLength, arena);
            var unalignedSpan = new NativeMemoryRange(alignedSpan.Pointer + (nuint)offsetWithinBlock, (nuint)length);
            return unalignedSpan;
        }

        public record struct NativeMemoryRange(nuint Pointer, nuint Length)
        {
        }
    }

    public enum IoMethodPreference
    {
        Unspecified = 0,
        Mmap = 1,
        DirectIo = 2,
    }
}

