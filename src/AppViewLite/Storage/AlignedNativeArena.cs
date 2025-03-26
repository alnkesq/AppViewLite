using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace AppViewLite.Storage
{

    public unsafe class AlignedNativeArena : IDisposable
    {
        public readonly nuint Alignment;
        public AlignedNativeArena(int alignment, nuint initialSize)
        {
            if (!BitOperations.IsPow2(alignment)) throw new ArgumentException();
            this.Alignment = (nuint)alignment;
            EnsureAligned(initialSize);
            lastChunkSize = initialSize / 2; // will be doubled on first alloc = Math.Max(1, initialBlockCount);

        }

        private List<(nuint Start, nuint Length)> chunks = new();
        private byte* nextAllocation;
        private byte* nextAllocationThreshold;

        private nuint lastChunkSize;
        private int consumedChunks;
        public byte* NextAllocation => nextAllocation;

        public bool IsDisposed => chunks != null;

        [ThreadStatic] public static AlignedNativeArena? ForCurrentThread;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte* Allocate(nuint size)
        {
            EnsureAligned(size);
            var updatedNextAllocation = nextAllocation + size;
            if (updatedNextAllocation >= nextAllocationThreshold)
            {
                return GrowAndAllocate(size);
            }
            var ptr = nextAllocation;
            nextAllocation = updatedNextAllocation;
            return ptr;
        }

        private void EnsureAligned(nuint size)
        {
            if ((size % Alignment) != 0)
                throw new ArgumentException();
        }

        public Span<byte> GetRemaingSpaceInCurrentChunk() => new Span<byte>(nextAllocation, (checked((int)(nextAllocationThreshold - nextAllocation))));
        public void AdvanceBy(nuint bytes)
        {
            EnsureAligned(bytes);
            nextAllocation += bytes;
        }

        public byte* Grow(nuint size)
        {
            EnsureAligned(size);
            while (consumedChunks < chunks.Count)
            {
                var chunk = chunks[consumedChunks];
                consumedChunks++;
                if (size <= chunk.Length)
                {
                    nextAllocation = (byte*)chunk.Start;
                    nextAllocationThreshold = nextAllocation + chunk.Length;
                    return nextAllocation;
                }
            }


            var chunkSize = Math.Max(BitOperations.RoundUpToPowerOf2(size), lastChunkSize);
            if (chunkSize * 2 < int.MaxValue)
                chunkSize *= 2;
            lastChunkSize = chunkSize;
            nextAllocation = (byte*)NativeMemory.AlignedAlloc(chunkSize, Alignment);
            nextAllocationThreshold = nextAllocation + chunkSize;
            chunks.Add(((nuint)nextAllocation, chunkSize));
            consumedChunks++;
            Debug.Assert(consumedChunks == chunks.Count);
            return nextAllocation;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private byte* GrowAndAllocate(nuint size)
        {
            Grow(size);
            return Allocate(size);
        }


        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private void Dispose(bool disposing)
        {
            if (chunks == null) return;
            foreach (var item in chunks)
            {
                NativeMemory.AlignedFree((void*)item.Start);
            }
            chunks = null!;
            nextAllocation = null;
        }

        ~AlignedNativeArena()
        {
            Dispose(false);
        }

        public void Reset()
        {
            consumedChunks = 0;
            nextAllocation = null;
            nextAllocationThreshold = null;
        }


    }

}


