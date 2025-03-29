using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace DuckDbSharp.Bindings
{

    public unsafe class NativeArenaSlim : IDisposable
    {
        [Obsolete("Only for ObjectPool<> compatibility.", error: true)]
        public NativeArenaSlim() => throw new InvalidOperationException();
        public NativeArenaSlim(int initialSize)
        {
            lastChunkSize = initialSize / 2;

        }

        private List<(nuint Start, int Length)> chunks = new();
        private byte* nextAllocation;
        private byte* nextAllocationThreshold;

        private int lastChunkSize;
        private int consumedChunks;
        public byte* NextAllocation => nextAllocation;

        public bool IsDisposed => chunks != null;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte* Allocate(int size)
        {
            var updatedNextAllocation = nextAllocation + size;
            if (updatedNextAllocation >= nextAllocationThreshold)
            {
                return GrowAndAllocate(size);
            }
            var ptr = nextAllocation;
            nextAllocation = updatedNextAllocation;
            return ptr;
        }

        public Span<byte> GetRemaingSpaceInCurrentChunk() => new Span<byte>(nextAllocation, (int)(nextAllocationThreshold - nextAllocation));
        public void AdvanceBy(int bytes) => nextAllocation += bytes;


        public byte* Grow(int size)
        {

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


            var chunkSizeLong = Math.Max(BitOperations.RoundUpToPowerOf2((uint)size), lastChunkSize);
            if (chunkSizeLong * 2 < int.MaxValue)
                chunkSizeLong *= 2;
            var chunkSize = checked((int)chunkSizeLong);
            lastChunkSize = chunkSize;
            nextAllocation = (byte*)NativeMemory.Alloc((nuint)chunkSize);
            //if ((nuint)nextAllocation % 8 != 0) throw new Exception();
            Debug.Assert((nuint)nextAllocation % 8 == 0);
            nextAllocationThreshold = nextAllocation + chunkSize;
            chunks.Add(((nuint)nextAllocation, chunkSize));
            consumedChunks++;
            Debug.Assert(consumedChunks == chunks.Count);
            return nextAllocation;
        }

        public long TotalAllocatedSize => chunks.Sum(x => (long)x.Length);


        [MethodImpl(MethodImplOptions.NoInlining)]
        private byte* GrowAndAllocate(int size)
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
                NativeMemory.Free((void*)item.Start);
            }
            chunks = null!;
            nextAllocation = null;
        }

        ~NativeArenaSlim()
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


