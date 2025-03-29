using Microsoft.Extensions.ObjectPool;
using AppViewLite.Storage;
using DuckDbSharp.Bindings;
using AppViewLite.Storage;
using System;

namespace AppViewLite.Storage
{
    public static class AlignedArenaPool
    {

        //public static ArenaBorrowScope AcquireScope(out AlignedNativeArena arena)
        //{
        //    var wrapper = Pool512.Get();
        //    arena = wrapper.Arena;
        //    return new ArenaBorrowScope(wrapper);
        //}

        //public static AlignedNativeArena Acquire()
        //{
        //    return Pool512.Get().arena;
        //}
        //private static void Return(AlignedNativeArena arena)
        //{
        //    if (arena.Alignment == 512) Pool512.Return(new AlignedArena512(arena));
        //    else throw new ArgumentException();
        //}


        public abstract class PooledAlignedArena
        {
            internal AlignedNativeArena Arena;
            public PooledAlignedArena(AlignedNativeArena arena)
            {
                Arena = arena;
            }
            public void Dispose()
            {
                Arena.Dispose();
            }

            public bool TryReset()
            {
                Arena.Reset();
                return true;
            }
        }

    }

    public class AlignedNativeArenaPoolingPolicy : IPooledObjectPolicy<AlignedNativeArena>
    {
        private readonly int alignment;
        private readonly nuint initialSize;
        public AlignedNativeArenaPoolingPolicy(int alignment, nuint initialSize)
        {
            this.alignment = alignment;
            this.initialSize = initialSize;
        }
        public AlignedNativeArena Create()
        {
            return new AlignedNativeArena(alignment, initialSize);
        }

        public bool Return(AlignedNativeArena obj)
        {
            if (obj.TotalAllocatedSize == (long)initialSize)
            {
                obj.Reset();
                return true;   
            }
            return false;
        }
    }


    public class NativeArenaSlimPoolingPolicy : IPooledObjectPolicy<NativeArenaSlim>
    {
        private readonly int initialSize;
        public NativeArenaSlimPoolingPolicy(int initialSize)
        {
            this.initialSize = initialSize;
        }
        public NativeArenaSlim Create()
        {
            return new NativeArenaSlim(initialSize);
        }

        public bool Return(NativeArenaSlim obj)
        {
            if (obj.TotalAllocatedSize == initialSize)
            {
                obj.Reset();
                return true;
            }
            return false;
        }
    }


    //public ref struct ArenaBorrowScope
    //{
    //    internal AlignedArenaPool.AlignedArena512 wrapper;
    //    internal ArenaBorrowScope(AlignedArenaPool.AlignedArena512 wrapper)
    //    {
    //        this.wrapper = wrapper;
    //    }

    //    public void Dispose()
    //    {
    //        if (wrapper == null) return;
    //        AlignedArenaPool.Pool512.Return(wrapper);
    //        wrapper = null!;
    //    }
    //}


}

