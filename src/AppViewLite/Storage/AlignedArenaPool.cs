using Microsoft.Extensions.ObjectPool;
using AppViewLite.Storage;
using System;

namespace AppViewLite.Storage
{
    public static class AlignedArenaPool
    {

        public static readonly ObjectPool<AlignedArena512> Pool512 = ObjectPool.Create<AlignedArena512>();
        public static readonly ObjectPool<AlignedArena4096> Pool4096 = ObjectPool.Create<AlignedArena4096>();


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

        public class AlignedArena512 : PooledAlignedArena, IDisposable, IResettable
        {

            public AlignedArena512()
                : base(new AlignedNativeArena(512, 256 * 1024))
            {
            }

        }

        public class AlignedArena4096 : PooledAlignedArena, IDisposable, IResettable
        {

            public AlignedArena4096()
                : base(new AlignedNativeArena(4096, 512 * 1024))
            {
            }

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

