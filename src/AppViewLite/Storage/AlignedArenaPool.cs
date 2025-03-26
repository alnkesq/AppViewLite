using Microsoft.Extensions.ObjectPool;
using AppViewLite.Storage;
using System;

namespace AppViewLite.Storage
{
    public static class AlignedArenaPool
    {

        public static readonly ObjectPool<AlignedArena512> Pool512 = ObjectPool.Create<AlignedArena512>();


        public static ArenaBorrowScope AcquireScope(out AlignedNativeArena arena)
        {
            var wrapper = Pool512.Get();
            arena = wrapper.Arena;
            return new ArenaBorrowScope(wrapper);
        }

        //public static AlignedNativeArena Acquire()
        //{
        //    return Pool512.Get().arena;
        //}
        //private static void Return(AlignedNativeArena arena)
        //{
        //    if (arena.Alignment == 512) Pool512.Return(new AlignedArena512(arena));
        //    else throw new ArgumentException();
        //}

        public class AlignedArena512 : IDisposable, IResettable
        {
            internal AlignedNativeArena Arena;
            public AlignedArena512(AlignedNativeArena arena)
            {
                Arena = arena;
            }
            public AlignedArena512()
            {
                Arena = new AlignedNativeArena(512, 256 * 1024);
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


    public ref struct ArenaBorrowScope
    {
        internal AlignedArenaPool.AlignedArena512 wrapper;
        internal ArenaBorrowScope(AlignedArenaPool.AlignedArena512 wrapper)
        {
            this.wrapper = wrapper;
        }

        public void Dispose()
        {
            if (wrapper == null) return;
            AlignedArenaPool.Pool512.Return(wrapper);
            wrapper = null!;
        }
    }



}

