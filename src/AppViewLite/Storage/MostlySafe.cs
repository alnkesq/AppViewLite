using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace AppViewLite
{
    public static class MostlySafe
    {
        public static TTo BitCast<TFrom, TTo>(TFrom source)
            where TFrom : allows ref struct
            where TTo : allows ref struct
        {
            return unsafe(Unsafe.BitCast<TFrom, TTo>(source));
        }

        public static ReadOnlySpan<TTo> Cast<TFrom, TTo>(ReadOnlySpan<TFrom> span)
            where TFrom : struct
            where TTo : struct
        {
            return unsafe (MemoryMarshal.Cast<TFrom, TTo>(span));
        }

        [OverloadResolutionPriority(1)]
        public static Span<TTo> Cast<TFrom, TTo>(Span<TFrom> span)
            where TFrom : struct
            where TTo : struct
        {
            return unsafe (MemoryMarshal.Cast<TFrom, TTo>(span));
        }
        public static Span<T> SkipInitForBuffer2<T>([UnscopedRef] out InlineArray2<T> buffer)
        {
            unsafe { Unsafe.SkipInit(out buffer); }
            return buffer;
        }
        public static Span<T> SkipInitForBuffer4<T>([UnscopedRef] out InlineArray4<T> buffer)
        {
            unsafe { Unsafe.SkipInit(out buffer); }
            return buffer;
        }
        public static Span<T> SkipInitForBuffer8<T>([UnscopedRef] out InlineArray8<T> buffer)
        {
            unsafe { Unsafe.SkipInit(out buffer); }
            return buffer;
        }
        public static Span<T> SkipInitForBuffer10<T>([UnscopedRef] out InlineArray10<T> buffer)
        {
            unsafe { Unsafe.SkipInit(out buffer); }
            return buffer;
        }
        public static Span<T> SkipInitForBuffer16<T>([UnscopedRef] out InlineArray16<T> buffer)
        {
            unsafe { Unsafe.SkipInit(out buffer); }
            return buffer;
        }

        public static ReadOnlySpan<byte> AsBytes<T>(ReadOnlySpan<T> span) where T : struct => unsafe(MemoryMarshal.AsBytes<T>(span));


        [OverloadResolutionPriority(1)]
        public static Span<byte> AsBytes<T>(Span<T> span) where T : struct => unsafe(MemoryMarshal.AsBytes<T>(span));
    }
}

