using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Storage
{
    internal static class ExtensionMethods
    {
        public static void WriteUnmanaged<T>(this BinaryWriter writer, T item) where T : unmanaged
        {
            writer.Write(MemoryMarshal.AsBytes(new ReadOnlySpan<T>(in item)));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long BinarySearch<T>(
            this HugeReadOnlySpan<T> span, T comparable)
            where T : IComparable<T>
        {

            return HugeSpanHelpers.BinarySearch(span, comparable);
        }
    }
}
