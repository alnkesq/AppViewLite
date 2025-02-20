using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite
{
    public static class ExtensionMethods
    {
        public static void FireAndForget(this Task task)
        {
            task.ContinueWith(t =>
            {
                if (t.IsFaulted)
                {
                    if (t.Exception.InnerException is OperationCanceledException)
                    {
                        return;
                    }
                    Console.Error.WriteLine(t.Exception);
                }
            }, TaskContinuationOptions.OnlyOnFaulted);
        }

        public static Queue<T> ToQueue<T>(this IEnumerable<T> items) => new(items);
        public static void EnqueueRange<T>(this Queue<T> queue, IEnumerable<T> items)
        {
            foreach (var item in items)
            {
                queue.Enqueue(item);
            }
        }

        public static T ReadUnmanaged<T>(this BinaryReader reader) where T : unmanaged
        {
            var bytes = reader.ReadBytes(Unsafe.SizeOf<T>());
            return MemoryMarshal.Cast<byte, T>(bytes)[0];
        }
        public static void WriteUnmanaged<T>(this BinaryWriter writer, T item) where T : unmanaged
        {
            writer.Write(MemoryMarshal.AsBytes([item]));
        }
    }


}

