using AppViewLite.Storage;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Storage
{
    public class SimpleColumnarReader : IDisposable
    {
        private readonly MemoryMappedFileSlim[] columns;
        public SimpleColumnarReader(string pathPrefix, int columnCount)
        {
            var cols = new List<MemoryMappedFileSlim>();
            try
            {
                for (int i = 0; i < columnCount; i++)
                {
                    cols.Add(new MemoryMappedFileSlim(pathPrefix + ".col" + i + ".dat"));
                }
            }
            catch
            {
                foreach (var col in cols)
                {
                    col.Dispose();
                }
                throw;
            }
            this.columns = cols.ToArray();
        }

        public void Dispose()
        {
            foreach (var col in columns)
            {
                col.Dispose();
            }
        }

        private unsafe static long ByteLengthToElementCount<T>(long byteLength) where T : unmanaged
        {
            return byteLength / sizeof(T);
        }

        public unsafe HugeReadOnlyMemory<T> GetColumnHugeMemory<T>(int index) where T : unmanaged
        {
            var col = columns[index];
            col.EnsureValid();
            return new HugeReadOnlyMemory<T>((T*)col.Pointer, ByteLengthToElementCount<T>(col.Length));
        }
        public unsafe DangerousHugeReadOnlyMemory<T> GetColumnDangerousHugeMemory<T>(int index) where T : unmanaged
        {
            var col = columns[index];
            col.EnsureValid();
            return new DangerousHugeReadOnlyMemory<T>((T*)col.Pointer, ByteLengthToElementCount<T>(col.Length));
        }
        public unsafe HugeReadOnlySpan<T> GetColumnHugeSpan<T>(int index) where T : unmanaged
        {
            var col = columns[index];
            col.EnsureValid();
            return new HugeReadOnlySpan<T>((T*)col.Pointer, ByteLengthToElementCount<T>(col.Length));
        }
        public unsafe ReadOnlySpan<T> GetColumnSpan<T>(int index) where T : unmanaged
        {
            var col = columns[index];
            col.EnsureValid();
            return new ReadOnlySpan<T>((T*)col.Pointer, checked((int)ByteLengthToElementCount<T>(col.Length)));
        }
    }
}

