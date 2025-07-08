using Microsoft.Win32.SafeHandles;
using AppViewLite;
using AppViewLite.Storage;
using System;
using System.Collections.Generic;

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
                    var file = new MemoryMappedFileSlim(CombinedPersistentMultiDictionary.ToPhysicalPath(pathPrefix + ".col" + i + ".dat"), randomAccess: true);

                    file.DirectIoReadCache = CombinedPersistentMultiDictionary.DirectIoReadCache;
                    cols.Add(file);
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

        public IReadOnlyList<MemoryMappedFileSlim> Columns => columns;
        public void Dispose()
        {
            foreach (var col in columns)
            {
                col.Dispose();
            }
        }

        private static unsafe long ByteLengthToElementCount<T>(MemoryMappedFileSlim col) where T : unmanaged
        {
            var byteLength = col.Length;
            if (byteLength % sizeof(T) != 0)
            {
                throw new Exception($"Size of file should be a multiple of sizeof({typeof(T).Name}). Was this file truncated, possibly due to a partial file copy? {col.Path}");
            }
            return byteLength / sizeof(T);
        }

        public MemoryMappedFileSlim GetMemoryMappedFile(int index) => columns[index];
        public unsafe HugeReadOnlyMemory<T> GetColumnHugeMemory<T>(int index) where T : unmanaged
        {
            var col = columns[index];
            col.EnsureValid();
            return new HugeReadOnlyMemory<T>((T*)col.Pointer, ByteLengthToElementCount<T>(col));
        }
        public unsafe DangerousHugeReadOnlyMemory<T> GetColumnDangerousHugeMemory<T>(int index) where T : unmanaged
        {
            var col = columns[index];
            col.EnsureValid();
            return new DangerousHugeReadOnlyMemory<T>((T*)col.Pointer, ByteLengthToElementCount<T>(col));
        }
        public unsafe HugeReadOnlySpan<T> GetColumnHugeSpan<T>(int index) where T : unmanaged
        {
            var col = columns[index];
            col.EnsureValid();
            return new HugeReadOnlySpan<T>((T*)col.Pointer, ByteLengthToElementCount<T>(col));
        }
        public unsafe ReadOnlySpan<T> GetColumnSpan<T>(int index) where T : unmanaged
        {
            var col = columns[index];
            col.EnsureValid();
            return new ReadOnlySpan<T>((T*)col.Pointer, checked((int)ByteLengthToElementCount<T>(col)));
        }
    }
}

