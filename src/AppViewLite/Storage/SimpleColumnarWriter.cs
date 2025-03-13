using System;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;

namespace AppViewLite.Storage
{
    public class SimpleColumnarWriter : IDisposable
    {
        private bool wasCommitted;
        private (BinaryWriter Writer, Action Commit, string TempFile)[] _columnWriters;
        public SimpleColumnarWriter(string destinationPrefix, int columnCount)
        {
            _columnWriters = Enumerable.Range(0, columnCount).Select(i =>
                {
                    var destFile = destinationPrefix + ".col" + i + ".dat";
                    var tempFile = destFile + ".tmp";
                    var z = new FileStream(tempFile, FileMode.Create, FileAccess.Write, FileShare.None, 4096);
                    return (new BinaryWriter(z), new Action(() => File.Move(tempFile, destFile, true)), tempFile);
                }).ToArray();
        }

        public void WriteElement<T>(int columnIndex, T value) where T : unmanaged
        {
            _columnWriters[columnIndex].Writer.WriteUnmanaged(value);
        }
        public void WriteElementRange<T>(int columnIndex, ReadOnlySpan<T> values) where T : unmanaged
        {
            _columnWriters[columnIndex].Writer.Write(MemoryMarshal.AsBytes(values));
        }

        public long CommitAndGetSize()
        {
            wasCommitted = true;
            var size = DisposeAndGetSize();
            foreach (var item in _columnWriters)
            {
                item.Commit();
            }
            return size;
        }
        public void Dispose()
        {
            foreach (var item in _columnWriters)
            {
                item.Writer.Dispose();
                if (!wasCommitted)
                    File.Delete(item.TempFile);
            }
        }
        public long DisposeAndGetSize()
        {
            long size = 0;
            foreach (var item in _columnWriters)
            {
                size += item.Writer.BaseStream.Length;
                item.Writer.Dispose();
                if (!wasCommitted)
                    File.Delete(item.TempFile);
            }
            return size;
        }
    }
}

