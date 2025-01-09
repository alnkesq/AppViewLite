using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Storage
{
    public class SimpleColumnarWriter : IDisposable
    {
        
        private (BinaryWriter Writer, Action Commit)[] _columnWriters;
        public SimpleColumnarWriter(string destinationPrefix, int columnCount)
        {
            _columnWriters = Enumerable.Range(0, columnCount).Select(i => 
            {
                var destFile = destinationPrefix + ".col" + i + ".dat";
                var tempFile = destFile + ".tmp";
                var z = new FileStream(tempFile, FileMode.Create, FileAccess.Write, FileShare.None, 4096);
                return (new BinaryWriter(z), new Action(() => File.Move(tempFile, destFile, true)));
            }).ToArray();
        }

        public void WriteElement<T>(int columnIndex, T value) where T : unmanaged
        {
            _columnWriters[columnIndex].Writer.WriteUnmanaged(value);
        }

        public void Commit()
        {
            Dispose();
            foreach (var item in _columnWriters)
            {
                item.Commit();
            }
        }
        public void Dispose()
        {
            foreach (var item in _columnWriters)
            {
                item.Writer.Dispose();
            }
        }
    }
}

