using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Models
{
    public record struct BlobResult(byte[]? Bytes, Stream? Stream, string? FileNameForDownload, bool IsFavIcon = false) : IDisposable
    {

        public async Task<byte[]> ReadAsBytesAsync()
        {
            if (Bytes != null) return Bytes;
            using var ms = new MemoryStream();
            using (Stream)
            {
                await Stream!.CopyToAsync(ms);
                return ms.ToArray();
            }

        }
        public void Dispose()
        {
            Stream?.Dispose();
        }

    }
}

