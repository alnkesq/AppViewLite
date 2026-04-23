using System;
using System.IO;
using System.Threading.Tasks;

namespace AppViewLite.Models
{
    public record struct BlobResult(byte[]? Bytes, Stream? Stream, string? FileNameForDownload, bool IsFavIcon = false) : IDisposable
    {

        public async readonly Task<byte[]> ReadAsBytesAsync()
        {
            if (Bytes != null) return Bytes;
            using var ms = new MemoryStream();
            using (Stream)
            {
                await Stream!.CopyToAsync(ms);
                return ms.ToArray();
            }

        }
        public readonly void Dispose()
        {
            Stream?.Dispose();
        }

    }
}

