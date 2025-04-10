using SixLabors.ImageSharp;
using SixLabors.ImageSharp.Formats.Jpeg;
using SixLabors.ImageSharp.Formats.Webp;
using SixLabors.ImageSharp.PixelFormats;
using SixLabors.ImageSharp.Processing;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace AppViewLite
{
    public static class ImageUploadProcessor
    {
        public static async Task<(MemoryStream ProcessedBytes, int Width, int Height)> ProcessAsync(byte[] uploadedBytes, CancellationToken ct)
        {
            if (!StartsWithAllowlistedMagicNumber(uploadedBytes))
                throw new Exception("Unsupported image format.");

            ct.ThrowIfCancellationRequested();
            using var image = Image<Rgba32>.Load(uploadedBytes);
            using var redrawn = new Image<Rgba32>(image.Width, image.Height);

            redrawn.Mutate(m => m.DrawImage(image, 1));
            //int[] qualityAttempts = [85, 75, 60, 40, 20, 5]; // JPEG
            int[] qualityAttempts = [75, 70, 63, 50, 20];
            var ms = new MemoryStream();
            foreach (var attemptQuality in qualityAttempts)
            {
                await redrawn.SaveAsWebpAsync(ms, new WebpEncoder
                {
                    Quality = attemptQuality
                }, ct);

                ms.Seek(0, SeekOrigin.Begin);

                if (ms.Length <= 1024 * 1024)
                    return (ms, image.Width, image.Height);

                ms.SetLength(0);
            }
            throw new Exception("Unable to compress the image to stay within PDS size limits, despite multiple attempts at progressively decreasing quality.");


        }

        private static ReadOnlySpan<byte> Magic_JPG => [0xff, 0xd8, 0xff];
        private static ReadOnlySpan<byte> Magic_PNG => [0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a];
        private static ReadOnlySpan<byte> Magic_RIFF => "RIFF"u8;
        private static ReadOnlySpan<byte> Magic_WEBP => [0x57, 0x45, 0x42, 0x50, 0x56, 0x50, 0x38];
        private static ReadOnlySpan<byte> Magic_GIF87a => "GIF87a"u8;
        private static ReadOnlySpan<byte> Magic_GIF89a => "GIF89a"u8;
        public static ReadOnlySpan<byte> Magic_ICO => "\x00\x00\x01\x00"u8;
        public static bool StartsWithAllowlistedMagicNumber(ReadOnlySpan<byte> bytes)
        {
            return
                bytes.StartsWith(Magic_JPG) ||
                bytes.StartsWith(Magic_PNG) ||
                bytes.StartsWith(Magic_WEBP) ||
                bytes.StartsWith(Magic_GIF87a) ||
                bytes.StartsWith(Magic_GIF89a) ||
                bytes.StartsWith(Magic_ICO) ||
                (bytes.StartsWith(Magic_RIFF) && bytes.Slice(8).StartsWith(Magic_WEBP))
                ;
        }
    }
}
