using SixLabors.ImageSharp;
using SixLabors.ImageSharp.Formats.Icon;
using SixLabors.ImageSharp.PixelFormats;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;

namespace AppViewLite.IconParser
{
    public static class IconUtils
    {
        public static (Size Size, IconDirEntry IconDirEntry, byte[] Bytes)[] GetIconSizes(byte[] ico)
        {
            using var br = new BinaryReader(new MemoryStream(ico));
            var iconDir = br.ReadUnmanaged<IconDir>();
            if (iconDir.Type != IconFileType.ICO) throw new NotSupportedException("Not an ICO file.");
            var entries = new List<IconDirEntry>();
            return Enumerable.Range(0, iconDir.Count).Select(x =>
            {
                var entry = br.ReadUnmanaged<IconDirEntry>();
                return (new Size(
                    entry.Width == 0 ? 256 : entry.Width,
                    entry.Height == 0 ? 256 : entry.Height),
                    entry,
                    ico.AsSpan((int)entry.ImageOffset, (int)entry.BytesInRes).ToArray());
            }).ToArray();

        }
        private static ReadOnlySpan<byte> Magic_PNG => [0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a];

        public static Image<Rgba32> LoadLargestImage(byte[] bytes)
        {
            var best = GetIconSizes(bytes).MaxBy(x => x.Size.Width * x.Size.Height);
            if (best.Bytes.AsSpan().StartsWith(Magic_PNG))
            {
                return Image.Load<Rgba32>(best.Bytes);
            }
            else
            {
                var bmp = ConvertIcoEntryToBmp(best.Size.Width, best.Size.Height, best.IconDirEntry.ColorCount, best.IconDirEntry.BitCount, best.Bytes);
                return Image.Load<Rgba32>(bmp);
            }
        }


        public static byte[] ConvertIcoEntryToBmp(int actualWidth, int actualHeight, byte colorCountFromEntry, ushort bitCountFromEntry, ReadOnlySpan<byte> entryBytes)
        {

            int bytesPerPixel = bitCountFromEntry / 8;
            int rowStride = ((actualWidth * bytesPerPixel + 3) / 4) * 4;
            int imageSize = rowStride * actualHeight;

            const int fileHeaderSize = 14;
            const int infoHeaderSize = 40;
            int bmpSize = fileHeaderSize + infoHeaderSize + imageSize;

            using (MemoryStream ms = new MemoryStream())
            using (BinaryWriter writer = new BinaryWriter(ms))
            {
                var infoHeader = MemoryMarshal.Cast<byte, BITMAPINFOHEADER>(entryBytes)[0];

                int paletteSize = 0;
                var bitCount = infoHeader.biBitCount;
                if (bitCount <= 8)
                {
                    var colorCount = colorCountFromEntry;
                    int maxColors = 1 << bitCount;
                    var actualColors = (colorCount > 0 && colorCount < maxColors) ? colorCount : maxColors;
                    paletteSize = bitCount <= 8 ? actualColors * 4 : 0;
                }

                writer.WriteUnmanaged(new BITMAPFILEHEADER
                {
                    bfType = 0x4D42, // "BM"
                    bfSize = (uint)bmpSize,
                    bfOffBits = (uint)(fileHeaderSize + infoHeaderSize + paletteSize)
                });


                infoHeader.biHeight /= 2;
                writer.WriteUnmanaged(infoHeader);

                entryBytes = entryBytes.Slice(infoHeaderSize);
                writer.Write(entryBytes);
                return ms.ToArray();
            }
        }

    }
}

