using SixLabors.ImageSharp.Formats.Icon;
using SkiaSharp;
using System;
using System.Collections.Generic;
using System.Drawing;
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

        public static SKBitmap LoadLargestImage(byte[] bytes)
        {
            var best = GetIconSizes(bytes).MaxBy(x => x.Size.Width * x.Size.Height);
            if (best.Bytes.AsSpan().StartsWith(Magic_PNG))
            {
                return SKBitmap.Load(best.Bytes);
            }
            else
            {
                return ConvertIcoEntryToBitmap(best.Size.Width, best.Size.Height, best.IconDirEntry.ColorCount, best.IconDirEntry.BitCount, best.Bytes);
            }
        }

        private static SKBitmap ConvertIcoEntryToBitmap(
            int width,
            int height,
            byte colorCount,
            ushort bitCountFromEntry,
            ReadOnlySpan<byte> entryBytes)
        {
            var header = MemoryMarshal.Read<BITMAPINFOHEADER>(entryBytes);

            int bitCount = header.biBitCount;

            int paletteEntries = bitCount <= 8
                ? (colorCount != 0 ? colorCount : 1 << bitCount)
                : 0;

            int paletteSize = paletteEntries * 4;

            var palette = entryBytes.Slice(40, paletteSize);

            int xorOffset = 40 + paletteSize;

            int xorStride = ((width * bitCount + 31) / 32) * 4;
            int andStride = ((width + 31) / 32) * 4;

            var xor = entryBytes.Slice(xorOffset, xorStride * height);
            var and = entryBytes.Slice(xorOffset + xor.Length, andStride * height);

            var bitmap = SKBitmap.Create(width, height);

            var bitmapAccessor = new PixelAccessor(bitmap);

            bool hasAlpha = false;

            if (bitCount == 32)
            {
                for (int i = 3; i < xor.Length; i += 4)
                {
                    if (xor[i] != 0)
                    {
                        hasAlpha = true;
                        break;
                    }
                }
            }

            for (int y = 0; y < height; y++)
            {

                int xorY = height - 1 - y;

                for (int x = 0; x < width; x++)
                {
                    byte b, g, r;


                    bool transparent =
                        (and[xorY * andStride + (x >> 3)] &
                            (0x80 >> (x & 7))) != 0;

                    byte a = transparent ? (byte)0 : (byte)255;

                    switch (bitCount)
                    {
                        case 32:
                            {
                                int p = xorY * xorStride + x * 4;
                                b = xor[p + 0];
                                g = xor[p + 1];
                                r = xor[p + 2];

                                byte xorAlpha = xor[p + 3];


                                // XP-era 32bpp icons:
                                // - alpha may be valid
                                // - alpha may be completely unused (all 0)
                                // - AND mask still matters

                                if (transparent)
                                    a = 0;
                                else if (hasAlpha)
                                    a = xorAlpha;
                                else
                                    a = 255;

                                break;
                            }

                        case 24:
                            {
                                int p = xorY * xorStride + x * 3;
                                b = xor[p + 0];
                                g = xor[p + 1];
                                r = xor[p + 2];
                                break;
                            }

                        case 8:
                            {
                                int index = xor[xorY * xorStride + x];
                                int p = index * 4;
                                b = palette[p];
                                g = palette[p + 1];
                                r = palette[p + 2];
                                break;
                            }

                        case 4:
                            {
                                byte packed = xor[xorY * xorStride + (x >> 1)];

                                int index = (x & 1) == 0
                                    ? packed >> 4
                                    : packed & 0x0F;

                                int p = index * 4;

                                b = palette[p + 0];
                                g = palette[p + 1];
                                r = palette[p + 2];
                                break;
                            }

                        case 1:
                            {
                                byte packed = xor[xorY * xorStride + (x >> 3)];

                                int index = (packed & (0x80 >> (x & 7))) != 0
                                    ? 1
                                    : 0;

                                int p = index * 4;

                                b = palette[p + 0];
                                g = palette[p + 1];
                                r = palette[p + 2];
                                break;
                            }
                        default:
                            throw new NotSupportedException($"ICO {bitCount}-bit not supported");
                    }


                    bitmapAccessor[x, y] = new SKColor(r, g, b, a);
                }
            }

            GC.KeepAlive(bitmap);
            return bitmap;
        }
    }
}

