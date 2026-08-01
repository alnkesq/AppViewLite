
using SkiaSharp;
using System;
using System.Collections.Generic;
using System.Linq;

namespace AppViewLite;

internal static class SkiaQuantizer
{
    private readonly struct Pixel
    {
        public readonly byte R, G, B, A;

        public Pixel(SKColor c)
        {
            R = c.Red;
            G = c.Green;
            B = c.Blue;
            A = c.Alpha;
        }

        public SKColor ToColor() => new(R, G, B, A);
    }

    private sealed class ColorBox
    {
        public List<Pixel> Pixels;

        public ColorBox(List<Pixel> pixels)
        {
            Pixels = pixels;
        }

        public int RangeR => Pixels.Max(p => p.R) - Pixels.Min(p => p.R);
        public int RangeG => Pixels.Max(p => p.G) - Pixels.Min(p => p.G);
        public int RangeB => Pixels.Max(p => p.B) - Pixels.Min(p => p.B);

        public int LongestAxis()
        {
            int r = RangeR;
            int g = RangeG;
            int b = RangeB;

            return r >= g && r >= b ? 0 :
                   g >= b ? 1 : 2;
        }

        public (ColorBox, ColorBox) Split()
        {
            int axis = LongestAxis();

            Pixels.Sort((a, b) => axis switch
            {
                0 => a.R.CompareTo(b.R),
                1 => a.G.CompareTo(b.G),
                _ => a.B.CompareTo(b.B)
            });

            int half = Pixels.Count / 2;

            return (
                new ColorBox(Pixels.Take(half).ToList()),
                new ColorBox(Pixels.Skip(half).ToList())
            );
        }

        public SKColor Average()
        {
            long r = 0, g = 0, b = 0, a = 0;

            foreach (var p in Pixels)
            {
                r += p.R;
                g += p.G;
                b += p.B;
                a += p.A;
            }

            int n = Pixels.Count;

            return new SKColor(
                (byte)(r / n),
                (byte)(g / n),
                (byte)(b / n),
                (byte)(a / n));
        }
    }

    public static SKBitmap Quantize(PixelAccessor source, int maxColors)
    {
        if (maxColors < 1)
            throw new ArgumentOutOfRangeException(nameof(maxColors));

        var pixels = new List<Pixel>(source.Width * source.Height);

        for (int y = 0; y < source.Height; y++)
        {
            for (int x = 0; x < source.Width; x++)
            {
                pixels.Add(new Pixel(source[x, y]));
            }
        }

        var boxes = new List<ColorBox>
        {
            new ColorBox(pixels)
        };

        while (boxes.Count < maxColors)
        {
            ColorBox? box = boxes
                .OrderByDescending(b =>
                    Math.Max(Math.Max(b.RangeR, b.RangeG), b.RangeB))
                .FirstOrDefault(b => b.Pixels.Count > 1);

            if (box == null)
                break;

            boxes.Remove(box);

            var (a, b) = box.Split();

            boxes.Add(a);
            boxes.Add(b);
        }

        var palette = boxes.Select(b => b.Average()).ToArray();

        var result = SKBitmap.Create(source.Width, source.Height);
        var resultAccessor = new PixelAccessor(result);
        for (int y = 0; y < source.Height; y++)
        {
            for (int x = 0; x < source.Width; x++)
            {
                var c = source[x, y];

                int best = 0;
                int bestDist = int.MaxValue;

                for (int i = 0; i < palette.Length; i++)
                {
                    var p = palette[i];

                    int dr = c.Red - p.Red;
                    int dg = c.Green - p.Green;
                    int db = c.Blue - p.Blue;
                    int da = c.Alpha - p.Alpha;

                    int dist =
                        dr * dr +
                        dg * dg +
                        db * db +
                        da * da;

                    if (dist < bestDist)
                    {
                        bestDist = dist;
                        best = i;
                    }
                }

                resultAccessor[x, y] = palette[best];
            }
        }

        GC.KeepAlive(result);
        return result;
    }
}
