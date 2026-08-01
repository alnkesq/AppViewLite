using SkiaSharp;
using System;
using System.Collections.Generic;
using System.Drawing;
using System.Threading.Tasks;

namespace AppViewLite;

internal static class LanczosResizer
{
    private readonly struct Tap
    {
        public readonly int Index;
        public readonly float Weight;

        public Tap(int index, float weight)
        {
            Index = index;
            Weight = weight;
        }
    }

    public static void ResizeLanczos3(
        SKBitmap input,
        SKBitmap output,
        Rectangle inputRectangle,
        Rectangle outputRectangle)
    {
        if (input.AlphaType != SKAlphaType.Unpremul || output.AlphaType != SKAlphaType.Unpremul) throw new NotSupportedException();

        var xTaps = BuildTaps(inputRectangle.Width, outputRectangle.Width);
        var yTaps = BuildTaps(inputRectangle.Height, outputRectangle.Height);

        using var temp = new SKBitmap(
            new SKImageInfo(
                outputRectangle.Width,
                inputRectangle.Height,
                SKColorType.Bgra8888,
                SKAlphaType.Premul));


        
        var inputAccessor = new PixelAccessor(input);
        var tempAccessor = new PixelAccessor(temp);
        var outputAccessor = new PixelAccessor(output);

        // Horizontal pass: unpremul input -> premul temp

        Parallel.For(0, inputRectangle.Height, new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount }, y =>
        {
            int srcY = inputRectangle.Y + y;

            for (int x = 0; x < outputRectangle.Width; x++)
            {
                float b = 0;
                float g = 0;
                float r = 0;
                float a = 0;

                foreach (var tap in xTaps[x])
                {
                    SKColor c = inputAccessor[inputRectangle.X + tap.Index, srcY];

                    float w = tap.Weight;
                    float alpha = c.Alpha / 255f;
                    var alphaW = alpha * w;
                    b += c.Blue * alphaW;
                    g += c.Green * alphaW;
                    r += c.Red * alphaW;
                    a += c.Alpha * w;
                }

                tempAccessor[x, y] = new SKColor(
                    Clamp(r),
                    Clamp(g),
                    Clamp(b),
                    Clamp(a));
            }
        });

        // Vertical pass: premul temp -> unpremul output
        Parallel.For(0, outputRectangle.Height, new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount }, y =>
        {
            for (int x = 0; x < outputRectangle.Width; x++)
            {
                float b = 0;
                float g = 0;
                float r = 0;
                float a = 0;

                foreach (var tap in yTaps[y])
                {
                    SKColor c = tempAccessor[x, tap.Index];

                    float w = tap.Weight;

                    b += c.Blue * w;
                    g += c.Green * w;
                    r += c.Red * w;
                    a += c.Alpha * w;
                }

                if (a > 0)
                {
                    float scale = 255f / a;
                    r *= scale;
                    g *= scale;
                    b *= scale;
                }

                outputAccessor[outputRectangle.X + x, outputRectangle.Y + y] =
                    new SKColor(
                        Clamp(r),
                        Clamp(g),
                        Clamp(b),
                        Clamp(a));
            }
        });


        GC.KeepAlive(input);
        GC.KeepAlive(output);
        GC.KeepAlive(temp);
    }

    private static Tap[][] BuildTaps(int srcSize, int dstSize)
    {
        float scale = (float)srcSize / dstSize;
        float filterScale = MathF.Max(scale, 1f);

        int radius = (int)MathF.Ceiling(3 * filterScale);

        var result = new Tap[dstSize][];

        for (int dst = 0; dst < dstSize; dst++)
        {
            float center = (dst + 0.5f) * scale - 0.5f;

            int first = (int)MathF.Floor(center - radius);
            int last = (int)MathF.Ceiling(center + radius);

            var taps = new List<Tap>();

            float sum = 0;

            for (int src = first; src <= last; src++)
            {
                int index = Math.Clamp(src, 0, srcSize - 1);

                float distance = center - src;

                float weight =
                    Lanczos3(distance / filterScale) / filterScale;

                taps.Add(new Tap(index, weight));
                sum += weight;
            }

            var normalized = new Tap[taps.Count];

            for (int i = 0; i < taps.Count; i++)
            {
                normalized[i] = new Tap(
                    taps[i].Index,
                    taps[i].Weight / sum);
            }

            result[dst] = normalized;
        }

        return result;
    }

    private static float Lanczos3(float x)
    {
        x = MathF.Abs(x);

        if (x < 1e-6f)
            return 1f;

        if (x >= 3f)
            return 0f;

        float px = MathF.PI * x;

        return
            3f *
            MathF.Sin(px) *
            MathF.Sin(px / 3f) /
            (px * px);
    }

    private static byte Clamp(float x)
    {
        return (byte)Math.Clamp(
            (int)MathF.Round(x),
            0,
            255);
    }
}
