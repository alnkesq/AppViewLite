using System;
using System.Buffers;
using System.Diagnostics.Contracts;
using System.Drawing;
using System.Globalization;
using System.IO;
using System.Runtime.CompilerServices;

namespace SkiaSharp
{
    public static class ImageSharpCompat
    {

        private static int Pow2(int a) => a * a;

        public static double ColorDistancePow(SKColor a, SKColor b)
        {
            var diff = Math.Sqrt(Pow2(a.R - b.R) + Pow2(a.G - b.G) + Pow2(a.B - b.B));
            return diff;

        }

        public const SKColorType BlittableSKColorType = SKColorType.Bgra8888;
        public const int DefaultWebpQuality = 75;
        public const int DefaultJpegQuality = 85;
        //public const int DefaultJpegXlQuality = 80;
        public static SKColorFilter CreateBrightnessFilter(float brightness)
        {
            return SKColorFilter.CreateColorMatrix(
            [
                brightness, 0,          0,          0, 0,
                0,          brightness, 0,          0, 0,
                0,          0,          brightness, 0, 0,
                0,          0,          0,          1, 0
            ]);
        }

        
        public static SKColorFilter CreateSaturationFilter(float saturation)
        {
            const float r = 0.2126f;
            const float g = 0.7152f;
            const float b = 0.0722f;

            float inv = 1f - saturation;

            ReadOnlySpan<float> matrix =
            [
                    r * inv + saturation, g * inv,             b * inv,             0, 0,
                    r * inv,             g * inv + saturation, b * inv,             0, 0,
                    r * inv,             g * inv,             b * inv + saturation, 0, 0,
                    0,                   0,                   0,                   1, 0
            ];

            var filter = SKColorFilter.CreateColorMatrix(matrix);
            return filter;
        }


        public static SKColorFilter CreateOpacityFilter(float opacity)
        {

            return SKColorFilter.CreateColorMatrix([
                1, 0, 0, 0, 0,
                0, 1, 0, 0, 0,
                0, 0, 1, 0, 0,
                0, 0, 0, opacity, 0
            ]);
        }

        extension(SKSamplingOptions options)
        {
            public static SKSamplingOptions NearestNeighbor => new SKSamplingOptions(SKFilterMode.Nearest);
            public static SKSamplingOptions Medium => new SKSamplingOptions(SKCubicResampler.Mitchell);
        }


        extension(SKResizeQuality quality)
        {
            public SKSamplingOptions ToSKSamplingOptions()
            {
                return quality switch
                {
                    SKResizeQuality.NearestNeighbor => SKSamplingOptions.NearestNeighbor,
                    SKResizeQuality.Medium => SKSamplingOptions.Medium,
                    _ => throw new NotSupportedException(),
                };
            }
        }

        extension(SKBitmap bitmap)
        {
#if NET11_0_OR_GREATER
            [Obsolete("SKBitmap[int x, int y] is slow, prefer ProcessPixelRows instead.")]
            public SKColor this[int x, int y]
            {
                get
                {
                    return bitmap.GetPixel(x, y);
                }
                set
                {
                    bitmap.SetPixel(x, y, value);
                }
            }
#endif
            public void SaveAsPng(string destination)
            {
                using var data = bitmap.Encode(SKEncodedImageFormat.Png, 100);
                SaveBytes(data, destination);
            }
            public void SaveAsWebp(string destination, int quality = ImageSharpCompat.DefaultWebpQuality)
            {
                using var data = bitmap.Encode(SKEncodedImageFormat.Webp, quality);
                SaveBytes(data, destination);
            }
            public void SaveAsJpeg(string destination, int quality = ImageSharpCompat.DefaultJpegQuality)
            {
                using var data = bitmap.Encode(SKEncodedImageFormat.Jpeg, quality);
                SaveBytes(data, destination);
            }
            //public void SaveAsJpegXl(string destination, int quality = ImageSharpCompat.DefaultJpegXlQuality)
            //{
            //    using var data = bitmap.Encode(SKEncodedImageFormat.Jpegxl, quality);
            //    SaveBytes(data, destination);
            //}
            public void SaveAsPng(System.IO.Stream destination)
            {
                using var data = bitmap.Encode(SKEncodedImageFormat.Png, 100);
                SaveBytes(data, destination);
            }
            public void SaveAsWebp(System.IO.Stream destination, int quality = ImageSharpCompat.DefaultWebpQuality)
            {
                using var data = bitmap.Encode(SKEncodedImageFormat.Webp, quality);
                SaveBytes(data, destination);
            }
            public void SaveAsJpeg(System.IO.Stream destination, int quality = ImageSharpCompat.DefaultJpegQuality)
            {
                using var data = bitmap.Encode(SKEncodedImageFormat.Jpeg, quality);
                SaveBytes(data, destination);
            }
            //public void SaveAsJpegXl(System.IO.Stream destination, int quality = ImageSharpCompat.DefaultJpegXlQuality)
            //{
            //    using var data = bitmap.Encode(SKEncodedImageFormat.Jpegxl, quality);
            //    SaveBytes(data, destination);
            //}

            private static void SaveBytes(SKData data, string destination)
            {
                System.IO.File.WriteAllBytes(destination, data.AsSpan());
            }
            private static void SaveBytes(SKData data, System.IO.Stream destination)
            {
                destination.Write(data.AsSpan());
            }
            public Size Size => new Size(bitmap.Width, bitmap.Height);
            public Rectangle Rectangle => new Rectangle(0, 0, bitmap.Width, bitmap.Height);

            public static SKBitmap Create(int width, int height) => new SKBitmap(width, height, ImageSharpCompat.BlittableSKColorType, SKAlphaType.Unpremul);
            public static SKBitmap Load(string path, SKColorType colorType = ImageSharpCompat.BlittableSKColorType, SKAlphaType alphaType = SKAlphaType.Unpremul)
            {
                using var codec = SKCodec.Create(path).CheckSucceeded();
                return Load(codec, colorType, alphaType);
            }
            public static SKBitmap Load(string path, out SKCodecInfoStruct info, SKColorType colorType = ImageSharpCompat.BlittableSKColorType, SKAlphaType alphaType = SKAlphaType.Unpremul)
            {
                using var codec = SKCodec.Create(path).CheckSucceeded();
                info = CopyCodecInfo(codec);
                return Load(codec, colorType, alphaType);
            }

            public static SKBitmap Load(ReadOnlySpan<byte> bytes, SKColorType colorType = ImageSharpCompat.BlittableSKColorType, SKAlphaType alphaType = SKAlphaType.Unpremul)
            {
                fixed (byte* b = bytes)
                {
                    using var skdata = SKData.Create((IntPtr)b, bytes.Length);
                    using var codec = SKCodec.Create(skdata).CheckSucceeded();
                    return Load(codec, colorType, alphaType);
                }
            }

            private static SKBitmap Load(SKCodec codec, SKColorType colorType, SKAlphaType alphaType)
            {
                var info = codec.Info;
                info.ColorType = colorType;
                info.AlphaType = alphaType;

                var resultBitmap = new SKBitmap(info);
                var result = codec.GetPixels(info, resultBitmap.GetPixels());

                if (result != SKCodecResult.Success &&
                    result != SKCodecResult.IncompleteInput)
                {
                    resultBitmap.Dispose();
                    throw new InvalidOperationException($"Decoding failed: {result}");
                }
                return resultBitmap;
            }

            private static SKCodecInfoStruct CopyCodecInfo(SKCodec codec)
            {
                return new SKCodecInfoStruct
                {
                    FrameCount = codec.FrameCount,
                    EncodedOrigin = codec.EncodedOrigin,
                    EncodedFormat = codec.EncodedFormat,
                };
            }
            public static SKBitmap Load(ReadOnlySpan<byte> bytes, out SKCodecInfoStruct info)
            {
                fixed (byte* b = bytes)
                {
                    using var skdata = SKData.Create((IntPtr)b, bytes.Length);
                    using var codec = SKCodec.Create(skdata).CheckSucceeded();
                    info = CopyCodecInfo(codec);
                    var result = SKBitmap.Decode(codec);
                    if (result == null) throw new InvalidDataException("Image could not be parsed.");
                    return result;
                }
            }

            public SKBitmap Clone() => bitmap.Copy();
            public void Mutate(Action<SKCanvas> action)
            {
                using var canvas = new SKCanvas(bitmap);
                action(canvas);
            }

            [Pure]
            public SKBitmap Clone(Action<SKCanvas> action)
            {
                var cloned = bitmap.Clone();
                cloned.Mutate(action);
                return cloned;
            }
            public Memory<SKColor> DangerousGetPixelRowMemory(int row)
            {
                return new SKBitmapRowMemoryManager(bitmap, row).Memory;
            }

            public void ProcessPixelRows(Action<PixelAccessor> processPixels)
            {
                var accessor = new PixelAccessor(bitmap);
                processPixels(accessor);
                GC.KeepAlive(bitmap);
            }
            public T ProcessPixelRows<T>(Func<PixelAccessor, T> processPixels)
            {
                var accessor = new PixelAccessor(bitmap);
                var result = processPixels(accessor);
                GC.KeepAlive(bitmap);
                return result;
            }
            public SKBitmap CreateWithSameSize() => new SKBitmap(bitmap.Info);
            public SKBitmap CreateWithSize(Size size) => new SKBitmap(bitmap.Info.WithSize(size.AsSKSizeI));
            public SKBitmap CreateWithSize(int width, int height) => bitmap.CreateWithSize(new Size(width, height));
            public SKBitmap ClonePadded(Size size, SKColor borderColor)
            {
                var padded = bitmap.CreateWithSize(size);

                using (var canvas = new SKCanvas(padded))
                {
                    canvas.Clear(borderColor);

                    var x = (size.Width - bitmap.Width) / 2;
                    var y = (size.Height - bitmap.Height) / 2;

                    canvas.DrawBitmap(bitmap, x, y, SKSamplingOptions.NearestNeighbor);
                }
                return padded;
            }
            public void DrawBitmap(SKBitmap source, Rectangle destRect, SKResizeQuality quality)
            {
                bitmap.DrawBitmap(source, source.Rectangle, destRect, quality);
            }
            public void DrawBitmap(SKBitmap source, Rectangle srcRect, Rectangle destRect, SKResizeQuality quality)
            {
                if (quality == SKResizeQuality.High)
                {
#if MINIMAL_IMAGESHARP_COMPAT
                    throw new NotSupportedException();
#else
                    AppViewLite.LanczosResizer.ResizeLanczos3(source, bitmap, srcRect, destRect);
#endif
                }
                else
                {
                    bitmap.Mutate(canvas => 
                    {
                        canvas.DrawBitmap(
                            source,
                            srcRect.AsSKRect,
                            destRect.AsSKRect,
                            quality.ToSKSamplingOptions()
                        );
                    });
                }
            }

#if !MINIMAL_IMAGESHARP_COMPAT
            public SKBitmap CloneQuantized(int maxColors)
            {
                return bitmap.ProcessPixelRows(accessor => AppViewLite.SkiaQuantizer.Quantize(accessor, maxColors));
            }
#endif

            public SKBitmap CloneResizedMax(Size maxSize, SKResizeQuality quality)
            {
                float scale = Math.Min(
                    (float)maxSize.Width / bitmap.Width,
                    (float)maxSize.Height / bitmap.Height);

                scale = Math.Min(scale, 1f);

                int newWidth = Math.Max(1, (int)Math.Round(bitmap.Width * scale));
                int newHeight = Math.Max(1, (int)Math.Round(bitmap.Height * scale));

                return bitmap.CloneResized(new Size(newWidth, newHeight), quality);
            }

            public SKBitmap CloneResized(Size size, SKResizeQuality quality)
            {
                var newWidth = size.Width;
                var newHeight = size.Height;

                if (newWidth == bitmap.Width && newHeight == bitmap.Height)
                    return bitmap.Clone();

                var resized = bitmap.CreateWithSize(newWidth, newHeight);

                resized.DrawBitmap(bitmap, new Rectangle(0, 0, newWidth, newHeight), quality);

                return resized;
            }
        }

        extension(Size size)
        {
            public SKRectI AsSKRectIAtZero => SKRectI.Create(0, 0, size.Width, size.Height);
            public SKSizeI AsSKSizeI => new SKSizeI(size.Width, size.Height);
        }

        extension(Rectangle rect)
        {
            public SKRectI AsSKRectI => new SKRectI(rect.Left, rect.Top, rect.Right, rect.Bottom);
            public SKRect AsSKRect => new SKRect(rect.Left, rect.Top, rect.Right, rect.Bottom);
        }
        extension(SKCodec codec)
        {
            public SKCodec CheckSucceeded() 
            {
                if (codec is null) throw new InvalidDataException("Image could not be parsed.");
                return codec;
            }
        }

        extension(SKCanvas canvas)
        {
            public void Fill(SKColor color)
            {
                canvas.Clear(color);
            }
            public void Fill(SKColor color, Rectangle rectangle)
            {
                using var paint = new SKPaint
                {
                    Color = color,
                    Style = SKPaintStyle.Fill
                };
                canvas.DrawRect(rectangle.ToSKRect(), paint);
            }
            
        }

        extension(Rectangle rectangle)
        {
            [Pure]
            public SKRect ToSKRect()
            {
                return new SKRect(rectangle.Left, rectangle.Top, rectangle.Right, rectangle.Bottom);
            }
        }


        extension(SKColor color)
        {
            public byte A => color.Alpha;
            public byte R => color.Red;
            public byte G => color.Green;
            public byte B => color.Blue;

            public System.Drawing.Color AsDrawingColor => System.Drawing.Color.FromArgb(color.Alpha, color.Red, color.Green, color.Blue);

            [Pure]
            public SKColor PremultiplyAlpha()
            {
                byte a = color.A;
                return a switch
                {
                    255 => color,
                    0 => default,
                    _ => new SKColor(
                        (byte)((color.R * a + 127) / 255),
                        (byte)((color.G * a + 127) / 255),
                        (byte)((color.B * a + 127) / 255),
                        a)
                };
            }
            [Pure]
            public SKColor UnpremultiplyAlpha()
            {
                var a = color.A;
                return a switch
                {
                    255 => color,
                    0 => default,
                    _ => new SKColor(
                        (byte)Math.Min(255, (color.R * 255 + a / 2) / a),
                        (byte)Math.Min(255, (color.G * 255 + a / 2) / a),
                        (byte)Math.Min(255, (color.B * 255 + a / 2) / a), 
                        a)
                };
            }
            [Pure]
            public string ToHex() => ((uint)(color.A | (color.B << 8) | (color.G << 16) | (color.R << 24))).ToString("X8", CultureInfo.InvariantCulture);

        }

#if false
        extension(SixLabors.ImageSharp.PixelFormats.Rgba32 color)
        {
            public System.Drawing.Color AsDrawingColor => System.Drawing.Color.FromArgb(color.A, color.R, color.G, color.B);
        }
#endif

        extension(System.Drawing.Color color)
        {
            public SKColor AsSkiaColor => new SKColor(color.R, color.G, color.B, color.A);

            [Pure]
            public System.Drawing.Color PremultiplyAlpha()
            {
                byte a = color.A;
                return a switch
                {
                    255 => color,
                    0 => default,
                    _ => Color.FromArgb(
                        a,
                        (byte)((color.R * a + 127) / 255),
                        (byte)((color.G * a + 127) / 255),
                        (byte)((color.B * a + 127) / 255))
                };
            }

            [Pure]
            public System.Drawing.Color UnpremultiplyAlpha()
            {
                var a = color.A;
                return a switch
                {
                    255 => color,
                    0 => default,
                    _ => Color.FromArgb(
                        color.A,
                        (byte)Math.Min(255, (color.R * 255 + a / 2) / a),
                        (byte)Math.Min(255, (color.G * 255 + a / 2) / a),
                        (byte)Math.Min(255, (color.B * 255 + a / 2) / a))
                };
            }

            [Pure]
            public string ToHex() => ((uint)(color.A | (color.B << 8) | (color.G << 16) | (color.R << 24))).ToString("X8", CultureInfo.InvariantCulture);

        }
    }

    internal unsafe class SKBitmapRowMemoryManager : MemoryManager<SKColor>
    {
        private readonly SKBitmap bitmap;
        private readonly SKColor* pointer;
        private readonly int length;

        public SKBitmapRowMemoryManager(SKBitmap bitmap, int row)
        {
            this.bitmap = bitmap ?? throw new ArgumentNullException(nameof(bitmap));

            if (bitmap.Info.ColorType != ImageSharpCompat.BlittableSKColorType ||
                bitmap.Info.BitsPerPixel != 32 ||
                !BitConverter.IsLittleEndian)
            {
                throw new NotSupportedException();
            }

            if ((uint)row >= (uint)bitmap.Height)
                throw new ArgumentOutOfRangeException(nameof(row));

            var pixels = (byte*)bitmap.GetPixels(out _);
            pointer = (SKColor*)(pixels + row * bitmap.RowBytes);
            length = bitmap.Width;
        }

        public override Span<SKColor> GetSpan()
        {
            if (bitmap.Handle == default) throw new ObjectDisposedException(nameof(SKBitmap));
            return new Span<SKColor>(pointer, length);
        }

        public override MemoryHandle Pin(int elementIndex = 0)
        {
            if ((uint)elementIndex > (uint)length)
                throw new ArgumentOutOfRangeException(nameof(elementIndex));
            return new MemoryHandle(pointer + elementIndex);
        }

        public override void Unpin()
        {
        }

        protected override void Dispose(bool disposing)
        {
            // Do not dispose bitmap.
        }

    }

    public readonly struct PixelAccessor
    {
        public readonly int Width => width;

        public readonly int Height => height;
        private readonly byte* pixels;
        private readonly int pixelLength;
        private readonly int width;
        private readonly int height;

        // SAFETY: Callers must GC.KeepAlive the bitmap until work with the PixelAccessor is done.
        public unsafe PixelAccessor(SKBitmap bitmap)
        {
            var info = bitmap.Info;
            if (info.ColorType != ImageSharpCompat.BlittableSKColorType ||
                info.BitsPerPixel != 32 ||
                !BitConverter.IsLittleEndian)
            {
                throw new NotSupportedException();
            }
            this.pixels = (byte*)bitmap.GetPixels();
            var rowBytes = info.RowBytes;
            this.pixelLength = rowBytes * info.Height;
            this.width = info.Width;
            this.height = info.Height;
            if (width * 4 != rowBytes) throw new NotSupportedException();
        }

        public unsafe Span<SKColor> GetRowSpan(int row)
        {
            if ((uint)row >= (uint)height)
                throw new ArgumentOutOfRangeException();

            var pointer = (SKColor*)(pixels + row * width * 4);
            return new Span<SKColor>(pointer, width);
        }

        public unsafe ref SKColor this[int x, int y]
        {
            get
            {
                var pixelIndex = y * width + x;
                if ((uint)pixelIndex >= (uint)pixelLength) throw new IndexOutOfRangeException();
                return ref Unsafe.AsRef<SKColor>((SKColor*)pixels + pixelIndex);
            }
        }
    }



    public class SKReplaceableBitmap : IDisposable
    {
        private SKBitmap bitmap;
        public SKBitmap Bitmap => bitmap;
        public SKImageInfo Info => bitmap.Info;
        public SKReplaceableBitmap(SKBitmap bitmap)
        {
            this.bitmap = bitmap;
        }
        public void Mutate(Action<SKCanvas> action)
        {
            bitmap.Mutate(action);
        }
        public void Dispose()
        {
            bitmap.Dispose();
        }

        public void ReplaceWith(SKBitmap updated)
        {
            ArgumentNullException.ThrowIfNull(updated);
            if (this.bitmap == updated) return;
            bitmap.Dispose();
            this.bitmap = updated;
        }

        public void MutateResizedMax(Size maxSize, SKResizeQuality quality)
        {
            if (bitmap.Width <= maxSize.Width && bitmap.Height <= maxSize.Height) return;
            ReplaceWith(bitmap.CloneResizedMax(maxSize, quality));
        }

        public void MutatePad(Size size, SKColor borderColor)
        {
            ReplaceWith(bitmap.ClonePadded(size, borderColor));
        }

        public void ReplaceWithCanvas(Action<SKBitmap, SKCanvas> update)
        {
            ReplaceWith((src, dest) =>
            {
                dest.Mutate(canvas => update(src, canvas));
            });
        }
        public void ReplaceWithCanvas(Size size, Action<SKBitmap, SKCanvas> update)
        {
            ReplaceWith(size, (src, dest) =>
            {
                dest.Mutate(canvas => update(src, canvas));
            });
        }
        public void ReplaceWith(Action<SKBitmap, SKBitmap> update)
        {
            ReplaceWith(new Size(this.bitmap.Width, this.bitmap.Height), update);
        }
        public void ReplaceWith(Size newSize, Action<SKBitmap, SKBitmap> update)
        {
            var info = this.bitmap.Info;
            info.Width = newSize.Width;
            info.Height = newSize.Height;
            var updated = new SKBitmap(info);
            update(this.bitmap, updated);
            ReplaceWith(updated);
        }

        public static explicit operator SKReplaceableBitmap(SKBitmap bitmap)
        {
            return new SKReplaceableBitmap(bitmap);
        }
        public static explicit operator SKBitmap(SKReplaceableBitmap bitmap)
        {
            return bitmap.bitmap;
        }
        public int Width => bitmap.Width;
        public int Height => bitmap.Height;


        public void SaveAsPng(string destination) => bitmap.SaveAsPng(destination);
        public void SaveAsJpeg(string destination, int quality = ImageSharpCompat.DefaultJpegQuality) => bitmap.SaveAsJpeg(destination, quality);
        //public void SaveAsJpegXl(string destination, int quality = ImageSharpCompat.DefaultJpegXlQuality) => bitmap.SaveAsJpegXl(destination, quality);
        public void SaveAsWebp(string destination, int quality = ImageSharpCompat.DefaultWebpQuality) => bitmap.SaveAsWebp(destination, quality);

        public void SaveAsPng(System.IO.Stream destination) => bitmap.SaveAsPng(destination);
        public void SaveAsJpeg(System.IO.Stream destination, int quality = ImageSharpCompat.DefaultJpegQuality) => bitmap.SaveAsJpeg(destination, quality);
        //public void SaveAsJpegXl(System.IO.Stream destination, int quality = ImageSharpCompat.DefaultJpegXlQuality) => bitmap.SaveAsJpegXl(destination, quality);
        public void SaveAsWebp(System.IO.Stream destination, int quality = ImageSharpCompat.DefaultWebpQuality) => bitmap.SaveAsWebp(destination, quality);
        [Pure]
        public void ApplyExifRotation(SKEncodedOrigin orientation)
        {
            // https://github.com/mono/SkiaSharp/issues/836#issuecomment-584895517

            var useWidth = bitmap.Width;
            var useHeight = bitmap.Height;
            Action<SKCanvas> transform = canvas => { };
            switch (orientation)
            {
                case SKEncodedOrigin.TopLeft:
                    return;
                case SKEncodedOrigin.TopRight:
                    // horizontal flip
                    transform = canvas => canvas.Scale(-1, 1, useWidth / 2, useHeight / 2);
                    break;
                case SKEncodedOrigin.BottomRight:
                    transform = canvas => canvas.RotateDegrees(180, useWidth / 2, useHeight / 2);
                    break;
                case SKEncodedOrigin.BottomLeft:
                    // vertical flip
                    transform = canvas => canvas.Scale(1, -1, useWidth / 2, useHeight / 2);
                    break;
                case SKEncodedOrigin.LeftTop:
                    useWidth = bitmap.Height;
                    useHeight = bitmap.Width;
                    transform = canvas =>
                    {
                        // Rotate 90
                        canvas.RotateDegrees(90, useWidth / 2, useHeight / 2);
                        canvas.Scale(useHeight * 1.0f / useWidth, -useWidth * 1.0f / useHeight, useWidth / 2, useHeight / 2);
                    };
                    break;
                case SKEncodedOrigin.RightTop:
                    useWidth = bitmap.Height;
                    useHeight = bitmap.Width;
                    transform = canvas =>
                    {
                        // Rotate 90
                        canvas.RotateDegrees(90, useWidth / 2, useHeight / 2);
                        canvas.Scale(useHeight * 1.0f / useWidth, useWidth * 1.0f / useHeight, useWidth / 2, useHeight / 2);
                    };
                    break;
                case SKEncodedOrigin.RightBottom:
                    useWidth = bitmap.Height;
                    useHeight = bitmap.Width;
                    transform = canvas =>
                    {
                        // Rotate 90
                        canvas.RotateDegrees(90, useWidth / 2, useHeight / 2);
                        canvas.Scale(-useHeight * 1.0f / useWidth, useWidth * 1.0f / useHeight, useWidth / 2, useHeight / 2);
                    };
                    break;
                case SKEncodedOrigin.LeftBottom:
                    useWidth = bitmap.Height;
                    useHeight = bitmap.Width;
                    transform = canvas =>
                    {
                        // Rotate 90
                        canvas.RotateDegrees(90, useWidth / 2, useHeight / 2);
                        canvas.Scale(-useHeight * 1.0f / useWidth, -useWidth * 1.0f / useHeight, useWidth / 2, useHeight / 2);
                    };
                    break;
                default:
                    throw new ArgumentException("Unknown SKEncodedOrigin value: " + orientation);
            }

            var finalSize = new Size(useWidth, useHeight);
            
            ReplaceWithCanvas(finalSize, (_, canvas) =>
            {
                using (var paint = new SKPaint())
                {
                    transform(canvas);
                    canvas.DrawBitmap(bitmap, finalSize.AsSKRectIAtZero, SKSamplingOptions.NearestNeighbor, paint);
                }
            });

        }



    }
    public record struct SKCodecInfoStruct
    {
        public int FrameCount;
        public SKEncodedOrigin EncodedOrigin;
        public SKEncodedImageFormat EncodedFormat;
    }

    public enum SKResizeQuality
    { 
        NearestNeighbor,
        Medium,
        High,
    }


}

