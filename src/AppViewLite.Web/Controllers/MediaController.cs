using AppViewLite.Models;
using Ipfs;
using Microsoft.AspNetCore.Mvc;
using SixLabors.ImageSharp;
using SixLabors.ImageSharp.PixelFormats;
using SixLabors.ImageSharp.Processing;
using SixLabors.ImageSharp.Processing.Processors.Transforms;
using System;
using System.Buffers;
using System.Net.Http.Headers;
using System.Runtime.InteropServices;

namespace AppViewLite.Web.Controllers
{
    [ApiController]
    public class MediaController : ControllerBase
    {
        private readonly BlueskyEnrichedApis apis;
        private readonly RequestContext ctx;
        public MediaController(BlueskyEnrichedApis apis, RequestContext ctx)
        {
            this.apis = apis;
            this.ctx = ctx;
        }

        public AdministrativeBlocklist AdministrativeBlocklist => apis.AdministrativeBlocklist;

        private readonly static SearchValues<char> CidChars = SearchValues.Create("0123456789abcdefghijklmnopqrstuvwxyz");
        private readonly static bool Enabled = 
            AppViewLiteConfiguration.GetBool(AppViewLiteParameter.APPVIEWLITE_SERVE_IMAGES) ??
            AppViewLiteConfiguration.GetString(AppViewLiteParameter.APPVIEWLITE_CDN) == null;
        private readonly static bool CacheAvatars = AppViewLiteConfiguration.GetBool(AppViewLiteParameter.APPVIEWLITE_CACHE_AVATARS) ?? true;
        private readonly static bool CacheFeedThumbs = AppViewLiteConfiguration.GetBool(AppViewLiteParameter.APPVIEWLITE_CACHE_FEED_THUMBS) ?? false;


        [Route("/watch/{encodedDid}/{cid}/{format}")]
        [HttpGet]
        public Task GetVideo(string format, string encodedDid, string cid, [FromQuery] string? pds, [FromQuery] string? name, CancellationToken ct)
        {

            var size = format switch
            {
                "thumbnail.jpg" => ThumbnailSize.video_thumbnail,
                "video.mp4" => ThumbnailSize.feed_video_blob,
                "playlist.m3u8" => ThumbnailSize.feed_video_playlist,
                _ => throw new ArgumentException(),
            };
            return GetThumbnail(size.ToString(), Uri.UnescapeDataString(encodedDid), cid, pds, name, ct);

        }

        [Route("/img/{size}/plain/{did}/{cid}@jpeg")]
        [HttpGet]
        public async Task GetThumbnail(string size, string did, string cid, [FromQuery] string? pds, [FromQuery] string? name, CancellationToken ct)
        {
            if (!Enabled) throw new Exception("Image serving is not enabled on this server.");
            var sizeEnum = Enum.Parse<ThumbnailSize>(size);

            void InitFileName(string? fallback = null)
            {
                name ??= fallback ?? (cid + (IsVideo(sizeEnum) ? ".mp4" : ".jpg"));
            }
            

            AdministrativeBlocklist.ThrowIfBlockedOutboundConnection(DidDocProto.GetDomainFromPds(pds));

            var isRawUrl = did.StartsWith("host:", StringComparison.Ordinal);
            if (isRawUrl)
            {
                AdministrativeBlocklist.ThrowIfBlockedOutboundConnection(did.Substring(5));
            }
            else
            {
                BlueskyEnrichedApis.EnsureValidDid(did);

                AdministrativeBlocklist.ThrowIfBlockedOutboundConnection(did);

                var pluggable = BlueskyRelationships.TryGetPluggableProtocolForDid(did);

                if (pluggable == null)
                {
                    if (cid.Length != 59) throw new Exception("Invalid CID length.");
                    if (cid.AsSpan().ContainsAnyExcept(CidChars)) throw new Exception("CID contains invalid characters.");
                }
            }

            var sizePixels = sizeEnum switch
            {
                ThumbnailSize.feed_thumbnail or ThumbnailSize.video_thumbnail => 1000,
                ThumbnailSize.feed_fullsize => 2000,
                ThumbnailSize.avatar => 1000,
                ThumbnailSize.avatar_thumbnail => 150,
                ThumbnailSize.banner => 1000,
                ThumbnailSize.emoji or ThumbnailSize.emoji_profile_name => 64,
                _ when IsVideo(sizeEnum) => -1,
                _ => throw new ArgumentException("Unrecognized image size.")
            };

            var storeToDisk =
                sizeEnum is ThumbnailSize.emoji or ThumbnailSize.emoji_profile_name ? true :
                sizeEnum is ThumbnailSize.avatar_thumbnail ? CacheAvatars :
                sizeEnum is ThumbnailSize.feed_thumbnail or ThumbnailSize.banner or ThumbnailSize.video_thumbnail ? CacheFeedThumbs : 
                false;


            if (storeToDisk)
            {
                ReadOnlySpan<char> shortDid;
                const int PAYLOAD_CUT = 3;
                int shortDidCut;
                if (isRawUrl)
                {
                    // host:example.com -> host:exa/mple.com
                    shortDid = did;
                    shortDidCut = 8;
                }
                else if (did.StartsWith("did:plc:", StringComparison.Ordinal))
                {
                    // did:plc:aaaaaaaaaa -> aaa/aaaaaaa
                    shortDid = did.AsSpan(8);
                    shortDidCut = PAYLOAD_CUT;
                }
                else
                {
                    // did:other:aaaaaaaaaa -> did:other:aaa/aaaaaaa
                    shortDid = did;
                    var secondColon = did.IndexOf(':', 4);
                    shortDidCut = secondColon + 1 + PAYLOAD_CUT;
                    if (did.StartsWith("did:nostr:npub", StringComparison.Ordinal))
                        shortDidCut += 5; // "npub1" is constant
                }

                shortDidCut = Math.Min(shortDidCut, shortDid.Length);

                if (shortDid.ContainsAny('/', '\\')) throw new Exception();
                var escapedPart1 = EscapeDidForFileSystem(shortDid.Slice(0, shortDidCut));
                var escapedPart2 = EscapeDidForFileSystem(shortDid.Slice(shortDidCut));


                var cacheDirectory = AppViewLiteConfiguration.GetString(AppViewLiteParameter.APPVIEWLITE_IMAGE_CACHE_DIRECTORY) ?? apis.DangerousUnlockedRelationships.BaseDirectory + "/image-cache";
                var ext = ".jpg";
                var filename = escapedPart2 + "_" + cid + ext;
                if (filename.Length + 4 >= 255)
                    filename = escapedPart2 + "_-" + Base32.ToBase32(System.Security.Cryptography.SHA256.HashData(Base32.FromBase32(cid))) + ext;
                var cachePath = Path.Combine(cacheDirectory, sizeEnum.ToString(), escapedPart1, filename);


                if (!System.IO.File.Exists(cachePath))
                {
                    if (IsVideo(sizeEnum))
                        throw new NotSupportedException("Caching of videos to disk is not currently supported.");

                    var imageResult = await GetImageAsync(did, cid, pds, sizePixels, sizeEnum, ct);
                    using var image = imageResult.Image;
                    
                    // TODO: if caching is enabled, we lose we don't serve the original content-disposition file name

                    Directory.CreateDirectory(Path.GetDirectoryName(cachePath)!);
                    try
                    {
                        using (var cacheStream = new System.IO.FileStream(cachePath + ".tmp", FileMode.Create, FileAccess.Write))
                        {
                            await WriteImageOrBytesAsync(image, imageResult.SvgData, cacheStream, ct);
                        }
                        System.IO.File.Move(cachePath + ".tmp", cachePath);
                    }
                    catch when (System.IO.File.Exists(cachePath)) 
                    { 
                        // concurrent request already saved this file, this is ok.
                    }
                }


                using var stream = new FileStream(cachePath, new FileStreamOptions 
                { 
                    Mode = FileMode.Open,
                    Share = FileShare.Read | FileShare.Delete,
                    Access = FileAccess.Read,
                    Options = FileOptions.Asynchronous
                });

                Memory<byte> initialBytes = new byte[256];
                initialBytes = initialBytes.Slice(0, await stream.ReadAsync(initialBytes, ct));
                stream.Seek(0, SeekOrigin.Begin);

                InitFileName();
                SetMediaHeaders(name, initialBytes: initialBytes);
                Response.ContentLength = stream.Length;
                await stream.CopyToAsync(Response.Body, ct);

            }
            else
            {
                if (sizeEnum == ThumbnailSize.feed_video_blob)
                {
                    using var blob = await apis.GetBlobAsync(did, cid, pds, sizeEnum, ctx, ct);
                    InitFileName(blob.FileNameForDownload);
                    SetMediaHeaders(name, "video/mp4");
                    if (blob.Bytes != null)
                    {
                        Response.ContentLength = blob.Bytes.Length;
                        await Response.Body.WriteAsync(blob.Bytes, ct);
                    }
                    else
                    {

                        using (blob.Stream)
                        {
                            await blob.Stream!.CopyToAsync(Response.Body, ct);
                        }
                        
                    }
                    
                }
                else if(sizeEnum == ThumbnailSize.feed_video_playlist)
                {
                    throw new NotSupportedException("Proxying of HLS streams is not currently supported.");
                }
                else
                {
                    var imageResult = await GetImageAsync(did, cid, pds, sizePixels, sizeEnum, ct);
                    using var image = imageResult.Image;
                    InitFileName(imageResult.FileNameForDownload);
                    SetMediaHeaders(name, initialBytes: imageResult.SvgData);
                    await WriteImageOrBytesAsync(image, imageResult.SvgData, Response.Body, ct);
                }
            }
        }

        private static bool IsVideo(ThumbnailSize size) => size is ThumbnailSize.feed_video_blob or ThumbnailSize.feed_video_playlist;
        private static async Task WriteImageOrBytesAsync(Image<Rgba32>? image, byte[]? bytes, Stream cacheStream, CancellationToken ct)
        {
            if (bytes != null)
            {
                cacheStream.Write(bytes);
            }
            else
            {
                using (image)
                {
                    await image.SaveAsWebpAsync(cacheStream, new SixLabors.ImageSharp.Formats.Webp.WebpEncoder
                    {
                        Quality =
                        image!.Width <= 16 ? 98 :
                        image.Width <= 32 ? 90 :
                        70
                    }, cancellationToken: ct);
                }
            }
        }

        private static string EscapeDidForFileSystem(ReadOnlySpan<char> did)
        {
            return did.ToString()
                .Replace("_", "__")
                .Replace(':', '_')
                .Replace('.', ',') /* avoids CON.com_etcetera issue on windows */;
        }

        private async Task<(Image<Rgba32>? Image, byte[]? SvgData, string? FileNameForDownload)> GetImageAsync(string did, string cid, string? pds, int sizePixels, ThumbnailSize sizeEnum, CancellationToken ct)
        {
            try
            {
                using var blob = await apis.GetBlobAsync(did, cid, pds, sizeEnum, ctx, ct);

                var bytes = await blob.ReadAsBytesAsync();

                if (bytes.AsSpan().StartsWith("<?xml "u8) && bytes.AsSpan().IndexOf("xmlns=\"http://www.w3.org/2000/svg\""u8) != -1)
                {
                    return (null, bytes, blob.FileNameForDownload);
                }

                if (!StartsWithAllowlistedMagicNumber(bytes)) throw new UnexpectedFirehoseDataException("Unrecognized image format.");
                Image<Rgba32> image;
                if (bytes.AsSpan().StartsWith(Magic_ICO))
                    image = IconParser.IconUtils.LoadLargestImage(bytes);
                else
                    image = SixLabors.ImageSharp.Image.Load<Rgba32>(bytes);

                if (image.Frames.Count > 1) return (image, null, blob.FileNameForDownload);

                if (Math.Max(image.Width, image.Height) > sizePixels)
                {
                    image.Mutate(m => m.Resize(new ResizeOptions { Mode = ResizeMode.Max, Size = new SixLabors.ImageSharp.Size(sizePixels, sizePixels) }));
                }

                if ((image.Width <= 60) && sizeEnum == ThumbnailSize.avatar_thumbnail)
                {
                    var borderColor = GetBorderAverageColor(image);
                    image.Mutate(m =>
                    {
                        var size = Math.Min(image.Width, image.Height) + 16;
                        m.Pad(size, size, borderColor);
                    });
                }

                if (sizeEnum == ThumbnailSize.emoji_profile_name)
                {
                    EnsureNotConfusableWithVerifiedBadge(ref image);
                }

                if (sizeEnum == ThumbnailSize.avatar_thumbnail)
                {
                    image.Mutate(m =>
                    {
                        // Workaround for https://github.com/alnkesq/AppViewLite/issues/87
                        m.BackgroundColor(Color.White);
                    });
                }



                var other = new Image<Rgba32>(image.Width, image.Height);
                using (image)
                {
                    other.Mutate(m => m.DrawImage(image, new Point(0, 0), 1));

                }

                return (other, null, blob.FileNameForDownload);
            }
            catch (Exception) when (sizeEnum == ThumbnailSize.avatar_thumbnail && did.StartsWith(AppViewLite.PluggableProtocols.Rss.RssProtocol.DidPrefix, StringComparison.Ordinal))
            {
                return (null, DefaultRssIconSvg, null);
            }
        }

        private static int Pow2(int a) => a * a;
        public static double ColorDistance(Rgba32 a, Rgba32 b)
        {
            var diff = Math.Sqrt(Pow2(a.R - b.R) + Pow2(a.G - b.G) + Pow2(a.B - b.B));
            return diff;
        }
        private static Color GetBorderAverageColor(Image<Rgba32> image)
        {
            Color borderColor = default;
            image.ProcessPixelRows(accessor =>
            {
                var r = 0;
                var g = 0;
                var b = 0;
                var a = 0;
                int borderCount = 0;
                var frequency = new Dictionary<Rgba32, int>();
                void Accumulate(Rgba32 pixel)
                {
                    r += pixel.R;
                    g += pixel.G;
                    b += pixel.B;
                    a += pixel.A;
                    CollectionsMarshal.GetValueRefOrAddDefault(frequency, pixel, out _)++;
                    borderCount++;
                }
                foreach (var pixel in accessor.GetRowSpan(0))
                {
                    Accumulate(pixel);
                }
                foreach (var pixel in accessor.GetRowSpan(accessor.Height - 1))
                {
                    Accumulate(pixel);
                }
                for (int i = accessor.Height - 2; i >= 1; i--)
                {
                    Accumulate(image[0, i]);
                    Accumulate(image[accessor.Width - 1, i]);
                }
                var borderCountFloat = (float)borderCount * 255;
                var average = new Rgba32(r / borderCountFloat, g / borderCountFloat, b / borderCountFloat, a / borderCountFloat);
                var mostFrequent = frequency.MaxBy(x => x.Value).Key;
                if (ColorDistance(average, mostFrequent) < 50) borderColor = mostFrequent;
                else borderColor = average;
            });
            return borderColor;
        }

        private readonly static Rgba32 Color_VerifiedGeneric = new Rgba32(0x1D, 0xA1, 0xF2);
        private readonly static Rgba32 Color_VerifiedOrganization = new Rgba32(0xE2, 0xB7, 0x19);
        private readonly static Rgba32 Color_VerifiedGovernment = new Rgba32(0x82, 0x9A, 0xAB);
        private const double VerifiedBadgeThreshold = 0.9;
        private const int ColorComponentDeltaThreshold = 30;
        private static bool AreColorsSimilar(Rgba32 a, Rgba32 b)
        {
            var deltaR = Math.Abs((int)a.R - b.R);
            var deltaG = Math.Abs((int)a.G - b.G);
            var deltaB = Math.Abs((int)a.B - b.B);
            return
                deltaR < ColorComponentDeltaThreshold &&
                deltaG < ColorComponentDeltaThreshold &&
                deltaB < ColorComponentDeltaThreshold;
        }
        private static Rgba32 BlendWithWhiteBackground(Rgba32 color)
        {
            byte alpha = color.A;

            if (alpha == 255) return new Rgba32(color.R, color.G, color.B, 255);
            if (alpha == 0) return new Rgba32(255, 255, 255, 255);

            float alphaFactor = alpha / 255f;

            byte r = (byte)(color.R * alphaFactor + 255 * (1 - alphaFactor));
            byte g = (byte)(color.G * alphaFactor + 255 * (1 - alphaFactor));
            byte b = (byte)(color.B * alphaFactor + 255 * (1 - alphaFactor));

            return new Rgba32(r, g, b, 255);
        }
        public static void EnsureNotConfusableWithVerifiedBadge(ref Image<Rgba32> image)
        {
            using var small = image.Clone(m => m.Resize(new ResizeOptions 
            {
                Mode = ResizeMode.Max,
                Size = new SixLabors.ImageSharp.Size(48, 48),
                Sampler = new NearestNeighborResampler(),
            }));
            int pixelsTotal = 0;
            var pixelsVerifiedGeneric = 0;
            var pixelsVerifiedOrganization = 0;
            var pixelsVerifiedGovernment = 0;
            var pixelsWhite = 0;
            var pixelsNonWhite = 0;

            small.ProcessPixelRows(x =>
            {
                for (int row = 0; row < x.Height; row++)
                {
                    var rowSpan = x.GetRowSpan(row);
                    for (int col = 0; col < rowSpan.Length; col++)
                    {
                        var pixel = rowSpan[col];
                        Rgba32 debugColor;
                        if (pixel.A < 48)
                        {
                            debugColor = Color.Gray;
                        }
                        else
                        {
                            pixelsTotal++;
                            var opaque = BlendWithWhiteBackground(pixel);
                            

                            if (AreColorsSimilar(opaque, Color.White))
                            {

                                pixelsWhite++;
                                debugColor = Color.Red;
                            }
                            else
                            {
                                pixelsNonWhite++;

                                if (AreColorsSimilar(opaque, Color_VerifiedGeneric))
                                {
                                    pixelsVerifiedGeneric++;
                                    debugColor = Color.Green;
                                }
                                else if (AreColorsSimilar(opaque, Color_VerifiedOrganization))
                                {
                                    pixelsVerifiedOrganization++;
                                    debugColor = Color.Blue;
                                }
                                else if (AreColorsSimilar(opaque, Color_VerifiedGovernment))
                                {
                                    pixelsVerifiedGovernment++;
                                    debugColor = Color.DeepPink;
                                }
                                else
                                {
                                    debugColor = opaque;
                                }
                            }
                        }

                        //small[col, row] = debugColor;
                    }

                }
            });

            if (pixelsNonWhite < 10) return;

            // 0.087 for verified badge
            var whitePixelsRatio = (float)pixelsWhite / pixelsTotal; 

            // 0.996 for verified badge
            var verifiedGenericRatio = (float)pixelsVerifiedGeneric / pixelsNonWhite;
            var verifiedOrganizationRatio = (float)pixelsVerifiedOrganization / pixelsNonWhite;
            var verifiedGovernmentRatio = (float)pixelsVerifiedGovernment / pixelsNonWhite;

            if (verifiedGenericRatio > VerifiedBadgeThreshold ||
                verifiedOrganizationRatio > VerifiedBadgeThreshold)
            {
                image.Mutate(m => m.Saturate(0.3f).Opacity(0.3f));
                RemoveOldPalette(ref image);
            }
            else if (verifiedGovernmentRatio > VerifiedBadgeThreshold)
            {
                image.Mutate(m => m.Brightness(0.3f));
                RemoveOldPalette(ref image);
            }

        }

        private static void RemoveOldPalette(ref Image<Rgba32> image)
        {
            // https://github.com/SixLabors/ImageSharp/issues/2865
            var other = new Image<Rgba32>(image.Width, image.Height);
            var image_ = image;
            other.Mutate(m => m.DrawImage(image_, 1));
            image.Dispose();
            image = other;
        }

        private void SetMediaHeaders(string? nameForDownload, string contentType = "image/jpeg", ReadOnlyMemory<byte> initialBytes = default)
        {
            if (initialBytes.Span.StartsWith("<?xml "u8))
                contentType = "image/svg+xml";
            Response.ContentType = contentType;
            Response.Headers.CacheControl = new(["public, max-age=31536000, immutable"]);
            Response.Headers.Pragma = new(["cache"]);
            Response.Headers.ETag = new(["permanent"]);
            Response.Headers.Expires = new(["Fri, 31 Dec 9999 23:59:59 GMT"]);
            if (nameForDownload != null)
            {
                var contentDisposition = new ContentDispositionHeaderValue("inline")
                {
                    FileNameStar = Uri.EscapeDataString(nameForDownload),
                    
                };
                Response.Headers.ContentDisposition = contentDisposition.ToString();
            }
        }

        private static ReadOnlySpan<byte> Magic_JPG => [0xff, 0xd8, 0xff];
        private static ReadOnlySpan<byte> Magic_PNG => [0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a];
        private static ReadOnlySpan<byte> Magic_RIFF => "RIFF"u8;
        private static ReadOnlySpan<byte> Magic_WEBP => [0x57, 0x45, 0x42, 0x50, 0x56, 0x50, 0x38];
        private static ReadOnlySpan<byte> Magic_GIF87a => "GIF87a"u8;
        private static ReadOnlySpan<byte> Magic_GIF89a => "GIF89a"u8;
        private static ReadOnlySpan<byte> Magic_ICO => "\x00\x00\x01\x00"u8;
        private static bool StartsWithAllowlistedMagicNumber(ReadOnlySpan<byte> bytes)
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

        private readonly static byte[] DefaultRssIconSvg =
            """
            <?xml version="1.0" encoding="iso-8859-1"?>
            <svg version="1.1" xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" viewBox="-80 -80 615.731 615.731" xml:space="preserve">
            <g>
            <rect x="-80" y="-80" style="fill:#F78422;" width="615.731" height="615.731"/>
            <g>
            <path style="fill:#FFFFFF;" d="M296.208,159.16C234.445,97.397,152.266,63.382,64.81,63.382v64.348
            c70.268,0,136.288,27.321,185.898,76.931c49.609,49.61,76.931,115.63,76.931,185.898h64.348
            C391.986,303.103,357.971,220.923,296.208,159.16z"/>
            <path style="fill:#FFFFFF;" d="M64.143,172.273v64.348c84.881,0,153.938,69.056,153.938,153.939h64.348
            C282.429,270.196,184.507,172.273,64.143,172.273z"/>
            <circle style="fill:#FFFFFF;" cx="109.833" cy="346.26" r="46.088"/>
            </g>
            </g>
            </svg>
            """u8.ToArray();
    }
}

