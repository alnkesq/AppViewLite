using AppViewLite.Models;
using Ipfs;
using Microsoft.AspNetCore.Mvc;
using SixLabors.ImageSharp;
using SixLabors.ImageSharp.Processing;
using System.Buffers;

namespace AppViewLite.Web.Controllers
{
    [Route("/img")]
    [ApiController]
    public class MediaController : ControllerBase
    {
        private readonly BlueskyEnrichedApis apis;

        public MediaController(BlueskyEnrichedApis apis)
        {
            this.apis = apis;
        }

        private readonly static SearchValues<char> CidChars = SearchValues.Create("0123456789abcdefghijklmnopqrstuvwxyz");
        private readonly static bool Enabled = 
            AppViewLiteConfiguration.GetBool(AppViewLiteParameter.APPVIEWLITE_SERVE_IMAGES) ??
            AppViewLiteConfiguration.GetString(AppViewLiteParameter.APPVIEWLITE_CDN) == null;
        private readonly static bool CacheAvatars = AppViewLiteConfiguration.GetBool(AppViewLiteParameter.APPVIEWLITE_CACHE_AVATARS) ?? true;
        private readonly static bool CacheFeedThumbs = AppViewLiteConfiguration.GetBool(AppViewLiteParameter.APPVIEWLITE_CACHE_FEED_THUMBS) ?? false;

        [Route("{size}/plain/{did}/{cid}@jpeg")]
        [HttpGet]
        public async Task GetThumbnail(string size, string did, string cid, [FromQuery] string? pds)
        {
            if (!Enabled) throw new Exception("Image serving is not enabled on this server.");
            var sizeEnum = Enum.Parse<ThumbnailSize>(size);

            BlueskyEnrichedApis.EnsureValidDid(did);

            var pluggable = BlueskyRelationships.TryGetPluggableProtocolForDid(did);

            if (pluggable == null)
            {
                if (cid.Length != 59) throw new Exception("Invalid CID length.");
                if (cid.AsSpan().ContainsAnyExcept(CidChars)) throw new Exception("CID contains invalid characters.");
            }

            var sizePixels = sizeEnum switch
            {
                ThumbnailSize.feed_thumbnail => 1000,
                ThumbnailSize.feed_fullsize => 2000,
                ThumbnailSize.avatar => 1000,
                ThumbnailSize.avatar_thumbnail => 150,
                ThumbnailSize.banner => 1000,
                _ => throw new ArgumentException("Unrecognized image size.")
            };

            var storeToDisk =
                size == "avatar_thumbnail" ? CacheAvatars :
                size is "feed_thumbnail" or "banner" ? CacheFeedThumbs : 
                false;

            if (storeToDisk)
            {
                ReadOnlySpan<char> shortDid;
                const int PAYLOAD_CUT = 3;
                int shortDidCut;
                if (did.StartsWith("did:plc:", StringComparison.Ordinal))
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
                var cachePath = Path.Combine(cacheDirectory, size, escapedPart1, filename);


                if (!System.IO.File.Exists(cachePath))
                {
                    using Image image = await GetImageAsync(did, cid, pds, sizePixels, sizeEnum);

                    Directory.CreateDirectory(Path.GetDirectoryName(cachePath)!);
                    try
                    {
                        await image.SaveAsJpegAsync(cachePath + ".tmp", new SixLabors.ImageSharp.Formats.Jpeg.JpegEncoder { Quality = 70 });
                        System.IO.File.Move(cachePath + ".tmp", cachePath);
                    }
                    catch when (System.IO.File.Exists(cachePath)) 
                    { 
                        // concurrent request already saved this file, this is ok.
                    }
                }

                SetMediaHeaders(cid);

                using var stream = new FileStream(cachePath, new FileStreamOptions 
                { 
                    Mode = FileMode.Open,
                    Share = FileShare.Read | FileShare.Delete,
                    Access = FileAccess.Read,
                    Options = FileOptions.Asynchronous
                });
                await stream.CopyToAsync(Response.Body);

            }
            else
            {
                using Image image = await GetImageAsync(did, cid, pds, sizePixels, sizeEnum);
                SetMediaHeaders(cid);
                await image.SaveAsJpegAsync(Response.Body, new SixLabors.ImageSharp.Formats.Jpeg.JpegEncoder { Quality = 80 });
            }
        }

        private static string EscapeDidForFileSystem(ReadOnlySpan<char> did)
        {
            return did.ToString()
                .Replace("_", "__")
                .Replace(':', '_')
                .Replace('.', ',') /* avoids CON.com_etcetera issue on windows */;
        }

        private async Task<Image> GetImageAsync(string did, string cid, string? pds, int sizePixels, ThumbnailSize sizeEnum)
        {
            var bytes = await apis.GetBlobAsync(did, cid, pds, sizeEnum);
            if (!StartsWithAllowlistedMagicNumber(bytes)) throw new Exception("Unrecognized image format.");
            var image = SixLabors.ImageSharp.Image.Load(bytes);

            if (Math.Max(image.Width, image.Height) > sizePixels)
            {
                image.Mutate(m => m.Resize(new ResizeOptions { Mode = ResizeMode.Max, Size = new SixLabors.ImageSharp.Size(sizePixels, sizePixels) }));
            }
            return image;
        }

        private void SetMediaHeaders(string cid)
        {
            Response.ContentType = "image/jpeg";
            Response.Headers.CacheControl = new(["public, max-age=31536000, immutable"]);
            Response.Headers.Pragma = new(["cache"]);
            Response.Headers.ETag = new(["permanent"]);
            Response.Headers.Expires = new(["Fri, 31 Dec 9999 23:59:59 GMT"]);
            Response.Headers.ContentDisposition = new(["inline; filename=\"" + cid + ".jpg\""]);
        }

        private static ReadOnlySpan<byte> Magic_JPG => [0xff, 0xd8, 0xff];
        private static ReadOnlySpan<byte> Magic_PNG => [0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a];
        private static ReadOnlySpan<byte> Magic_RIFF => "RIFF"u8;
        private static ReadOnlySpan<byte> Magic_WEBP => [0x57, 0x45, 0x42, 0x50, 0x56, 0x50, 0x38];
        private static ReadOnlySpan<byte> Magic_GIF87a => "GIF87a"u8;
        private static ReadOnlySpan<byte> Magic_GIF89a => "GIF89a"u8;
        private static bool StartsWithAllowlistedMagicNumber(ReadOnlySpan<byte> bytes)
        {
            return 
                bytes.StartsWith(Magic_JPG) ||
                bytes.StartsWith(Magic_PNG) ||
                bytes.StartsWith(Magic_WEBP) ||
                bytes.StartsWith(Magic_GIF87a) ||
                bytes.StartsWith(Magic_GIF89a) ||
                (bytes.StartsWith(Magic_RIFF) && bytes.Slice(8).StartsWith(Magic_WEBP))
                ;
        }
    }
}

