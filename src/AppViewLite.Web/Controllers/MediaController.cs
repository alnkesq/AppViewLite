using Ipfs;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Mvc;
using SixLabors.ImageSharp;
using SixLabors.ImageSharp.PixelFormats;
using SixLabors.ImageSharp.Processing;
using System;
using System.Buffers;
using System.Net.Http.Headers;
using System.Text;

namespace AppViewLite.Web.Controllers
{
    [Route("/img")]
    [ApiController]
    public class MediaController : ControllerBase
    {
        
        

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

            BlueskyEnrichedApis.EnsureValidDid(did);

            if (cid.Length != 59) throw new Exception("Invalid CID length.");
            if (cid.AsSpan().ContainsAnyExcept(CidChars)) throw new Exception("CID contains invalid characters.");

            var sizePixels = size switch
            {
                "feed_thumbnail" => 1000,
                "feed_fullsize" => 2000,
                "avatar" => 1000,
                "avatar_thumbnail" => 150,
                "banner" => 1000,
                _ => throw new ArgumentException("Unrecognized image size.")
            };

            var storeToDisk =
                size == "avatar_thumbnail" ? CacheAvatars :
                size is "feed_thumbnail" or "banner" ? CacheFeedThumbs : 
                false;

            if (storeToDisk)
            {
                ReadOnlySpan<char> shortDid;
                int cut = 3;
                if (did.StartsWith("did:plc:", StringComparison.Ordinal))
                {
                    shortDid = did.AsSpan(8);
                }
                else
                {
                    shortDid = did.Replace(':', '_').AsSpan(4);
                    cut = 5;
                }


                var cacheDirectory = AppViewLiteConfiguration.GetString(AppViewLiteParameter.APPVIEWLITE_IMAGE_CACHE_DIRECTORY) ?? BlueskyEnrichedApis.Instance.DangerousUnlockedRelationships.BaseDirectory + "/image-cache";
                var cachePath = Path.Combine(cacheDirectory, size, shortDid.Slice(0, cut).ToString(), shortDid.Slice(cut).ToString() + "_" + cid + ".jpg");


                if (!System.IO.File.Exists(cachePath))
                {
                    using Image image = await GetImageAsync(did, cid, pds, sizePixels);

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
                using Image image = await GetImageAsync(did, cid, pds, sizePixels);
                SetMediaHeaders(cid);
                await image.SaveAsJpegAsync(Response.Body, new SixLabors.ImageSharp.Formats.Jpeg.JpegEncoder { Quality = 80 });
            }
        }

        private async Task<Image> GetImageAsync(string did, string cid, string? pds, int sizePixels)
        {
            var bytes = await BlueskyEnrichedApis.Instance.GetBlobAsBytesAsync(did, cid, pds);
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
        private bool StartsWithAllowlistedMagicNumber(ReadOnlySpan<byte> bytes)
        {
            return 
                bytes.StartsWith(Magic_JPG) ||
                bytes.StartsWith(Magic_PNG);
        }
    }
}

