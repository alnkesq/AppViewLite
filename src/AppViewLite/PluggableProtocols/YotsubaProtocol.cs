using ProtoBuf;
using DuckDbSharp;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Net.Http.Json;
using AppViewLite.Models;
using FishyFlip.Lexicon;
using System.Text.Json;
using PeterO.Cbor;
using AngleSharp.Dom;
using System.IO;
using AppViewLite;
using System.Text.RegularExpressions;
using AppViewLite.Numerics;

namespace AppViewLite.PluggableProtocols.Yotsuba
{
    public class YotsubaProtocol : PluggableProtocol
    {

        private readonly Dictionary<string, (string ImageHost, string ApiHost)> HostConfiguration;
        public YotsubaProtocol() : base("did:yotsuba:")
        {
            HostConfiguration = (
                AppViewLiteConfiguration.GetStringList(AppViewLiteParameter.APPVIEWLITE_YOTSUBA_HOSTS) ?? [])
                .Select(x =>
                {
                    var parts = x.Split('/');
                    var host = parts[0];
                    var imageHost = parts[1];
                    var apiHost = parts[2];

                    return (host, Configuration: (string.IsNullOrEmpty(imageHost) ? host : imageHost, string.IsNullOrEmpty(apiHost) ? host : apiHost));
                }).ToDictionary(x => x.host, x => x.Configuration);
        }

        public override Task DiscoverAsync(CancellationToken ct)
        {
            foreach (var host in HostConfiguration.Keys)
            {
                DiscoverFromHostAsync(host, ct).FireAndForget();
            }

            return Task.CompletedTask;
        }

        private async Task DiscoverFromHostAsync(string host, CancellationToken ct)
        {

            var boards = await BlueskyEnrichedApis.DefaultHttpClient.GetFromJsonAsync<YotsubaBoardMetadataResponseJson>(GetApiPrefix(host) + "/boards.json", JsonOptions, ct);

            foreach (var board in boards!.boards)
            {
                var displayHost = host;
                if (displayHost.StartsWith("boards.", StringComparison.Ordinal))
                    displayHost = displayHost.Substring("boards.".Length);

                var boardId = new YotsubaBoardId(host, board.board);
                var description = ParseHtml(board.meta_description, boardId);

                var did = ToDid(boardId);
                var prev = Apis.WithRelationshipsLockForDid(did, (plc, rels) => rels.GetProfileBasicInfo(plc), null);

                var avatarCidBytes = prev?.AvatarCidBytes;
                if (avatarCidBytes == null)
                {
                    var faviconUrl = await BlueskyEnrichedApis.GetFaviconUrlAsync(new Uri(boardId.BaseUrl.AbsoluteUri + "/"));
                    avatarCidBytes = Encoding.UTF8.GetBytes(faviconUrl.AbsoluteUri);
                }

                OnProfileDiscovered(did, new BlueskyProfileBasicInfo 
                { 
                     DisplayName = "/" + boardId.BoardName + "/ - " + board.title + " (" + displayHost + ")",
                     Description = description.Text,
                     DescriptionFacets = description.Facets,
                     HasExplicitFacets = true,
                     AvatarCidBytes = avatarCidBytes
                }, shouldIndex: true);
                BoardLoopAsync(boardId, ct).FireAndForget();
            }
        }

        private async Task BoardLoopAsync(YotsubaBoardId boardId, CancellationToken ct)
        {
            EnsureValidDid(ToDid(boardId));
            while (true)
            {
                try
                {
                    
                    await BoardIterationAsync(boardId, ct);
                }
                catch (Exception ex) when (!ct.IsCancellationRequested)
                {
                    Console.Error.WriteLine(ex);
                }

                await Task.Delay(TimeSpan.FromMinutes(Random.Shared.Next(15, 45)), ct);



            }
        }
        private readonly static JsonSerializerOptions JsonOptions = new JsonSerializerOptions() { IncludeFields = true, PropertyNameCaseInsensitive = true };
        private async Task BoardIterationAsync(YotsubaBoardId boardId, CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();
            var ctx = RequestContext.CreateForFirehose("Yotsuba:" + boardId.Host);

            var threadPages = await BlueskyEnrichedApis.DefaultHttpClient.GetFromJsonAsync<YotsubaThreadPageJson[]>(GetApiPrefix(boardId) + "/catalog.json", JsonOptions, ct);
            var boardDid = ToDid(boardId);
            var plc = Apis.WithRelationshipsUpgradableLock(rels => rels.SerializeDid(boardDid), ctx);
            foreach (var threadPage in threadPages!)
            {
                foreach (var thread in threadPage.Threads)
                {
                    try
                    {
                        IndexThread(plc, boardId, thread, ctx);
                    }
                    catch (Exception ex)
                    {
                        Console.Error.WriteLine(ex);
                    }
                    await Task.Delay(300, ct);
                }
            }
        }

        private string GetApiPrefix(string host) => "https://" + HostConfiguration[host].ApiHost;
        private string GetImagePrefix(string host) => "https://" + HostConfiguration[host].ImageHost;
        private string GetApiPrefix(YotsubaBoardId boardId) => GetApiPrefix(boardId.Host) + "/" + boardId.BoardName;
        private string GetImagePrefix(YotsubaBoardId boardId) => GetImagePrefix(boardId.Host) + "/" + boardId.BoardName;


        private void IndexThread(Plc plc, YotsubaBoardId boardId, YotsubaCatalogThreadJson thread, RequestContext ctx)
        {
            using var _ = BlueskyRelationshipsClientBase.CreateIngestionThreadPriorityScope();

            var date = DateTime.UnixEpoch.AddSeconds(thread.Time);

            var threadNumber = thread.No;
            var threadId = new QualifiedPluggablePostId(ToDid(boardId), new NonQualifiedPluggablePostId(CreateSyntheticTid(date, threadNumber.ToString()), threadNumber));
            var threadIdCore = new PostId(plc, threadId.PostId.Tid);

            if (Apis.WithRelationshipsLock(rels =>
            {
                return rels.TryGetPostData(threadIdCore)?.PluggableReplyCount == thread.Replies;
            }))
                return;

            var subject = thread.Sub?.Trim();
            var comment = thread.Com?.Trim();
            if (string.IsNullOrEmpty(subject)) subject = null;
            if (string.IsNullOrEmpty(comment)) comment = null;

            var bodyHtml = subject != null && comment != null ? subject + ": " + comment : (subject ?? comment);
            var body = ParseHtml(bodyHtml, boardId);

            if (subject != null && comment != null)
            {
                body.Facets ??= [];
                body.Facets = body.Facets.Concat([new FacetData() { Start = 0, Length = Encoding.UTF8.GetByteCount(subject), Bold = true }]).ToArray();
            }

            var threadData = new BlueskyPostData
            {
                Text = body.Text,
                Facets = body.Facets,
                Media = [new BlueskyMediaData
                {
                    Cid = Encoding.UTF8.GetBytes(thread.Tim + thread.Ext),
                    IsVideo = thread.Ext is ".webm" or ".mp4"
                }],
                PluggableReplyCount = (int)thread.Replies,
                PluggableLikeCount = (int)thread.Replies,
            };
            OnPostDiscovered(threadId, null, null, threadData, ctx);
        }

        private static (string? Text, FacetData[]? Facets) ParseHtml(string? html, YotsubaBoardId boardId)
        {
            var (text, facets) = StringUtils.HtmlToFacets(StringUtils.ParseHtml(html).Body!, x => StringUtils.DefaultElementToFacet(x, boardId.BaseUrl));
            facets = (facets ?? []).Concat(StringUtils.GuessFacets(text, includeHashtags: false) ?? []).ToArray();
            return (text, facets);
        }

        private string ToDid(YotsubaBoardId boardId)
        {
            return DidPrefix + boardId.Host + ":" + boardId.BoardName;
        }



        protected internal override void EnsureValidDid(string did)
        {
            GetBoardIdFromDid(did);
        }

        public override async Task<BlobResult> GetBlobAsync(string did, byte[] bytes, ThumbnailSize preferredSize, CancellationToken ct)
        {
            var board = GetBoardIdFromDid(did);
            var imageId = Encoding.UTF8.GetString(bytes);
            if (imageId.StartsWith("https://", StringComparison.Ordinal) || imageId.StartsWith("http://", StringComparison.Ordinal))
            {
                var image = await BlueskyEnrichedApis.GetBlobFromUrl(new Uri(imageId), ct: ct);
                image.IsFavIcon = true;
                return image;
            }
            if (preferredSize is ThumbnailSize.feed_thumbnail or ThumbnailSize.video_thumbnail)
            {
                imageId = Path.GetFileNameWithoutExtension(imageId) + "s.jpg";
            }
            var url = $"{GetImagePrefix(board)}/{imageId}";
            return await BlueskyEnrichedApis.GetBlobFromUrl(new Uri(url), ct: ct);
        }

        public YotsubaBoardId GetBoardIdFromDid(string did)
        {
            var parts = did.Substring(DidPrefixLength).Split(':');
            if (parts.Length != 2) throw new ArgumentException("Invalid Yotsuba DID.");
            BlueskyEnrichedApis.EnsureValidDomain(parts[0]);
            if (!Regex.IsMatch(parts[1].AsSpan(), @"^[\w\d]+$"))
                throw new ArgumentException("Invalid Yotsuba DID.");
            return new YotsubaBoardId(parts[0], parts[1]);
        }

        public override string? GetIndexableDidText(string did)
        {
            var parsed = GetBoardIdFromDid(did);
            return parsed.Host + " " + parsed.BoardName;
        }

        public override string? TryGetOriginalPostUrl(QualifiedPluggablePostId postId, BlueskyPost post)
        {
            var board = GetBoardIdFromDid(postId.Did);
            return "https://" + board.Host + "/" + board.BoardName + "/thread/" + postId.PostId.AsString;
        }

        public override string? TryGetOriginalProfileUrl(BlueskyProfile profile)
        {
            var board = GetBoardIdFromDid(profile.Did);
            return "https://" + board.Host + "/" + board.BoardName + "/catalog";
        }


        public override string? TryGetDomainForDid(string did)
        {
            return GetBoardIdFromDid(did).Host;
        }

        public override string? GetDefaultBannerColor(string did)
        {
            return "#D4D8F0";
        }

        public override Task<string?> TryGetDidOrLocalPathFromUrlAsync(Uri url)
        {
            var segments = url.GetSegments();
            if (segments.Length == 1 || (segments.Length == 2 && segments[1] is "catalog" or "archive"))
            {
                if (HostConfiguration.ContainsKey(url.Host))
                    return Task.FromResult<string?>(DidPrefix + url.Host + ":" + segments[0]);
            }
            return Task.FromResult<string?>(null);
        }

    }

    public record struct YotsubaBoardId(string Host, string BoardName)
    {
        public Uri BaseUrl => new Uri("https://" + Host + "/" + BoardName);
    }


}

