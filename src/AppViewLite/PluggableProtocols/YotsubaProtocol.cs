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
                OnProfileDiscovered(ToDid(boardId), new BlueskyProfileBasicInfo 
                { 
                     DisplayName = "/" + boardId.BoardName + "/ - " + board.title + " (" + displayHost + ")",
                     Description = description.Text,
                     DescriptionFacets = description.Facets,
                     HasExplicitFacets = true,
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
                catch (Exception ex)
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

            var threadPages = await BlueskyEnrichedApis.DefaultHttpClient.GetFromJsonAsync<YotsubaThreadPageJson[]>(GetApiPrefix(boardId) + "/catalog.json", JsonOptions, ct);
            var boardDid = ToDid(boardId);
            var plc = Apis.WithRelationshipsUpgradableLock(rels => rels.SerializeDid(boardDid));
            foreach (var threadPage in threadPages!)
            {
                foreach (var thread in threadPage.Threads)
                {
                    try
                    {
                        IndexThread(plc, boardId, thread);
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


        private void IndexThread(Plc plc, YotsubaBoardId boardId, YotsubaCatalogThreadJson thread)
        {
            var date = DateTime.UnixEpoch.AddSeconds(thread.Time);
            if (date < ApproximateDateTime32.MinValueAsDateTime) return;

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
                    Cid = Encoding.UTF8.GetBytes(thread.Tim + thread.Ext)
                }],
                PluggableReplyCount = (int)thread.Replies,
                PluggableLikeCount = (int)thread.Replies,
            };
            OnPostDiscovered(threadId, null, null, threadData, shouldIndex: true);
        }

        private static (string? Text, FacetData[]? Facets) ParseHtml(string? html, YotsubaBoardId boardId)
        {
            var (text, facets) = StringUtils.HtmlToFacets(StringUtils.ParseHtml(html).Body!, element =>
            {
                return ElementToFacet(element, boardId.BaseUrl);
            });
            facets = (facets ?? []).Concat(StringUtils.GuessFacets(text, includeHashtags: false) ?? []).ToArray();
            return (text, facets);
        }

        private string ToDid(YotsubaBoardId boardId)
        {
            return DidPrefix + boardId.Host + ":" + boardId.BoardName;
        }

        private static FacetData? ElementToFacet(IElement element, Uri baseUrl)
        {
            if (element.TagName == "A")
            {
                var link = element.GetAttribute("href");
                if (!string.IsNullOrEmpty(link) && link != "#")
                {
                    var url = new Uri(baseUrl, link);
                    link = url.ToString();
                    if (link == element.Text())
                        return new FacetData { SameLinkAsText = true };
                    else
                        return new FacetData { Link = link };
                }
                
            }
            return null;
        }

        protected internal override void EnsureValidDid(string did)
        {
            GetBoardIdFromDid(did);
        }

        public override Task<BlobResult> GetBlobAsync(string did, byte[] bytes, ThumbnailSize preferredSize)
        {
            var board = GetBoardIdFromDid(did);
            var imageId = Encoding.UTF8.GetString(bytes);
            if (preferredSize == ThumbnailSize.feed_thumbnail)
            {
                imageId = Path.GetFileNameWithoutExtension(imageId) + "s.jpg";
            }
            var url = $"{GetImagePrefix(board)}/{imageId}";
            return BlueskyEnrichedApis.GetBlobFromUrl(new Uri(url));
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

        public override string? TryGetOriginalPostUrl(QualifiedPluggablePostId postId)
        {
            var board = GetBoardIdFromDid(postId.Did);
            return "https://" + board.Host + "/" + board.BoardName + "/thread/" + postId.PostId.AsString;
        }

        public override string? TryGetOriginalProfileUrl(string did)
        {
            var board = GetBoardIdFromDid(did);
            return "https://" + board.Host + "/" + board.BoardName + "/catalog";
        }

        public override bool UseSmallThumbnails => true;

        public override string? TryGetDomainForDid(string did)
        {
            return GetBoardIdFromDid(did).Host;
        }

    }

    public record struct YotsubaBoardId(string Host, string BoardName)
    {
        public Uri BaseUrl => new Uri("https://" + Host + "/" + BoardName);
    }


}

