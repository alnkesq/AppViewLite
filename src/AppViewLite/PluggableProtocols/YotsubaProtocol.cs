using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Net.Http.Json;
using AppViewLite.Models;
using System.Text.Json;
using System.IO;
using System.Text.RegularExpressions;
using AppViewLite;

namespace AppViewLite.PluggableProtocols.Yotsuba
{
    public class YotsubaProtocol : PluggableProtocol
    {

        private readonly Dictionary<string, (string CatalogUrlFormatter, string ThreadUrlFormatter, string ThumbnailUrlFormatter, string? BoardsJsonUrl, YotsubaBoardMetadataJson[]? BoardsFromConfig)> HostConfiguration;
        private static string? NormalizeNull(string? s) => string.IsNullOrEmpty(s) ? null : s;
        public YotsubaProtocol() : base("did:yotsuba:")
        {
            HostConfiguration = (
                AppViewLiteConfiguration.GetStringList(AppViewLiteParameter.APPVIEWLITE_YOTSUBA_HOSTS) ?? [])
                .Where(x => x != "-")
                .Select(x =>
                {
                    var mainParts = x.Split('+', StringSplitOptions.TrimEntries);

                    var host = mainParts[0];
                    string ToAbsolute(string url) => new Uri(new Uri("https://" + host), url).ToString(); // minimal escaping to preserve {0}



                    var catalogUrlFormatter = ToAbsolute(NormalizeNull(mainParts.ElementAtOrDefault(1)) ?? "/{0}/catalog.json");
                    var threadUrlFormatter = ToAbsolute(NormalizeNull(mainParts.ElementAtOrDefault(2)) ?? "/{0}/res/{1}.html");
                    var thumbnailUrlFormatter = ToAbsolute(NormalizeNull(mainParts.ElementAtOrDefault(3)) ?? "/{0}/thumb/{1}.png");
                    var boardsJsonOrBoards = NormalizeNull(mainParts.ElementAtOrDefault(4)) ?? "/boards.json";


                    var parsedBoards = boardsJsonOrBoards.StartsWith('/') || boardsJsonOrBoards.StartsWith("https://", StringComparison.Ordinal) || boardsJsonOrBoards.StartsWith("http://", StringComparison.Ordinal) ? null : boardsJsonOrBoards.Split('/', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).Select(x =>
                    {
                        var parts = x.Split('(', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
                        if (parts.Length == 1) return new YotsubaBoardMetadataJson() { board = x };
                        else return new YotsubaBoardMetadataJson() { board = parts[0], title = parts[1].TrimEnd(')').Trim() };
                    }).ToArray();
                    var boardsJsonUrl = parsedBoards != null ? null : boardsJsonOrBoards;

                    return (host, Configuration: (catalogUrlFormatter, threadUrlFormatter, thumbnailUrlFormatter, parsedBoards == null ? ToAbsolute(boardsJsonOrBoards) : null, parsedBoards));
                }).ToDictionary(x => x.host, x => x.Configuration);
        }

        public override Task DiscoverAsync(CancellationToken ct)
        {
            foreach (var host in HostConfiguration)
            {
                DiscoverFromHostAsync(host.Key, host.Value.BoardsFromConfig, ct).FireAndForget();
            }

            return Task.CompletedTask;
        }

        private async Task DiscoverFromHostAsync(string host, YotsubaBoardMetadataJson[]? boardsFromConfig, CancellationToken ct)
        {

            await Task.Delay(TimeSpan.FromMinutes(1), ct);

            var ctx = RequestContext.CreateForFirehose("Yotsuba:" + host);

            var boards = boardsFromConfig ?? (await BlueskyEnrichedApis.DefaultHttpClient.GetFromJsonAsync<YotsubaBoardMetadataResponseJson>(HostConfiguration[host].BoardsJsonUrl, JsonOptions, ct))!.boards;

            foreach (var board in boards)
            {
                var displayHost = host;
                if (displayHost.StartsWith("boards.", StringComparison.Ordinal))
                    displayHost = displayHost.Substring("boards.".Length);

                var boardId = new YotsubaBoardId(host, board.board);
                var description = ParseHtml(board.meta_description, boardId);

                var did = ToDid(boardId);
                var prev = Apis.WithRelationshipsLockForDid(did, (plc, rels) => rels.GetProfileBasicInfo(plc), ctx);

                var avatarCidBytes = prev?.AvatarCidBytes;
                if (avatarCidBytes == null)
                {
                    var faviconUrl = await BlueskyEnrichedApis.GetFaviconUrlAsync(new Uri(boardId.BaseUrl.AbsoluteUri + "/"));
                    avatarCidBytes = Encoding.UTF8.GetBytes(faviconUrl.AbsoluteUri);
                }

                OnProfileDiscovered(did, new BlueskyProfileBasicInfo
                {
                    DisplayName = "/" + boardId.BoardName + "/" + (board.title != null ? " - " + board.title : null),
                    Description = description.Text,
                    DescriptionFacets = description.Facets,
                    HasExplicitFacets = true,
                    AvatarCidBytes = avatarCidBytes
                }, ctx, shouldIndex: true);
                BoardLoopAsync(boardId, ct).FireAndForget();
            }
        }

        private async Task BoardLoopAsync(YotsubaBoardId boardId, CancellationToken ct)
        {
            EnsureValidDid(ToDid(boardId));
            double averageThreadsPerDay = 12;
            await Task.Delay(TimeSpan.FromMinutes(10 * Random.Shared.NextDouble()), ct);
            while (true)
            {
                try
                {

                    averageThreadsPerDay = await BoardIterationAsync(boardId, ct);
                }
                catch (Exception ex) when (!ct.IsCancellationRequested)
                {
                    averageThreadsPerDay /= 2;
                    LogNonCriticalException(boardId.BaseUrl.AbsoluteUri, ex);
                }

                var intervalDays = 0.5 / averageThreadsPerDay;
                intervalDays = Math.Clamp(intervalDays, TimeSpan.FromMinutes(5).TotalDays, TimeSpan.FromHours(12).TotalDays);
                await Task.Delay(TimeSpan.FromDays(intervalDays), ct);



            }
        }
        private readonly static JsonSerializerOptions JsonOptions = new JsonSerializerOptions() { IncludeFields = true, PropertyNameCaseInsensitive = true, NumberHandling = System.Text.Json.Serialization.JsonNumberHandling.AllowReadingFromString };
        private LruCache<(YotsubaBoardId Board, long PostId), int> LastObservedReplyCount = new(32 * 1024);
        private async Task<double> BoardIterationAsync(YotsubaBoardId boardId, CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();
            var ctx = RequestContext.CreateForFirehose("Yotsuba:" + boardId.Host);

            var threadPages = await BlueskyEnrichedApis.DefaultHttpClient.GetFromJsonAsync<YotsubaThreadPageJson[]>(string.Format(HostConfiguration[boardId.Host].CatalogUrlFormatter, boardId.BoardName), JsonOptions, ct);
            var boardDid = ToDid(boardId);
            var plc = Apis.WithRelationshipsUpgradableLock(rels => rels.SerializeDid(boardDid, ctx), ctx);
            var postDates = new List<DateTime>();
            foreach (var threadPage in threadPages!)
            {
                foreach (var thread in threadPage.Threads)
                {
                    try
                    {
                        var date = IndexThread(plc, boardId, thread, ctx);
                        postDates.Add(date);
                    }
                    catch (Exception ex)
                    {
                        LogNonCriticalException(ex);
                    }
                }
            }

            var sample = postDates.OrderByDescending(x => x).Take(postDates.Count / 4).ToArray(); // ignore old posts for frequency estimation (threads can disappear if they're not bumped)

            var threadsPerDay = (double)sample.Length / (DateTime.UtcNow - (sample?.LastOrDefault() ?? default)).TotalDays;

            return threadsPerDay;

        }

        private string GetThumbnailUrl(YotsubaBoardId boardId, string imageId) => string.Format(HostConfiguration[boardId.Host].ThumbnailUrlFormatter, boardId.BoardName, imageId);


        private DateTime IndexThread(Plc plc, YotsubaBoardId boardId, YotsubaCatalogThreadJson thread, RequestContext ctx)
        {

            var date = DateTime.UnixEpoch.AddSeconds(thread.Time);
            var threadNumber = thread.No;
            var replyCount = (int)thread.Replies;

            var boardAndPostId = (boardId, threadNumber);
            lock (LastObservedReplyCount)
            {
                if (LastObservedReplyCount.TryGetValue(boardAndPostId, out var prevReplyCount) && prevReplyCount == replyCount)
                {
                    return date;
                }
            }


            using var _ = BlueskyRelationshipsClientBase.CreateIngestionThreadPriorityScope();



            var threadId = new QualifiedPluggablePostId(ToDid(boardId), new NonQualifiedPluggablePostId(CreateSyntheticTid(date, threadNumber.ToString()), threadNumber));
            var threadIdCore = new PostId(plc, threadId.PostId.Tid);

            if (Apis.WithRelationshipsLock(rels =>
            {
                return rels.TryGetPostData(threadIdCore, skipBpeDecompression: true, ignoreDeletions: true)?.PluggableReplyCount == thread.Replies;
            }, ctx))
                return date;

            var subject = thread.Sub?.Trim();
            var comment = thread.Com?.Trim();
            if (string.IsNullOrEmpty(subject)) subject = null;
            if (string.IsNullOrEmpty(comment)) comment = null;

            var bodyHtml = subject != null && comment != null ? ConcatenateSubjectAndComment(subject, comment) : (subject ?? comment);
            var body = ParseHtml(bodyHtml, boardId);

            var media = new List<BlueskyMediaData>();
            if (thread.TimObject != null && !(thread.TimObject.GetValueKind() is JsonValueKind.Null or JsonValueKind.Undefined))
            {
                media.Add(new BlueskyMediaData
                {
                    Cid = Encoding.UTF8.GetBytes(thread.TimObject.Deserialize<object>() + thread.Ext),
                    IsVideo = thread.Ext is ".webm" or ".mp4"
                });
            }
            var files = (thread.ExtraFiles ?? []).Concat(thread.Files ?? []);
            foreach (var file in files)
            {
                media.Add(new BlueskyMediaData
                {
                    Cid = Encoding.UTF8.GetBytes(file.Tim + file.Ext),
                    IsVideo = file.Ext is ".webm" or ".mp4"
                });
            }
            var threadData = new BlueskyPostData
            {
                Text = body.Text,
                Facets = body.Facets,
                Media = media.ToArray(),
                PluggableReplyCount = replyCount,
                PluggableLikeCount = replyCount,
            };
            if (body.Text != null && Regex.IsMatch(body.Text, @"\bGeneral\b|gen\/", RegexOptions.IgnoreCase))
            {
                threadData.PluggableLikeCountForScoring = (int)Math.Ceiling(replyCount * 0.1);
            }
            OnPostDiscovered(threadId, null, null, threadData, ctx);
            lock (LastObservedReplyCount)
            {
                LastObservedReplyCount.Add(boardAndPostId, replyCount);
            }
            return date;
        }

        private static string ConcatenateSubjectAndComment(string subject, string comment)
        {
            var boldSubject = "<b>" + subject + "</b>";
            if (comment.StartsWith(@"<span class=""quote"">", StringComparison.Ordinal)) return boldSubject + "<br>" + comment;
            if (subject.Length != 0 && subject[^1] is '?' or '!' or '.' or ':') return boldSubject + " " + comment;
            return boldSubject + ": " + comment;
        }

        private static (string? Text, FacetData[]? Facets) ParseHtml(string? html, YotsubaBoardId boardId)
        {
            var (text, facets) = StringUtils.HtmlToFacets(StringUtils.ParseHtml(html).Body!, x => StringUtils.DefaultElementToFacet(x, boardId.BaseUrl));
            facets = (facets ?? []).Concat(StringUtils.GuessFacets(text, includeImplicitFacets: false) ?? []).ToArray();
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
            //if (preferredSize is ThumbnailSize.feed_thumbnail or ThumbnailSize.video_thumbnail)
            //{
            //    imageId = Path.GetFileNameWithoutExtension(imageId) + "s.jpg";
            //}
            var baseName = Path.GetFileNameWithoutExtension(imageId);

            var url = GetThumbnailUrl(board, baseName);
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
            if (!postId.HasExternalIdentifier) return null;
            var board = GetBoardIdFromDid(postId.Did);
            return string.Format(HostConfiguration[board.Host].ThreadUrlFormatter, board.BoardName, postId.PostId.AsString);
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

        public override Task<string?> TryGetDidOrLocalPathFromUrlAsync(Uri url, bool preferDid)
        {
            var segments = url.GetSegments();
            if (segments.Length == 1 || (segments.Length == 2 && segments[1] is "catalog" or "archive"))
            {
                if (HostConfiguration.ContainsKey(url.Host))
                    return Task.FromResult<string?>(DidPrefix + url.Host + ":" + segments[0]);
            }
            return Task.FromResult<string?>(null);
        }

        public override bool ProvidesLikeCount => true;


        public override bool ShouldShowRepliesTab(BlueskyProfile profile)
        {
            return false;
        }

        public override bool ShouldUseCompactMediaThumbnails(BlueskyPost post)
        {
            return true;
        }

        public override string? GetDisplayHandle(BlueskyProfile profile)
        {
            var board = GetBoardIdFromDid(profile.Did);
            return board.Host + "/" + board.BoardName + "/";
        }
    }

    public record struct YotsubaBoardId(string Host, string BoardName)
    {
        public Uri BaseUrl => new Uri("https://" + Host + "/" + BoardName);

        public override string ToString() => BaseUrl.ToString();
    }


}

