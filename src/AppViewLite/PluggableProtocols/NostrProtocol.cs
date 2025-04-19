using AppViewLite.Models;
using AppViewLite.Numerics;
using NNostr.Client;
using NNostr.Client.Protocols;
using DuckDbSharp.Types;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO.Hashing;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace AppViewLite.PluggableProtocols.Nostr
{
    public class NostrProtocol : PluggableProtocol
    {
        public NostrProtocol() : base("did:nostr:")
        {

        }

        private ConcurrentSet<UInt128> RecentlyAddedPosts = new();

        public override Task DiscoverAsync(CancellationToken ct)
        {
            foreach (var relay in AppViewLiteConfiguration.GetStringList(AppViewLiteParameter.APPVIEWLITE_LISTEN_NOSTR_RELAYS) ?? [])
            {
                if (relay == "-") continue;
                RetryInfiniteLoopAsync(relay, ct => ListenNostrRelayAsync(relay, ct), ct).FireAndForget();
            }
            return Task.CompletedTask;
        }


        public async Task ListenNostrRelayAsync(string relay, CancellationToken ct)
        {
            using var client = new NostrClient(new Uri("wss://" + relay));
            await client.Connect(ct);
            await client.WaitUntilConnected(ct);
            var tcs = new TaskCompletionSource();
            ct.Register(tcs.SetCanceled);

            client.EventsReceived += (s, e) =>
            {
                using var _ = BlueskyRelationshipsClientBase.CreateIngestionThreadPriorityScope();
                foreach (var evt in e.events)
                {
                    try
                    {
                        OnEventReceived(evt, relay);
                    }
                    catch (Exception ex)
                    {
                        LogLowImportanceException(ex);
                    }
                }
            };
            await client.CreateSubscription("main", [new NostrSubscriptionFilter { }], ct);
            await client.ListenForMessages();
            await tcs.Task;
        }

        private readonly static JsonSerializerOptions JsonOptions = new JsonSerializerOptions { IncludeFields = true };
        private readonly static Regex? DontIngestTextRegex = AppViewLiteConfiguration.GetString(AppViewLiteParameter.APPVIEWLITE_NOSTR_IGNORE_REGEX) is { } s ? new Regex(s, RegexOptions.Singleline) : null;
        private void OnEventReceived(NostrEvent e, string relay)
        {
            var content = e.Content;
            var kind = (NostrEventKind)e.Kind;

            if (content?.Length >= 4 * 1024) return;
            if (e.Kind == (int)NostrEventKind.Short_Text_Note && content != null)
            {
                var trimmed = content.AsSpan().Trim();
                if (trimmed.Length >= 30 && !trimmed.Contains(' ') && !trimmed.StartsWith("http", StringComparison.Ordinal))
                    return; // probably not natural language
            }

            if ((NostrEventKind)e.Kind is NostrEventKind.Short_Text_Note or NostrEventKind.User_Metadata)
            {
                if (!string.IsNullOrEmpty(content) && DontIngestTextRegex?.IsMatch(content) == true)
                    return;
            }


            var did = GetDidFromPubKey(e.PublicKey);
            var didHash = StringUtils.HashUnicodeToUuid(did);

            if (!RecentlyAddedPosts.TryAdd(XxHash128.HashToUInt128(MemoryMarshal.AsBytes<char>(e.PublicKey + " " + e.Id))))
                return;
            if (RecentlyAddedPosts.Count >= 10_000)
                RecentlyAddedPosts = new();

            var ctx = RequestContext.CreateForFirehose("Nostr:" + kind + ":" + relay, allowStale: true);
            var previouslySeen = Apis.WithRelationshipsLock(rels => rels.NostrSeenPubkeyHashes.ContainsKey(didHash), ctx);
            if (!previouslySeen)
            {
                // Avoid wasting Plc assignments for single-use spam pubkeys.
                if (kind == NostrEventKind.Short_Text_Note)
                    Apis.WithRelationshipsWriteLock(rels => rels.NostrSeenPubkeyHashes.Add(didHash, 0), ctx);
                return;
            }

            if (kind == NostrEventKind.Short_Text_Note)
            {
                if (Apis.WithRelationshipsLock(rels => rels.KnownMirrorsToIgnore.ContainsKey(didHash), ctx))
                    return;
                var tid = CreateSyntheticTid(e.CreatedAt!.Value.UtcDateTime, e.Id);
                var postId = new QualifiedPluggablePostId(did, GetNonQualifiedPostId(tid, e.Id));
                var data = new BlueskyPostData
                {
                    Text = e.Content,
                };

                var emojis = GetCustomEmojis(e);
                Apis.MaybeAddCustomEmojis(emojis, ctx);

                data.Facets = StringUtils.GuessFacets(data.Text, includeImplicitFacets: false);


                //var labelsNs = e.Tags.Where(x => x.TagIdentifier == NostrTags.label_label_namespace).Select(x => x.Data).ToArray();
                //var labels = e.Tags.Where(x => x.TagIdentifier == NostrTags.label_namespace).Select(x => x.Data).ToArray();

                //var allLabels = labelsNs.Concat(labels).ToArray();
                //if (allLabels.Length != 0)
                //{
                //    if (allLabels.Any(x => x.Contains("en") || x.Contains("en-US"))) { }
                //}

                //var parent = e.Tags.Where(x => x.TagIdentifier == NostrTags.reply && x.Data[2] == "reply").FirstOrDefault();
                //if (parent != null)
                //{
                //var root = e.Tags.Where(x => x.TagIdentifier == NostrTags.reply && x.Data[2] == "root").FirstOrDefault() ?? parent;

                //var inReplyToPostId = new QualifiedPluggablePostId(GetDidFromPubKey(parent.Data[3]), new NonQualifiedPluggablePostId(default, Convert.FromHexString(parent.Data[0])));
                //var rootPostId = new QualifiedPluggablePostId(GetDidFromPubKey(root.Data[3]), new NonQualifiedPluggablePostId(default, Convert.FromHexString(root.Data[0])));
                //}

                data.IsReplyToUnspecifiedPost = e.Tags.Any(x => x.TagIdentifier == NostrTags.reply);

                var images = e.Tags.Where(x => x.TagIdentifier == NostrTags.imeta)
                    .Select(VariadicTagToMultiDictionary)
                    .ToDictionaryIgnoreDuplicates(x => GetSingleVariadicTag(x, "url")!, x => x);

                var utf8Body = data.GetUtf8IfNeededByCompactFacets();
                var media = new List<BlueskyMediaData>();
                if (data.Facets != null)
                {
                    foreach (var facet in data.Facets)
                    {
                        if (facet.IsLink)
                        {
                            var url = facet.GetLink(utf8Body)!;
                            var urlPath = new Uri(url).AbsolutePath;
                            if (images.TryGetValue(url, out var kvs))
                            {
                                var alt = GetSingleVariadicTag(kvs, "alt");
                                var mime = GetSingleVariadicTag(kvs, "m");

                                media.Add(new BlueskyMediaData
                                {
                                    AltText = alt,
                                    Cid = BlueskyRelationships.CompressBpe(url)!,
                                    IsVideo = mime?.StartsWith("video", StringComparison.Ordinal) == true
                                });
                            }
                            else if (
                                urlPath.EndsWith(".jpg", StringComparison.Ordinal) ||
                                urlPath.EndsWith(".jpeg", StringComparison.Ordinal) ||
                                urlPath.EndsWith(".webp", StringComparison.Ordinal) ||
                                urlPath.EndsWith(".png", StringComparison.Ordinal)
                                )
                            {
                                media.Add(new BlueskyMediaData
                                {
                                    Cid = BlueskyRelationships.CompressBpe(url)!,
                                });
                            }
                        }
                    }
                }
                data.Media = media.ToArray();
                StringUtils.GuessCustomEmojiFacets(data.Text, ref data.Facets, emojis);



                OnPostDiscovered(postId, null, null, data, ctx);
            }
            else if (kind == NostrEventKind.User_Metadata)
            {
                if (e.Content == "hello") return; // avoid noisy exceptions
                var p = System.Text.Json.JsonSerializer.Deserialize<NostrProfileJson>(e.Content!, JsonOptions)!;

                var nip05 = p.nip05.ValueKind == JsonValueKind.String ? p.nip05.GetString() : null;
                if (IsMirrorProfile(p, nip05))
                {
                    OnMirrorFound(didHash, ctx);
                    return;
                }


                var data = new BlueskyProfileBasicInfo
                {
                    DisplayName = p.name,
                    AvatarCidBytes = BlueskyRelationships.CompressBpe(p.picture),
                    BannerCidBytes = BlueskyRelationships.CompressBpe(p.banner),
                    Description = p.about,
                    CustomFields = [

                        new CustomFieldProto("website", p.website),
                        new CustomFieldProto("nip05", nip05),
                        new CustomFieldProto("lud16", p.lud16),
                        ..((p.fields ?? []).Select(x => new CustomFieldProto(x.FirstOrDefault(), x.ElementAtOrDefault(1))))
                    ],
                };

                var emojis = GetCustomEmojis(e);
                Apis.MaybeAddCustomEmojis(emojis, ctx);

                StringUtils.GuessCustomEmojiFacets(data.Description, ref data.DescriptionFacets, emojis);

                OnProfileDiscovered(did, data, ctx);
            }
            //else if (kind is NostrEventKind.Relay_List_Metadata or  NostrEventKind.Draft_Event) { } // noisy, uninteresting

            //else if (kind == NostrEventKind.Follows) { } 
            //else if (kind == NostrEventKind.Reaction) { }
            //else if (kind == NostrEventKind.User_Metadata) { }
            //else if (kind == NostrEventKind.Repost) { }
            //else if (kind == NostrEventKind.Zap) { }
            //else if (kind == NostrEventKind.Event_Deletion_Request) { }
            //else
            //{
            //    LogInfo(kind);
            //}


        }



        private static bool IsMirrorProfile(NostrProfileJson p, string? nip05)
        {
            if (nip05?.EndsWith(".mostr.pub", StringComparison.Ordinal) == true) return true;
            if (p.about?.Contains("https://hugohp.codeberg.page/@rss-to-nostr/", StringComparison.Ordinal) == true) return true;
            if (p.name?.Contains("RSS Feed", StringComparison.OrdinalIgnoreCase) == true) return true;
            return false;
        }

        private static string? GetSingleVariadicTag(Dictionary<string, List<string>> kvs, string k)
        {
            if (kvs.TryGetValue(k, out var vals))
            {
                var val = vals.FirstOrDefault();
                return string.IsNullOrEmpty(val) ? null : val;
            }
            return null;
        }

        private Dictionary<string, List<string>> VariadicTagToMultiDictionary(NostrEventTag tag)
        {
            var dict = new Dictionary<string, List<string>>();
            foreach (var data in tag.Data)
            {
                var space = data.IndexOf(' ');
                string k;
                string v;
                if (space == -1)
                {
                    k = data;
                    v = string.Empty;
                }
                else
                {
                    k = data.Substring(0, space);
                    v = data.Substring(space + 1);

                }
                if (!dict.ContainsKey(k))
                    dict[k] = new();
                dict[k].Add(v);
            }
            return dict;
        }

        private static CustomEmoji[] GetCustomEmojis(NostrEvent e)
        {
            return e.Tags.Where(x => x.TagIdentifier == NostrTags.emoji).Select(x => new CustomEmoji(x.Data[0], x.Data[1])).ToArray();
        }

        private static NonQualifiedPluggablePostId GetNonQualifiedPostId(Tid tid, string id)
        {
            if (id.Length != 64) throw new ArgumentException("Nostr id should be 64 characters.");
            return new NonQualifiedPluggablePostId(tid, Convert.FromHexString(id));
        }

        protected internal override void EnsureValidDid(string did)
        {
            var payload = did.AsSpan(DidPrefixLength);
            if (!payload.StartsWith("npub", StringComparison.Ordinal) || payload.ContainsAnyExcept(LettersAndDigits))
                throw new UnexpectedFirehoseDataException("Invalid nostr did.");
        }

        //private readonly static SearchValues<char> HexDigits = SearchValues.Create("0123456789abcdef");
        private readonly static SearchValues<char> LettersAndDigits = SearchValues.Create("0123456789abcdefghijklmnopqrstuvwxyz");

        public override Task<BlobResult> GetBlobAsync(string did, byte[] bytes, ThumbnailSize preferredSize, CancellationToken ct)
        {
            if (preferredSize == ThumbnailSize.video_thumbnail) throw new NotSupportedException("Nostr: GetBlobAsync with preferredSize=video_thumbnail is not supported.");
            var url = BlueskyRelationships.DecompressBpe(bytes)!;
            return BlueskyEnrichedApis.GetBlobFromUrl(new Uri(url), preferredSize: preferredSize, ct: ct);
        }

        public override string? TryGetOriginalProfileUrl(BlueskyProfile profile)
        {
            return "https://primal.net/p/" + GetNip19FromDid(profile.Did);
        }

        public override string? TryGetOriginalPostUrl(QualifiedPluggablePostId postId, BlueskyPost post)
        {
            if (!postId.HasExternalIdentifier) return null;
            return "https://primal.net/e/" + GetNoteId(postId.PostId);
        }


        private readonly static MethodInfo EncodeMethod = typeof(NIP19).Assembly.GetType("LNURL.Bech32Engine", true)!.GetMethod("Encode", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static, [typeof(string), typeof(byte[])])!;
        private static string GetNoteId(NonQualifiedPluggablePostId postId)
        {
            return Nip19Encode("note", postId.Bytes!);
        }
        private string GetDidFromPubKey(string publicKey)
        {
            return DidPrefix + GetNpubFromPubKey(publicKey);
        }

        private static string GetNpubFromPubKey(string publicKey)
        {
            return Nip19Encode("npub", Convert.FromHexString(publicKey));
        }

        private static string Nip19Encode(string prefix, byte[] data)
        {
            return (string)EncodeMethod.Invoke(null, [prefix, data])!;
        }


        private string GetNip19FromDid(string did)
        {
            return did.Substring(DidPrefixLength);
        }

        public override string? GetDefaultAvatar(string did)
        {
            return "/assets/default-nostr-avatar.png";
        }

        public override string? GetDefaultBannerColor(string did)
        {
            return "#662482";
        }

        public override string? GetDisplayNameFromDid(string did)
        {
            return string.Concat(GetNip19FromDid(did).AsSpan(0, 9), "…");
        }

        public override bool ShouldDisplayExternalLinkInBio => false;

        public override Task<string?> TryGetDidOrLocalPathFromUrlAsync(Uri url, bool preferDid)
        {
            if (url.Host == "primal.net")
            {
                var segments = url.GetSegments();
                if (segments.Length == 2 && segments[0] == "p" && segments[1].StartsWith("npub", StringComparison.Ordinal))
                    return Task.FromResult<string?>(DidPrefix + segments[1]);
            }
            return Task.FromResult<string?>(null);
        }

        public override string? GetDisplayHandle(BlueskyProfile profile)
        {
            return GetDisplayNameFromDid(profile.Did);
        }

        public override bool RequiresLateOpenGraphData(BlueskyPost post)
        {
            return DefaultRequiresLateOpenGraphData(post, alsoConsiderLinkFacets: true);
        }
    }
}

