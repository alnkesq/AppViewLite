using AppViewLite.Models;
using AppViewLite.Numerics;
using DuckDbSharp.Types;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace AppViewLite.PluggableProtocols
{
    public abstract class PluggableProtocol
    {
        public PluggableProtocol(string didPrefix)
        {
            if (didPrefix == "did:plc:" || didPrefix == "did:web:" || !Regex.IsMatch(didPrefix, @"^did:[\w\-]+:$"))
                throw new ArgumentException();
            DidPrefix = didPrefix;

        }
        public string DidPrefix { get; private set; }
        internal BlueskyEnrichedApis Apis;
        protected int DidPrefixLength => DidPrefix.Length;

        private void EnsureOwnDid(string did)
        {
            if (!did.StartsWith(DidPrefix, StringComparison.Ordinal))
                throw new ArgumentException();
        }

        public void OnProfileDiscovered(string did, BlueskyProfileBasicInfo data, bool shouldIndex)
        {
            EnsureOwnDid(did);

            if (Apis.AdministrativeBlocklist.ShouldBlockIngestion(did)) return;

            var didWords = StringUtils.GetDistinctWords(GetIndexableDidText(did));

            if (data.DisplayNameFacets != null)
                data.DisplayNameFacets = data.DisplayNameFacets.Where(x => x.CustomEmojiHash != null).ToArray(); // only emoji facets allowed in display name

            if (data.DisplayNameFacets != null && data.DisplayNameFacets.Length == 0) data.DisplayNameFacets = null;
            if (data.DescriptionFacets != null && data.DescriptionFacets.Length == 0) data.DescriptionFacets = null;
            if (string.IsNullOrWhiteSpace(data.Description) && data.DescriptionFacets == null)
                data.Description = null;
            if (string.IsNullOrWhiteSpace(data.DisplayName))
                data.DisplayName = null;
            if (data.CustomFields != null) data.CustomFields = data.CustomFields.Where(x => !string.IsNullOrEmpty(x.Name) && !string.IsNullOrEmpty(x.Value)).ToArray();
            if (data.CustomFields != null && data.CustomFields.Length == 0)
                data.CustomFields = null;

            Apis.WithRelationshipsWriteLock(rels =>
            {
                var plc = rels.SerializeDid(did);
                if (shouldIndex)
                {
                    rels.IndexProfile(plc, data);
                    foreach (var word in didWords)
                    {
                        rels.IndexProfileWord(word, plc);
                    }
                    
                }
                
                rels.StoreProfileBasicInfo(plc, data);
            });
        }

        public void OnPostDiscovered(QualifiedPluggablePostId postId, QualifiedPluggablePostId? inReplyTo, QualifiedPluggablePostId? rootPostId, BlueskyPostData data, bool shouldIndex)
        {
            rootPostId ??= inReplyTo ?? postId;
            EnsureOwnDid(postId.Did);

            if (Apis.AdministrativeBlocklist.ShouldBlockIngestion(postId.Did)) return;






            if (inReplyTo != null) EnsureOwnDid(inReplyTo.Value.Did);
            EnsureOwnDid(rootPostId.Value.Did);

            if (data.Facets != null && data.Facets.Length == 0) data.Facets = null;
            if (string.IsNullOrWhiteSpace(data.Text) && data.Facets == null)
                data.Text = null;

            if (data.Media != null && data.Media.Length == 0) data.Media = null;
            data.Language ??= LanguageEnum.Unknown;

            Apis.WithRelationshipsWriteLock(rels =>
            {
                var authorPlc = rels.SerializeDid(postId.Did);


                if (!StoreTidIfNotReversible(rels, ref postId))
                    return;



                data.PluggablePostId = postId.PostId;
                data.PostId = new PostId(authorPlc, postId.PostId.Tid);

                if (!StoreTidIfNotReversible(rels, ref inReplyTo))
                {
                    data.IsReplyToUnspecifiedPost = true;
                    inReplyTo = null;
                }

                if (!StoreTidIfNotReversible(rels, ref rootPostId))
                {
                    rootPostId = inReplyTo ?? postId;
                }

                if (inReplyTo != null)
                {
                    data.InReplyToPlc = rels.SerializeDid(inReplyTo.Value.Did).PlcValue;
                    data.InReplyToRKey = inReplyTo.Value.PostId.Tid.TidValue;
                    data.PluggableInReplyToPostId = inReplyTo.Value.PostId;
                }

                data.RootPostPlc = rels.SerializeDid(rootPostId!.Value.Did).PlcValue;
                data.RootPostRKey = rootPostId.Value.PostId.Tid.TidValue;
                data.PluggableRootPostId = rootPostId.Value.PostId;

                if (shouldIndex)
                {
                    rels.IndexPost(data);

                    foreach (var hashtag in StringUtils.GuessHashtags(data.Text))
                    {
                        rels.AddToSearchIndex(hashtag.ToLowerInvariant(), BlueskyRelationships.GetApproxTime32(data.PostId.PostRKey));
                    }
                }

                if (inReplyTo != null)
                {
                    rels.DirectReplies.AddIfMissing(data.InReplyToPostId!.Value, data.PostId);
                }

                if (data.RootPostId != data.PostId)
                {
                    rels.RecursiveReplies.AddIfMissing(data.RootPostId, data.PostId);
                }

                rels.UserToRecentPosts.AddIfMissing(data.PostId.Author, new RecentPost(data.PostId.PostRKey, new Plc(data.InReplyToPlc.GetValueOrDefault())));

                if (data.Media != null)
                    rels.UserToRecentMediaPosts.AddIfMissing(data.PostId.Author, data.PostId.PostRKey);

                rels.PostData.AddRange(new PostId(authorPlc, postId.PostId.Tid), BlueskyRelationships.SerializePostData(data, postId.Did));



            });
        }
        private bool StoreTidIfNotReversible(BlueskyRelationships rels, ref QualifiedPluggablePostId? postId)
        {
            if (postId != null)
            {
                var p = postId.Value;
                var result = StoreTidIfNotReversible(rels, ref p);
                postId = p;
                return result;
            }
            return true;
        }

        private bool StoreTidIfNotReversible(BlueskyRelationships rels, ref QualifiedPluggablePostId postId)
        {
            var reversedTid = TryGetTidFromPostId(new QualifiedPluggablePostId(postId.Did, postId.PostId.CloneWithoutTid())) ?? default;
            if (reversedTid != default)
            {
                if (postId.Tid != default && reversedTid != default)
                    throw new ArgumentException("Mismatch between TryGetTidFromPostId and the TID passed to OnPostDiscovered.");

                postId = postId.WithTid(reversedTid);
            }
            else
            {
                var hash = postId.GetExternalPostIdHash();
                if (rels.ExternalPostIdHashToSyntheticTid.TryGetSingleValue(hash, out var tid))
                {
                    postId = postId.WithTid(tid);
                }
                else
                {
                    if (postId.Tid != default)
                    {
                        rels.ExternalPostIdHashToSyntheticTid.Add(hash, postId.Tid);
                    }
                    else
                    {
                        return false;
                    }
                }
            }

            return true;
        }

        public bool RequiresExplicitPostIdStorage(NonQualifiedPluggablePostId? postId)
        {
            if (postId == null) return false;
            var reversed = TryGetPostIdFromTid(postId.Value.Tid);
            if (!reversed.HasValue) return true;
            return false;
        }

        public virtual NonQualifiedPluggablePostId? TryGetPostIdFromTid(Tid tid)
        {
            return null;
        }
        public virtual Tid? TryGetTidFromPostId(QualifiedPluggablePostId id)
        {
            return null;
        }

        public abstract Task DiscoverAsync(CancellationToken ct);

        internal static PluggableProtocol Register(Type type)
        {
            var instance = RegisteredPluggableProtocols.FirstOrDefault(x => x.GetType() == type);

            if (instance == null)
            {
                instance = (PluggableProtocol)Activator.CreateInstance(type)!;
                RegisteredPluggableProtocols.Add(instance);
            }
            return instance;
        }

        public readonly static List<PluggableProtocol> RegisteredPluggableProtocols = new();

        public static PluggableProtocol? TryGetPluggableProtocolForDid(string did)
        {
            foreach (var item in RegisteredPluggableProtocols)
            {
                if (did.StartsWith(item.DidPrefix, StringComparison.Ordinal))
                    return item;
            }
            return null;
        }

        public static async Task RetryInfiniteLoopAsync(Func<CancellationToken, Task> attempt, CancellationToken ct)
        {

            while (true)
            {
                try
                {
                    await attempt(ct);
                }
                catch when (ct.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    Console.Error.WriteLine("Pluggable protocol error:");
                    Console.Error.WriteLine(ex);
                }
                await Task.Delay(TimeSpan.FromSeconds(30), ct);
            }
        }

        public static Tid CreateSyntheticTid(DateTime date, ReadOnlySpan<char> hashableData)
        {
            var roundedDate = new DateTime(date.Ticks / TimeSpan.TicksPerSecond * TimeSpan.TicksPerSecond, DateTimeKind.Utc);

            var hash = System.IO.Hashing.XxHash64.HashToUInt64(MemoryMarshal.AsBytes(hashableData));

            var fakeMicros = hash % 0x80000;
            if (fakeMicros >= 1_000_000) throw new Exception();
            var fakeClock = hash >> 64 - 5;
            roundedDate = roundedDate.AddTicks((long)fakeMicros * TimeSpan.TicksPerMicrosecond);
            var tid = Tid.FromDateTime(roundedDate, (uint)fakeClock);
            return tid;
        }

        internal abstract protected void EnsureValidDid(string did);

        public virtual string? TryHandleToDid(string handle)
        {
            return null;
        }

        public virtual string? TryGetHandleFromDid(string did)
        {
            return null;
        }

        internal void DecompressPluggablePostId(ref NonQualifiedPluggablePostId? postId, Tid tid, NonQualifiedPluggablePostId? fallback)
        {
            if (postId != null) return;
            postId = TryGetPostIdFromTid(tid);
            if (postId != null) return;

            postId = fallback!.Value;


        }

        public virtual string? TryGetOriginalPostUrl(QualifiedPluggablePostId postId)
        {
            return null;
        }

        public virtual string? TryGetOriginalProfileUrl(string did)
        {
            return null;
        }

        public virtual Task<BlobResult> GetBlobAsync(string did, byte[] cid, ThumbnailSize preferredSize, CancellationToken ct)
        {
            throw new NotSupportedException();
        }

        public virtual bool ShouldUseM3u8ForVideo(string did, byte[] cid) => false;

        public virtual string? GetIndexableDidText(string did)
        {
            return null;
        }

        public virtual string? TryGetDomainForDid(string did) => null;

        public virtual bool UseSmallThumbnails => false;

        protected void OnMirrorFound(DuckDbUuid didHash)
        {
            if (!Apis.WithRelationshipsLock(rels => rels.KnownMirrorsToIgnore.ContainsKey(didHash)))
            {
                Apis.WithRelationshipsWriteLock(rels => rels.KnownMirrorsToIgnore.Add(didHash, 0));
            }
        }
    }
}

