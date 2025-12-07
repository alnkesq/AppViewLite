using AppViewLite.Models;
using AppViewLite.Numerics;
using AppViewLite.PluggableProtocols.Rss;
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
    public abstract class PluggableProtocol : LoggableBase
    {
        public PluggableProtocol(string didPrefix)
        {
            if (didPrefix == "did:plc:" || didPrefix == "did:web:" || !Regex.IsMatch(didPrefix, @"^did:[\w\-]+:$"))
                AssertionLiteException.Throw("Invalid pluggable protocol DID prefix.");
            DidPrefix = didPrefix;
            Apis = null!;
        }
        public string DidPrefix { get; private set; }
        internal BlueskyEnrichedApis Apis;
        protected int DidPrefixLength => DidPrefix.Length;

        private void EnsureOwnDid(string did)
        {
            if (!did.StartsWith(DidPrefix, StringComparison.Ordinal))
                AssertionLiteException.Throw("Not pluggable protocol's own DID.");
        }

        public void OnProfileDiscovered(string did, BlueskyProfileBasicInfo data, RequestContext ctx, bool shouldIndex = true, bool willOnlyRepost = false, string[]? extraIndexableWords = null)
        {
            EnsureOwnDid(did);
            EnsureValidDid(did);

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
            data.Pronouns = StringUtils.NormalizeNull(data.Pronouns);
            data.Location = StringUtils.NormalizeNull(data.Location);
            data.ExternalWebsite = StringUtils.NormalizeNull(data.ExternalWebsite);

            Apis.WithRelationshipsWriteLock(rels =>
            {
                var plc = rels.SerializeDid(did, ctx);
                if (shouldIndex)
                {
                    rels.IndexProfile(plc, data);
                    foreach (var word in didWords)
                    {
                        rels.IndexProfileWord(word, plc);
                    }

                    if (extraIndexableWords != null)
                    {
                        foreach (var word in extraIndexableWords)
                        {
                            rels.IndexProfileWord(word, plc);
                        }
                    }
                }
                if (willOnlyRepost)
                    rels.ReposterOnlyProfile.AddIfMissing(plc, 0);

                rels.StoreProfileBasicInfo(plc, data);
            }, ctx);
        }

        public virtual bool RepostsAreCategories => false;

        public QualifiedPluggablePostId? GetPostIdWithCorrectTid(QualifiedPluggablePostId qualifiedPostId, RequestContext ctx)
        {
            var reversedTid = TryGetTidFromPostId(new QualifiedPluggablePostId(qualifiedPostId.Did, qualifiedPostId.PostId.CloneWithoutTid())) ?? default;
            if (reversedTid != default)
            {
                if (qualifiedPostId.Tid != default && qualifiedPostId.Tid != reversedTid) throw new Exception("TID returned by TryGetTidFromPostId and TID explicitly passed to GetPostId don't match.");
                return qualifiedPostId.WithTid(reversedTid);
            }

            var result = Apis.WithRelationshipsLock(rels => rels.TryGetStoredSyntheticTidFromPluggablePostId(qualifiedPostId), ctx);
            if (result.Tid != default) return result;

            return null;

        }

        public void OnRepostDiscovered(string reposterDid, QualifiedPluggablePostId qualifiedPostId, DateTime repostDate, RequestContext ctx)
        {
            ctx.AllowStale = false;
            EnsureOwnDid(reposterDid);
            EnsureValidDid(reposterDid);
            var tid = (GetPostIdWithCorrectTid(qualifiedPostId, ctx) ?? default).Tid;
            if (tid == default) throw new ArgumentNullException("Pluggable repost with unavailable Tid");
            BlueskyRelationships.EnsureNotExcessivelyFutureDate(tid);

            if (Apis.AdministrativeBlocklist.ShouldBlockIngestion(reposterDid)) return;


            Apis.WithRelationshipsWriteLock(rels =>
            {
                if (Apis.AdministrativeBlocklist.ShouldBlockIngestion(qualifiedPostId.Did)) return;
                var reposterPlc = rels.SerializeDid(reposterDid, ctx);
                var postId = new PostId(rels.SerializeDid(qualifiedPostId.Did, ctx), tid);
                var repostRkey = Tid.FromDateTime(repostDate);
                if (rels.Reposts.Add(postId, new Relationship(reposterPlc, repostRkey)))
                {
                    var repost = new RecentRepost(repostRkey, postId);
                    rels.UserToRecentReposts.Add(reposterPlc, repost);
                    rels.AddRepostToRecentRepostCache(reposterPlc, repost);
                }
            }, ctx);
        }
        public PostId? OnPostDiscovered(QualifiedPluggablePostId postId, QualifiedPluggablePostId? inReplyTo, QualifiedPluggablePostId? rootPostId, BlueskyPostData data, RequestContext ctx, bool shouldIndex = true, bool replyIsSemanticallyRepost = false, bool onlyInsertIfNew = false)
        {
            if (inReplyTo != null && inReplyTo.Value.Equals(default(QualifiedPluggablePostId))) inReplyTo = null;
            if (rootPostId != null && rootPostId.Value.Equals(default(QualifiedPluggablePostId))) rootPostId = null;

            if (postId.Tid != default)
                BlueskyRelationships.EnsureNotExcessivelyFutureDate(postId.Tid);

            rootPostId ??= inReplyTo ?? postId;
            EnsureOwnDid(postId.Did);

            if (Apis.AdministrativeBlocklist.ShouldBlockIngestion(postId.Did)) return null;

            if ((data.PluggableLikeCount != null || data.PluggableLikeCountForScoring != null) && !ProvidesLikeCount(postId.Did))
                throw new ArgumentException("PluggableProtocol.ProvidesLikeCount should be overriden if posts are populated with PluggableLikeCount.");


            if (inReplyTo != null) EnsureOwnDid(inReplyTo.Value.Did);
            EnsureOwnDid(rootPostId.Value.Did);


            MaybeTrimLongText(ref data.Text);
            MaybeTrimLongText(ref data.ExternalTitle);
            MaybeTrimLongText(ref data.ExternalDescription);
            if (data.Media != null)
            {
                foreach (var media in data.Media)
                {
                    MaybeTrimLongText(ref media.AltText);
                }
            }


            if (data.Facets != null && data.Facets.Length == 0) data.Facets = null;
            if (string.IsNullOrWhiteSpace(data.Text) && data.Facets == null)
                data.Text = null;

            if (data.Media != null && data.Media.Length == 0) data.Media = null;
            data.Language ??= LanguageEnum.Unknown;

            EnsureValidDid(postId.Did);
            if (inReplyTo != null) EnsureValidDid(inReplyTo.Value.Did);
            if (rootPostId != null) EnsureValidDid(rootPostId.Value.Did);


            ContinueOutsideLock continueOutsideLock = default;
            PostId simplePostId = default;

            var preresolved = Apis.SerializeSingleDid(postId.Did, ctx);

            Apis.WithRelationshipsWriteLock(rels =>
            {
                var authorPlc = rels.SerializeDidWithHint(postId.Did, ctx, preresolved);


                if (!StoreTidIfNotReversible(rels, ref postId))
                    return;



                data.PluggablePostId = postId.PostId;
                data.PostId = new PostId(authorPlc, postId.PostId.Tid);

                if (!StoreTidIfNotReversible(rels, ref inReplyTo))
                {
                    data.IsReplyToUnspecifiedPost = true;
                    inReplyTo = null;
                }

                if (!postId.Equals(rootPostId))
                {
                    if (!StoreTidIfNotReversible(rels, ref rootPostId))
                    {
                        rootPostId = inReplyTo ?? postId;
                    }
                }

                if (inReplyTo != null)
                {
                    data.InReplyToPlc = rels.SerializeDid(inReplyTo.Value.Did, ctx).PlcValue;
                    data.InReplyToRKey = inReplyTo.Value.PostId.Tid.TidValue;
                    data.PluggableInReplyToPostId = inReplyTo.Value.PostId;
                }

                data.RootPostPlc = rels.SerializeDid(rootPostId!.Value.Did, ctx).PlcValue;
                data.RootPostRKey = rootPostId.Value.PostId.Tid.TidValue;
                data.PluggableRootPostId = rootPostId.Value.PostId;

                if (shouldIndex)
                {
                    rels.IndexPost(data);

                    if (data.PostId.PostRKey.Date >= ApproximateDateTime32.MinValueAsDateTime)
                    {
                        foreach (var hashtag in StringUtils.GuessHashtags(data.Text))
                        {
                            rels.AddToSearchIndex(hashtag.ToLowerInvariant(), BlueskyRelationships.GetApproxTime32(data.PostId.PostRKey));
                        }
                    }
                }

                if (inReplyTo != null)
                {
                    rels.DirectReplies.AddIfMissing(data.InReplyToPostId!.Value, data.PostId);
                }

                rels.UserToRecentPosts.Add(data.PostId.Author, new RecentPost(data.PostId.PostRKey, replyIsSemanticallyRepost ? default : new Plc(data.InReplyToPlc.GetValueOrDefault())));

                if (data.Media != null)
                    rels.UserToRecentMediaPosts.Add(data.PostId.Author, data.PostId.PostRKey);

                simplePostId = new PostId(authorPlc, postId.PostId.Tid);

                if (!onlyInsertIfNew || !rels.PostData.ContainsKey(simplePostId))
                {
                    var likeCountForScoring = (data.PluggableLikeCountForScoring ?? data.PluggableLikeCount).GetValueOrDefault();

                    rels.AddPostToRecentPostCache(authorPlc, new UserRecentPostWithScore(simplePostId.PostRKey, data.InReplyToPostId?.Author ?? default, likeCountForScoring));


                    if (likeCountForScoring != 0)
                    {
                        if (rels.RecentPluggablePostLikeCount.AddIfMissing(simplePostId, likeCountForScoring))
                        {
                            rels.IncrementRecentPopularPostLikeCount(simplePostId, likeCountForScoring);
                            rels.ReplicaOnlyApproximateLikeCountCache.UpdateIfExists(simplePostId, likeCountForScoring);
                            rels.ApproximateLikeCountCache.UpdateIfExists(simplePostId, likeCountForScoring);
                        }
                    }


                    byte[]? postBytes = null;
                    continueOutsideLock = new ContinueOutsideLock(() => postBytes = BlueskyRelationships.SerializePostData(data, postId.Did), relationships =>
                    {
                        relationships.PostData.AddRange(simplePostId, postBytes); // double insertions are fine, the second one wins.
                    });
                }

            }, ctx);
            if (continueOutsideLock != default)
            {
                continueOutsideLock.OutsideLock();
                Apis.WithRelationshipsWriteLock(rels => continueOutsideLock.Complete(rels), ctx);
            }
            return simplePostId != default ? simplePostId : null;
        }

        private static void MaybeTrimLongText(ref string? text)
        {
            if (text != null && text.Length > EfficientTextCompressor.MaxLength)
            {
                text = string.Concat(text.AsSpan(0, EfficientTextCompressor.MaxLength - 1), "…");
            }
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

        public bool StoreTidIfNotReversible(BlueskyRelationships rels, ref QualifiedPluggablePostId postId)
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

        public static async Task RetryInfiniteLoopAsync(string nameForDebugging, Func<CancellationToken, Task> attempt, CancellationToken ct, RetryPolicy? retryPolicy = null)
        {
            retryPolicy ??= RetryPolicy.CreateConstant(TimeSpan.FromSeconds(30));
            // This method must not throw, but CAN exit cleanly on CancellationToken.IsCancellationRequested (due to CaptureFirehoseCursors -= CaptureFirehoseCursor in callers)
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
                    LogNonCriticalException("Pluggable protocol error (" + nameForDebugging + ")", ex);
                    await Task.Delay(retryPolicy.OnException(ex));
                }
            }
        }

        public static Tid CreateSyntheticTid(DateTime date, ReadOnlySpan<char> hashableData)
        {
            var roundedDate = new DateTime(date.Ticks / TimeSpan.TicksPerSecond * TimeSpan.TicksPerSecond, DateTimeKind.Utc);

            var hash = System.IO.Hashing.XxHash64.HashToUInt64(MemoryMarshal.AsBytes(hashableData));

            var fakeMicros = hash % 0x80000;
            if (fakeMicros >= 1_000_000) AssertionLiteException.Throw("fakeMicros is >= 1 million");
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

            postId = fallback;


        }

        public virtual string? TryGetOriginalPostUrl(QualifiedPluggablePostId postId, BlueskyPost post)
        {
            return null;
        }

        public virtual string? TryGetOriginalProfileUrl(BlueskyProfile profile)
        {
            return null;
        }

        public virtual Task<BlobResult> GetBlobAsync(string did, byte[] cid, ThumbnailSize preferredSize, CancellationToken ct)
        {
            throw new NotSupportedException("This pluggable protocol does not support blob retrieval.");
        }

        public virtual bool ShouldUseM3u8ForVideo(string did, byte[] cid)
        {
            return Nitter.IsTwitterDid(did);
        }

        public virtual string? GetIndexableDidText(string did)
        {
            return null;
        }

        public virtual string? TryGetDomainForDid(string did) => null;


        protected void OnMirrorFound(DuckDbUuid didHash, RequestContext ctx)
        {
            if (!Apis.WithRelationshipsLock(rels => rels.KnownMirrorsToIgnore.ContainsKey(didHash), ctx))
            {
                Apis.WithRelationshipsWriteLock(rels => rels.KnownMirrorsToIgnore.Add(didHash, 0), ctx);
            }
        }

        public virtual string? GetFollowingUrl(string did) => null;
        public virtual string? GetFollowersUrl(string did) => null;

        public virtual string? GetDisplayNameFromDid(string did) => null;
        public virtual string? GetDefaultAvatar(string did) => null;

        public virtual string? GetDefaultBannerColor(string did) => null;

        public virtual TimeSpan GetProfilePageMaxPostAge() => TimeSpan.FromDays(90);

        public virtual bool ShouldDisplayExternalLinkInBio => true;

        public virtual bool ProvidesLikeCount(string did) => false;

        public virtual Task<string?> TryGetDidOrLocalPathFromUrlAsync(Uri url, bool preferDid, RequestContext ctx) => Task.FromResult<string?>(null);

        public virtual bool ShouldIncludeFullReplyChain(BlueskyPost post) => false;

        public virtual bool ShouldShowRepliesTab(BlueskyProfile profile) => true;
        public virtual bool ShouldShowMediaTab(BlueskyProfile profile) => true;
        public virtual bool SupportsProfileMetadataLookup(string did) => false;

        public virtual Task TryFetchProfileMetadataAsync(string did, RequestContext ctx) => Task.FromResult<BlueskyProfileBasicInfo?>(null);


        protected static bool DefaultRequiresLateOpenGraphData(BlueskyPost post, bool alsoConsiderLinkFacets)
        {
            var data = post.Data;
            if (data == null) return false;
            if (data.QuotedPostId != null || (data.Media != null && data.Media.Length != 0)) return false;

            if (data.ExternalUrl == null && !(alsoConsiderLinkFacets && post.ExternalLinkOrFirstLinkFacet is { } facetLink && !BlueskyEnrichedApis.ExternalDomainsNoAutoPreview.Contains(StringUtils.TryParseUri(facetLink)?.GetDomainTrimWww()!))) return false;
            if (data.ExternalDescription != null || data.ExternalTitle != null || data.ExternalThumbCid != null) return false;
            return true;
        }

        public virtual bool RequiresLateOpenGraphData(BlueskyPost post)
        {
            return DefaultRequiresLateOpenGraphData(post, alsoConsiderLinkFacets: false);
        }

        public virtual bool ShouldUseCompactMediaThumbnails(BlueskyPost post)
        {
            return false;
        }

        public virtual string? GetDisplayHandle(BlueskyProfile profile)
        {
            return null;
        }

        public virtual bool ReusesThumbImageForFullSizeImages(BlueskyPost post) => false;
    }

}

