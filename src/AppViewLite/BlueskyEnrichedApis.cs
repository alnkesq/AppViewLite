using AppViewLite.Models;
using AppViewLite.Numerics;
using AppViewLite.PluggableProtocols;
using AppViewLite.Storage;
using DnsClient;
using FishyFlip;
using FishyFlip.Lexicon;
using FishyFlip.Lexicon.App.Bsky.Actor;
using FishyFlip.Lexicon.App.Bsky.Embed;
using FishyFlip.Lexicon.App.Bsky.Feed;
using FishyFlip.Lexicon.Com.Atproto.Repo;
using FishyFlip.Lexicon.Com.Atproto.Sync;
using FishyFlip.Models;
using FishyFlip.Tools;
using Ipfs;
using Microsoft.Extensions.ObjectPool;
using PeterO.Cbor;
using AppViewLite.Storage;
using DuckDbSharp.Types;
using AppViewLite.Storage;
using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Json;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Block = FishyFlip.Lexicon.App.Bsky.Graph.Block;
using Follow = FishyFlip.Lexicon.App.Bsky.Graph.Follow;
using List = FishyFlip.Lexicon.App.Bsky.Graph.List;
using Listblock = FishyFlip.Lexicon.App.Bsky.Graph.Listblock;
using Listitem = FishyFlip.Lexicon.App.Bsky.Graph.Listitem;

namespace AppViewLite
{
    public class BlueskyEnrichedApis : BlueskyRelationshipsClientBase
    {
        public static BlueskyEnrichedApis Instance;
        public bool IsReadOnly => relationshipsUnlocked.IsReadOnly;

        public BlueskyRelationships DangerousUnlockedRelationships => relationshipsUnlocked;
        public BlueskyRelationships? DangerousReadOnlyReplicaUnlockedRelationships => readOnlyReplicaRelationshipsUnlocked;

        public BlueskyEnrichedApis(PrimarySecondaryPair primarySecondaryPair)
            : base(primarySecondaryPair)
        {
            RunHandleVerificationDict = new(async (handle, ctx) =>
            {
                return new(await ResolveHandleAsync(handle, ctx: ctx, allowUnendorsed: false), ctx.MinVersion);
            });
            FetchAndStoreDidDocNoOverrideDict = new(async (pair, anyCtx) =>
            {
                return new(await FetchAndStoreDidDocNoOverrideCoreAsync(pair.Did, pair.Plc, anyCtx), anyCtx.MinVersion);
            });
            FetchAndStoreLabelerServiceMetadataDict = new(FetchAndStoreLabelerServiceMetadataCoreAsync);
            FetchAndStoreProfileDict = new(FetchAndStoreProfileCoreAsync);
            FetchAndStoreAccountStateFromPdsDict = new(FetchAndStoreAccountStateFromPdsCoreAsync);
            FetchAndStoreListMetadataDict = new(FetchAndStoreListMetadataCoreAsync);
            FetchAndStorePostDict = new(FetchAndStorePostCoreAsync);
            FetchAndStoreOpenGraphDict = new(FetchOpenGraphDictCoreAsync);
            HandleToDidAndStoreDict = new(HandleToDidAndStoreCoreAsync);
            CarImportDict = new((args, extraArgs) => ImportCarIncrementalCoreAsync(extraArgs.Did, args.Kind, args.Plc, args.Incremental ? new Tid(extraArgs.Previous?.LastRevOrTid ?? default) : default, default, ctx: extraArgs.Ctx, authenticatedCtx: extraArgs.AuthenticatedCtx, extraArgs.SlowImport));
            DidDocOverrides = new ReloadableFile<DidDocOverridesConfiguration>(AppViewLiteConfiguration.GetString(AppViewLiteParameter.APPVIEWLITE_DID_DOC_OVERRIDES), path =>
            {
                var config = DidDocOverridesConfiguration.ReadFromFile(path);

                var pdsesToDids = config.CustomDidDocs.GroupBy(x => x.Value.Pds).ToDictionary(x => x.Key, x => x.Select(x => x.Key).ToArray());

                lock (SecondaryFirehoses)
                {

                    var stopListening = SecondaryFirehoses.Where(x => !pdsesToDids.ContainsKey(x.Key));
                    foreach (var oldpds in stopListening)
                    {
                        Log("Stopping secondary firehose: " + oldpds.Key);
                        oldpds.Value.Cancel();
                        SecondaryFirehoses.Remove(oldpds.Key);
                    }

                    foreach (var (newpds, dids) in pdsesToDids)
                    {
                        if (!SecondaryFirehoses.ContainsKey(newpds))
                        {
                            var cts = new CancellationTokenSource();
                            var indexer = new Indexer(this);
                            var didsHashset = dids.ToHashSet();
                            indexer.VerifyValidForCurrentRelay = did =>
                            {
                                if (!didsHashset.Contains(did))
                                    throw new UnexpectedFirehoseDataException($"Ignoring record for {did} from relay {newpds} because it's not one of the allowlisted DIDs for that PDS.");
                            };
                            indexer.FirehoseUrl = new Uri(newpds);
                            Log("Starting secondary firehose: " + newpds);
                            indexer.StartListeningToAtProtoFirehoseRepos(RetryPolicy.CreateForUnreliableServer(), useWatchdog: false, cts.Token).FireAndForget();
                            SecondaryFirehoses.Add(newpds, cts);
                        }
                    }
                }

                return config;
            });
            (new Func<Task>(async () =>
            {
                // in case there's no one else to call GetValue for us (only overrides, no main firehose)
                while (true)
                {
                    if (DangerousUnlockedRelationships.IsDisposed) return;
                    DidDocOverrides.GetValue();
                    await Task.Delay(TimeSpan.FromSeconds(10));
                }
            }))();


            primarySecondaryPair.relationshipsUnlocked.NotificationGenerated += Relationships_NotificationGenerated;
        }

        private async Task<Versioned<AccountState>> FetchAndStoreAccountStateFromPdsCoreAsync(string did, RequestContext ctx)
        {
            using var protocol = await CreateProtocolForDidAsync(did, ctx);
            var response = (await protocol.GetRepoStatusAsync(new ATDid(did))).HandleResult()!;
            return SetAccountState(did, response.Active, response.Status, ctx);
        }

        private async Task<long> FetchOpenGraphDictCoreAsync(string url, RequestContext ctx)
        {
            var urlHash = StringUtils.HashUnicodeToUuid(url);
            var data = await OpenGraph.TryRetrieveOpenGraphDataAsync(new Uri(url));
            EfficientTextCompressor.CompressInPlace(ref data.ExternalTitle, ref data.ExternalTitleBpe);
            EfficientTextCompressor.CompressInPlace(ref data.ExternalDescription, ref data.ExternalDescriptionBpe);
            EfficientTextCompressor.CompressInPlace(ref data.ExternalThumbnailUrl, ref data.ExternalThumbnailUrlBpe);
            EfficientTextCompressor.CompressInPlace(ref data.ExternalUrl, ref data.ExternalUrlBpe);
            return WithRelationshipsWriteLock(rels =>
            {
                rels.OpenGraphData.AddRange(urlHash, BlueskyRelationships.SerializeProto(data));
                return rels.Version;
            }, ctx);
        }

        private void Relationships_NotificationGenerated(Plc destination, Notification notification, RequestContext ctx)
        {
            // Here we're inside the lock.
            var rels = relationshipsUnlocked;


            var actor = notification.Actor;

            // Notifications don't support DOM dynamic refresh (it would be bad UX anyways). Prefetch post and profile data now.
            if (actor != default && !rels.Profiles.ContainsKey(actor) && !rels.FailedProfileLookups.ContainsKey(actor))
            {
                var profile = rels.GetProfile(actor);
                DispatchOutsideTheLock(() => EnrichAsync([profile], ctx).FireAndForget());
            }

            // These notifications can reference posts from the past that we don't have.
            if (notification.Kind is NotificationKind.LikedYourPost or NotificationKind.RepostedYourPost or NotificationKind.LabeledYourPost)
            {
                var postId = new PostId(destination, notification.RKey);
                if (!rels.PostData.ContainsKey(postId) && !rels.FailedPostLookups.ContainsKey(postId))
                {
                    var post = rels.GetPost(postId);
                    DispatchOutsideTheLock(() => EnrichAsync([post], ctx).FireAndForget());
                }
            }
        }

        private Dictionary<string, CancellationTokenSource> SecondaryFirehoses = new();


        public ReloadableFile<DidDocOverridesConfiguration> DidDocOverrides;

        public TaskDictionary<string, RequestContext, Versioned<string>> RunHandleVerificationDict;
        public TaskDictionary<(Plc Plc, string Did), RequestContext, Versioned<DidDocProto>> FetchAndStoreDidDocNoOverrideDict;
        public TaskDictionary<string, RequestContext, (long MinVersion, IReadOnlyList<LabelId> Labels)> FetchAndStoreLabelerServiceMetadataDict;
        public TaskDictionary<string, RequestContext, long> FetchAndStoreProfileDict;
        public TaskDictionary<string, RequestContext, Versioned<AccountState>> FetchAndStoreAccountStateFromPdsDict;
        public TaskDictionary<RelationshipStr, RequestContext, long> FetchAndStoreListMetadataDict;
        public TaskDictionary<PostIdString, RequestContext, long> FetchAndStorePostDict;
        public TaskDictionary<string, RequestContext, long> FetchAndStoreOpenGraphDict;
        public TaskDictionary<string, RequestContext, Versioned<string>> HandleToDidAndStoreDict;

        public Task<long> FetchAndStoreOpenGraphAsync(Uri url, RequestContext ctx) => FetchAndStoreOpenGraphDict.GetValueAsync(url.AbsoluteUri, RequestContext.CreateForTaskDictionary(ctx));

        public async Task<string> RunHandleVerificationAsync(string handle, RequestContext ctx)
        {
            var result = await RunHandleVerificationDict.GetValueAsync(handle, RequestContext.CreateForTaskDictionary(ctx));
            result.BumpMinimumVersion(ctx);
            return result.Value;
        }

        public async Task<DidDocProto> FetchAndStoreDidDocNoOverrideAsync(Plc plc, string did, RequestContext ctx)
        {
            var result = await FetchAndStoreDidDocNoOverrideDict.GetValueAsync((plc, did), RequestContext.CreateForTaskDictionary(ctx));
            result.BumpMinimumVersion(ctx);
            return result.Value;
        }

        private async Task<(long MinVersion, IReadOnlyList<LabelId> Labels)> FetchAndStoreLabelerServiceMetadataCoreAsync(string did, RequestContext ctx)
        {
            var record = (FishyFlip.Lexicon.App.Bsky.Labeler.Service)(await GetRecordAsync(did, FishyFlip.Lexicon.App.Bsky.Labeler.Service.RecordType, "self", ctx: ctx)).Value;

            var defs = (record.Policies?.LabelValueDefinitions ?? [])?.ToDictionary(x => x.Identifier);

            return WithRelationshipsWriteLock(rels =>
            {
                var plc = rels.SerializeDid(did, ctx);
                var labels = new List<LabelId>();
                foreach (var policy in record.Policies?.LabelValues ?? [])
                {
                    defs!.TryGetValue(policy, out var def);

                    var locale = def?.Locales?.FirstOrDefault(x => x.Lang == "en" || x.Lang == "en-US") ?? def?.Locales?.FirstOrDefault();
                    var labelInfo = new BlueskyLabelData
                    {
                        ReuseDefaultDefinition = def == null,

                        DisplayName = locale?.Name,
                        Description = locale?.Description,
                        AdultOnly = def?.AdultOnly ?? false,
                        Severity = def?.Severity != null ? Enum.Parse<BlueskyLabelSeverity>(def.Severity, ignoreCase: true) : default,
                        Blur = def?.Blurs != null ? Enum.Parse<BlueskyLabelBlur>(def.Blurs, ignoreCase: true) : default,
                        DefaultSetting = def?.DefaultSetting != null ? Enum.Parse<BlueskyLabelDefaultSetting>(def.DefaultSetting, ignoreCase: true) : default,

                    };
                    var labelId = new LabelId(plc, BlueskyRelationships.HashLabelName(policy));
                    rels.LabelData.AddRange(labelId, BlueskyRelationships.SerializeProto(labelInfo, x => x.Dummy = true));
                    if (!rels.LabelNames.ContainsKey(labelId.NameHash))
                    {
                        rels.LabelNames.AddRange(labelId.NameHash, System.Text.Encoding.UTF8.GetBytes(policy));
                    }
                    labels.Add(labelId);
                }
                return (rels.Version, labels);
            }, ctx);
        }


        public async Task<BlueskyProfile[]> EnrichAsync(BlueskyProfile[] profiles, RequestContext ctx, Action<BlueskyProfile>? onLateDataAvailable = null, bool omitLabelsAndViewerFlags = false, CancellationToken ct = default)
        {
            if (profiles.Length == 0) return profiles;

            if (!omitLabelsAndViewerFlags)
                PopulateViewerFlags(profiles, ctx);

            if (!IsReadOnly)
            {
                foreach (var profile in profiles.Distinct())
                {
                    if (profile.HandleIsUncertain)
                    {
                        VerifyHandleAndNotifyAsync(profile.Did, profile.PossibleHandle, RequestContext.ToNonUrgent(ctx)).FireAndForget();
                    }
                }

                await AwaitWithShortDeadline(Task.WhenAll(profiles.Where(x => x.BasicData == null).Distinct().Select(async profile =>
                {
                    var version = await FetchAndStoreProfileDict.GetValueAsync(profile.Did, RequestContext.CreateForTaskDictionary(ctx));
                    ctx.BumpMinimumVersion(version);
                    WithRelationshipsLock(rels =>
                    {
                        profile.BasicData = rels.GetProfileBasicInfo(profile.Plc);
                    }, ctx);

                    onLateDataAvailable?.Invoke(profile);
                })), ctx);
            }

            if (!omitLabelsAndViewerFlags)
                await EnrichAsync(profiles.SelectMany(x => x.Labels ?? []).ToArray(), ctx);

            return profiles;
        }

        private static Task AwaitWithShortDeadline(Task task, RequestContext ctx)
        {
            if (ctx.ShortDeadline != null)
            {
                return Task.WhenAny(task, ctx.ShortDeadline);
            }
            else
            {
                return task;
            }
        }


        private async Task<long> FetchAndStoreProfileCoreAsync(string did, RequestContext ctx)
        {
            if (PluggableProtocol.TryGetPluggableProtocolForDid(did) is { } pluggable)
            {
                try
                {
                    await pluggable.TryFetchProfileMetadataAsync(did, ctx);
                }
                catch (Exception)
                {
                }
                return WithRelationshipsWriteLock(rels =>
                {
                    var plc = rels.SerializeDid(did, ctx);
                    var have = rels.Profiles.ContainsKey(plc);
                    if (!have)
                        rels.FailedProfileLookups.Add(plc, DateTime.UtcNow);
                    return rels.Version;
                }, ctx);
            }
            else
            {
                Profile? response = null;
                try
                {
                    response = (Profile)(await GetRecordAsync(did, Profile.RecordType, "self", ctx)).Value;
                }
                catch (Exception)
                {
                }

                return WithRelationshipsWriteLock(rels =>
                {
                    var plc = rels.SerializeDid(did, ctx);
                    if (response != null)
                    {
                        rels.StoreProfileBasicInfo(plc, response, ctx);
                    }
                    else
                    {
                        rels.FailedProfileLookups.Add(plc, DateTime.UtcNow);
                    }
                    return rels.Version;
                }, ctx);
            }
        }




        private async Task<long> FetchAndStoreListMetadataCoreAsync(RelationshipStr listId, RequestContext ctx)
        {
            List? response = null;
            try
            {
                response = (List)(await GetRecordAsync(listId.Did, List.RecordType, listId.RKey, ctx)).Value;
            }
            catch (Exception)
            {
            }

            return WithRelationshipsWriteLock(rels =>
            {
                var id = new Models.Relationship(rels.SerializeDid(listId.Did, ctx), Tid.Parse(listId.RKey));
                if (response != null)
                {
                    rels.Lists.AddRange(id, BlueskyRelationships.SerializeProto(BlueskyRelationships.ListToProto(response)));
                }
                else
                {
                    rels.FailedListLookups.Add(id, DateTime.UtcNow);
                }
                return rels.Version;
            }, ctx);
        }

        private async Task<long> FetchAndStorePostCoreAsync(PostIdString postId, RequestContext ctx)
        {
            Post? response = null;
            try
            {
                response = (Post)(await GetRecordAsync(postId.Did, Post.RecordType, postId.RKey, ctx)).Value;
            }
            catch (Exception)
            {
            }

            return WithRelationshipsWriteLock(rels =>
            {
                var id = rels.GetPostId(postId.Did, postId.RKey, ctx);

                if (response != null)
                {
                    rels.StorePostInfo(id, response, postId.Did, ctx);
                }
                else
                {
                    rels.FailedPostLookups.Add(id, DateTime.UtcNow);
                }

                return rels.Version;
            }, ctx);
        }


        public async Task<string?> TryDidToHandleAsync(string did, RequestContext ctx)
        {
            if (!BlueskyRelationships.IsNativeAtProtoDid(did))
            {
                if (did.StartsWith(AppViewLite.PluggableProtocols.ActivityPub.ActivityPubProtocol.DidPrefix, StringComparison.Ordinal))
                {
                    var bridged = TryGetBidirectionalAtProtoBridgeForFediverseProfileAsync(did, ctx);
                    if (bridged != null) return null; // URL explicitly requested did:fedi:
                    return AppViewLite.PluggableProtocols.ActivityPub.ActivityPubProtocol.Instance!.TryGetHandleFromDid(did);
                }
                return null;
            }
            try
            {
                var profile = GetSingleProfile(did, ctx);
                var handle = profile.PossibleHandle;
                if (profile.HandleIsUncertain)
                {
                    if (handle == null)
                    {
                        var diddoc = await GetDidDocAsync(did, ctx);
                        handle = diddoc.Handle;
                        if (handle == null) return null;
                    }
                    var did2 = await RunHandleVerificationAsync(handle, ctx);
                    if (did != did2) return null;
                }

                if (handle == did) return null;
                handle = BlueskyRelationships.MaybeBridgyHandleToFediHandle(handle) ?? handle;
                return handle;
            }
            catch (Exception ex)
            {
                LogNonCriticalException(ex);
                return null;
            }
        }

        public async Task VerifyHandleAndNotifyAsync(string did, string? handle, RequestContext ctx)
        {
            if (handle == null)
            {
                // happens when the PLC directory is not synced.
                // Note that handle-based badges won't be live-updated.
                var diddoc = await GetDidDocAsync(did, ctx);
                handle = diddoc.Handle;
                if (handle == null) return;
            }

            RunHandleVerificationAsync(handle, ctx).ContinueWith(task =>
            {
                var k = task.IsCompletedSuccessfully && task.Result == did ? handle : null;
#nullable disable
                ctx.SendSignalrAsync("HandleVerificationResult", did, BlueskyRelationships.MaybeBridgyHandleToFediHandle(k));
#nullable restore
            }).FireAndForgetLowImportance();
        }


        public async Task<ProfilesAndContinuation> GetAppViewLiteUsers(string? continuation, int limit, RequestContext ctx)
        {
            EnsureLimit(ref limit);
            var parsedContinuation = StringUtils.DeserializeFromString<Plc>(continuation);
            var users = WithRelationshipsLock(rels => rels.AppViewLiteProfiles.EnumerateKeysSortedDescending(parsedContinuation).Select(x => rels.GetProfile(x, ctx)).Take(limit + 1).ToArray(), ctx);
            await EnrichAsync(users, ctx);
            return GetPageAndNextPaginationFromLimitPlus1(users, limit, x => StringUtils.SerializeToString(x.Plc));
        }
        public async Task<ProfilesAndContinuation> GetRecentProfiles(string? continuation, int limit, RequestContext ctx)
        {
            EnsureLimit(ref limit);
            var parsedContinuation = StringUtils.DeserializeFromString<Plc>(continuation);
            var users = WithRelationshipsLock(rels => SimpleJoin.ConcatPresortedEnumerablesKeepOrdered([rels.PlcToDidOther.EnumerateKeysSortedDescending(parsedContinuation), rels.PlcToDidPlc.EnumerateKeysSortedDescending(parsedContinuation)], x => x, new ReverseComparer<Plc>()).Select(x => rels.GetProfile(x, ctx)).Take(limit + 1).ToArray(), ctx);
            await EnrichAsync(users, ctx);
            return GetPageAndNextPaginationFromLimitPlus1(users, limit, x => StringUtils.SerializeToString(x.Plc));
        }

        public async Task<ProfilesAndContinuation> GetFollowingPrivateAsync(string did, string? continuation, int limit, RequestContext ctx)
        {
            if (!ctx.IsLoggedIn || did != ctx.Session.Did)
                return new();
            EnsureLimit(ref limit, 50);
            var offset = continuation != null ? int.Parse(continuation) : 0;
            var plcs = ctx.UserContext.PrivateProfile!.PrivateFollows!
                .Where(x => (x.Flags & PrivateFollowFlags.PrivateFollow) != default)
                .OrderByDescending(x => x.DatePrivateFollowed)
                .Skip(offset)
                .Take(limit + 1)
                .Select(x => new Plc(x.Plc))
                .ToArray();
            var profiles = WithRelationshipsLock(rels => plcs.Select(x => rels.GetProfile(x, ctx)).ToArray(), ctx);
            await EnrichAsync(profiles, ctx);
            return (profiles.Take(limit).ToArray(), profiles.Length == limit + 1 ? (offset + limit).ToString() : null);

        }

        public async Task<ProfilesAndContinuation> GetFollowingAsync(string did, string? continuation, int limit, RequestContext ctx)
        {
            EnsureLimit(ref limit, 50);
            var response = await ListRecordsAsync(did, Follow.RecordType, limit: limit + 1, cursor: continuation, ctx);
            var following = WithRelationshipsUpgradableLock(rels =>
            {
                return response!.Records!.TrySelect(x => rels.GetProfile(rels.SerializeDid(((FishyFlip.Lexicon.App.Bsky.Graph.Follow)x.Value!).Subject!.Handler, ctx))).ToArray();
            }, ctx);
            await EnrichAsync(following, ctx);
            return (following, response.Records.Count > limit ? response!.Cursor : null);
        }

        public async Task<ProfilesAndContinuation> GetBlockingAsync(string did, string? continuation, int limit, RequestContext ctx)
        {
            EnsureLimit(ref limit, 50);
            var response = await ListRecordsAsync(did, Block.RecordType, limit: limit + 1, cursor: continuation, ctx);
            var following = WithRelationshipsUpgradableLock(rels =>
            {
                return response!.Records!.TrySelect(x => rels.GetProfile(rels.SerializeDid(((FishyFlip.Lexicon.App.Bsky.Graph.Block)x.Value!).Subject!.Handler, ctx))).ToArray();
            }, ctx);
            await EnrichAsync(following, ctx);
            return (following, response.Records.Count > limit ? response!.Cursor : null);
        }
        public async Task<ProfilesAndContinuation> GetFollowersYouFollowAsync(string did, string? continuation, int limit, RequestContext ctx)
        {
            EnsureLimit(ref limit, 50);
            var r = WithRelationshipsLock(rels => rels.GetFollowersYouFollow(did, continuation, limit, ctx), ctx);
            await EnrichAsync(r.Profiles, ctx);
            return r;
        }


        public readonly static FrozenSet<string> ExternalDomainsAlwaysCompactView = (AppViewLiteConfiguration.GetStringList(AppViewLiteParameter.APPVIEWLITE_EXTERNAL_PREVIEW_SMALL_THUMBNAIL_DOMAINS) ?? []).ToFrozenSet();
        public readonly static FrozenSet<string> ExternalDomainsNoAutoPreview = (AppViewLiteConfiguration.GetStringList(AppViewLiteParameter.APPVIEWLITE_EXTERNAL_PREVIEW_DISABLE_AUTOMATIC_FOR_DOMAINS) ?? []).ToFrozenSet();
        public readonly static FrozenSet<string> ExternalDomainsIgnoreDescription = (AppViewLiteConfiguration.GetStringList(AppViewLiteParameter.APPVIEWLITE_EXTERNAL_PREVIEW_IGNORE_DESCRIPTION_FOR_DOMAINS) ?? []).ToFrozenSet();

        public async Task<BlueskyPost[]> EnrichAsync(BlueskyPost[] posts, RequestContext ctx, Action<BlueskyPost>? onPostDataAvailable = null, bool loadQuotes = true, bool sideWithQuotee = false, Plc? focalPostAuthor = null, CancellationToken ct = default)
        {
            if (posts.Length == 0) return posts;

            


            void OnPostDataAvailable(BlueskyRelationships rels, BlueskyPost post)
            {
                if (post.Data == null)
                {
                    (post.Data, post.InReplyToUser, post.RootPostDid) = rels.TryGetPostDataAndInReplyTo(rels.GetPostId(post.Author.Did, post.RKey, ctx), ctx);
                }

                BlueskyRelationships.MaybePropagateAdministrativeBlockToPost(post);

                if (loadQuotes)
                {
                    rels.PopulateQuotedPost(post, ctx);
                }

                if (post.Data?.InReplyToPlc != null && post.InReplyToUser == null)
                {
                    post.InReplyToUser = rels.GetProfile(new Plc(post.Data.InReplyToPlc.Value), ctx);
                }


                var author = post.Author.Plc;
                post.EmbedRecord = relationshipsUnlocked.TryGetAtObject(post.Data?.EmbedRecordUri, ctx);

                if (focalPostAuthor != null)
                {
                    post.FocalAndAuthorBlockReason = rels.UsersHaveBlockRelationship(focalPostAuthor.Value, post.AuthorId, ctx);
                }


                if (post.Data?.InReplyToPostId is { Author: { } inReplyToAuthor })
                {
                    post.ParentAndAuthorBlockReason = rels.UsersHaveBlockRelationship(inReplyToAuthor, author, ctx);

                    if (post.RootPostId.Author != inReplyToAuthor)
                    {
                        post.RootAndAuthorBlockReason = rels.UsersHaveBlockRelationship(post.RootPostId.Author, author, ctx);
                    }
                }



                post.Threadgate = rels.TryGetThreadgate(post.RootPostId);
                if (post.Threadgate != null)
                {
                    if (post.Threadgate.IsHiddenReply(post.PostId))
                    {
                        if (post.PostBlockReason == default)
                            post.PostBlockReason = PostBlockReasonKind.HiddenReply;
                        post.ViolatesThreadgate = true;
                    }
                    else if (!rels.ThreadgateAllowsUser(post.RootPostId, post.Threadgate, post.PostId.Author))
                    {
                        if (post.PostBlockReason == default)
                            post.PostBlockReason = PostBlockReasonKind.NotAllowlistedReply;
                        post.ViolatesThreadgate = true;
                    }
                }


                if (post.RootAndAuthorBlockReason != default)
                {
                    post.ViolatesThreadgate = true;
                }

                rels.PopulateViewerFlags(post, ctx);

                if (post.HasExternalThumbnailBestGuess)
                {
                    var domain = StringUtils.TryParseUri(post.ExternalLinkOrFirstLinkFacet!)?.GetDomainTrimWww();
                    if (domain != null && ExternalDomainsAlwaysCompactView.Contains(domain))
                        post.ShouldUseCompactView = true;
                }

                if (onPostDataAvailable != null)
                {
                    // The user callback must run outside the lock.
                    DispatchOutsideTheLock(() => onPostDataAvailable.Invoke(post));
                }
            }

            WithRelationshipsLock(rels =>
            {
                DangerousHugeReadOnlyMemory<BookmarkPostFirst>[]? userBookmarks = null;
                DangerousHugeReadOnlyMemory<Tid>[]? userDeletedBookmarks = null;

                foreach (var post in posts)
                {

                    if (ctx.IsLoggedIn)
                    {
                        var loggedInUser = ctx.LoggedInUser;
                        if (rels.Likes.HasActor(post.PostId, loggedInUser, out var likeTid))
                            post.IsLikedBySelf = likeTid.RelationshipRKey;
                        if (rels.Reposts.HasActor(post.PostId, loggedInUser, out var repostTid))
                            post.IsRepostedBySelf = repostTid.RelationshipRKey;

                        post.IsBookmarkedBySelf = rels.TryGetLatestBookmarkForPost(post.PostId, ctx.LoggedInUser, ref userBookmarks, ref userDeletedBookmarks);
                    }

                    if (loadQuotes)
                    {
                        rels.PopulateQuotedPost(post, ctx);
                        if (post.QuotedPost?.InReplyToUser != null)
                            rels.PopulateViewerFlags(post.QuotedPost.InReplyToUser, ctx);
                    }
                    if (post.InReplyToUser != null)
                        rels.PopulateViewerFlags(post.InReplyToUser, ctx);
                }

                foreach (var post in posts.Where(x => x.Data != null))
                {
                    OnPostDataAvailable(rels, post);
                }
            }, ctx);

            if (!IsReadOnly)
            {
                await AwaitWithShortDeadline(Task.WhenAll(posts.Where(x => x.Data == null).Select(async post =>
                {
                    var version = await FetchAndStorePostDict.GetValueAsync(post.PostIdStr, RequestContext.CreateForTaskDictionary(ctx));
                    ctx.BumpMinimumVersion(version);
                    WithRelationshipsLock(rels =>
                    {
                        OnPostDataAvailable(rels, post);
                    }, ctx);
                })), ctx);

                await AwaitWithShortDeadline(Task.WhenAll(posts.Where(x => x.RequiresLateOpenGraphData).Select(async post =>
                {
                    var externalUrl = post.ExternalLinkOrFirstLinkFacet!;
                    var version = await FetchAndStoreOpenGraphDict.GetValueAsync(externalUrl, RequestContext.CreateForTaskDictionary(ctx));
                    ctx.BumpMinimumVersion(version);
                    WithRelationshipsLock(rels =>
                    {
                        post.ApplyLateOpenGraphData(rels.GetOpenGraphData(externalUrl));
                    }, ctx);


                    onPostDataAvailable?.Invoke(post);
                })), ctx);


            }

            await EnrichAsync(posts.SelectMany(x => x.Labels ?? []).ToArray(), ctx);
            await EnrichAsync(posts.SelectMany(x => new[] { x.Author, x.InReplyToUser, x.RepostedBy, x.QuotedPost?.Author, x.QuotedPost?.InReplyToUser }).WhereNonNull().ToArray(), ctx, ct: ct);

            if (loadQuotes)
            {
                var r = posts.Select(x => x.QuotedPost).WhereNonNull().ToArray();
                if (r.Length != 0)
                {
                    await EnrichAsync(r, ctx, onPostDataAvailable, loadQuotes: false, focalPostAuthor: focalPostAuthor, ct: ct);
                    WithRelationshipsLock(rels =>
                    {
                        foreach (var quoter in posts)
                        {
                            var quoted = quoter.QuotedPost;
                            if (quoted == null) continue;
                            if (sideWithQuotee) quoter.QuoteeAndAuthorBlockReason = rels.UsersHaveBlockRelationship(quoter.PostId.Author, quoted.PostId.Author, ctx);
                            else quoted.QuoterAndAuthorBlockReason = rels.UsersHaveBlockRelationship(quoter.PostId.Author, quoted.PostId.Author, ctx);
                            var quotedPostgate = rels.TryGetPostgate(quoted.PostId);
                            if (quotedPostgate != null)
                            {
                                if (quotedPostgate.DetachedEmbeddings != null && quotedPostgate.DetachedEmbeddings.Any(x => x.PostId == quoter.PostId))
                                {
                                    if (sideWithQuotee) quoter.PostBlockReason = PostBlockReasonKind.RemovedByQuoteeOnQuoter;
                                    else quoted.PostBlockReason = PostBlockReasonKind.RemovedByQuotee;
                                }
                                else if (quotedPostgate.DisallowQuotes && quoter.AuthorId != quoted.AuthorId)
                                {
                                    if (sideWithQuotee) quoter.PostBlockReason = PostBlockReasonKind.DisabledQuotesOnQuoter;
                                    else quoted.PostBlockReason = PostBlockReasonKind.DisabledQuotes;
                                }
                            }

                        }
                    }, ctx);
                }
            }

            return posts;
        }



        public void DispatchOutsideTheLock(Action value)
        {
            if (DangerousUnlockedRelationships.IsLockHeld)
                Task.Run(value);
            else
                value();
        }



        private static ATIdentifier GetAtId(string did)
        {
            return ATIdentifier.Create(did)!;
        }

        public record struct CachedSearchResult(BlueskyPost? Post, long LikeCount);

        private Dictionary<DuckDbUuid, SearchSession> recentSearches = new();
        private class SearchSession
        {
            public Stopwatch LastSeen = Stopwatch.StartNew();
            public List<(PostId[] Posts, int NextContinuationMinLikes)> Pages = new();
            public ConcurrentDictionary<PostId, CachedSearchResult> AlreadyProcessed = new();

        }

        public async Task<PostsAndContinuation> SearchTopPostsAsync(PostSearchOptions options, RequestContext ctx, int limit = 0, string? continuation = null)
        {
            EnsureLimit(ref limit, 30);
            options = await InitializeSearchOptionsAsync(options, ctx);

            var cursor = continuation != null ? TopPostSearchCursor.Deserialize(continuation) : new TopPostSearchCursor(65536, Guid.NewGuid(), 0);
            var minLikes = cursor.MinLikes;

            SearchSession? searchSession;
            lock (recentSearches)
            {
                if (continuation == null)
                {
                    recentSearches[cursor.SearchId] = searchSession = new();
                }
                else
                {
                    if (!recentSearches.TryGetValue(cursor.SearchId, out searchSession))
                    {
                        // Search session expired. Approximate a new one.
                        cursor = new TopPostSearchCursor(cursor.MinLikes, Guid.NewGuid(), 0);
                        recentSearches[cursor.SearchId] = searchSession = new();
                    }
                }
            }

            lock (searchSession)
            {
                searchSession.LastSeen.Restart();
            }



            if (recentSearches.Count >= 10000)
            {
                foreach (var item in recentSearches.ToArray())
                {
                    if (item.Value.LastSeen.Elapsed.TotalMinutes >= 30)
                        recentSearches.Remove(item.Key, out _);
                }
            }

            BlueskyPost[]? mandatoryPosts = null;
            TopPostSearchCursor? mandatoryNextContinuation = null;
            lock (searchSession)
            {
                if (cursor.PageIndex < searchSession.Pages.Count)
                {
                    var page = searchSession.Pages[cursor.PageIndex];
                    mandatoryPosts = WithRelationshipsLock(rels => page.Posts.Select(x => rels.GetPost(x)).ToArray(), ctx);
                    mandatoryNextContinuation = page.NextContinuationMinLikes != -1 ? new TopPostSearchCursor(page.NextContinuationMinLikes, cursor.SearchId, cursor.PageIndex + 1) : null;
                }
            }
            if (mandatoryPosts != null)
            {
                await EnrichAsync(mandatoryPosts, ctx);
                return (mandatoryPosts, mandatoryNextContinuation?.Serialize());
            }

            bool HasEnoughPrefetchedResults() => searchSession.AlreadyProcessed.Count(x => x.Value.LikeCount != -1) > limit; // strictly greater (we want limit + 1)


            if (!HasEnoughPrefetchedResults())
            {
                while (true)
                {
                    LogInfo("Try top search with minLikes: " + minLikes);
                    var latest = await SearchLatestPostsAsync(options with { MinLikes = Math.Max(minLikes, options.MinLikes) }, ctx, limit: limit * 2, enrichOutput: false, alreadyProcessedPosts: searchSession.AlreadyProcessed);
                    if (latest.Posts.Length != 0)
                    {
                        foreach (var post in latest.Posts)
                        {
                            searchSession.AlreadyProcessed[post.PostId] = new CachedSearchResult(post, post.LikeCount);
                        }
                        if (HasEnoughPrefetchedResults()) break;
                    }
                    if (minLikes == 0) break;
                    if (minLikes == 1) minLikes = 0;
                    else minLikes = minLikes / 2;
                    if (minLikes < options.MinLikes / 2) break;
                }
            }

            var resultCore = searchSession.AlreadyProcessed
                .Where(x => x.Value.LikeCount != -1)
                .OrderByDescending(x => x.Value.LikeCount)
                .Take(limit + 1)
                .ToArray();
            var result = WithRelationshipsLock(rels => resultCore.Select(x => x.Value.Post ?? rels.GetPost(x.Key)).ToArray(), ctx);


            var hasMorePages = result.Length > limit;
            if (hasMorePages)
                result = result.AsSpan(0, limit).ToArray();

            bool tryAgainAlreadyProcessed = false;
            lock (searchSession)
            {
                var pages = searchSession.Pages;
                if (cursor.PageIndex < pages.Count)
                {
                    // A concurrent request for the same cursor occurred.
                    tryAgainAlreadyProcessed = true;
                }
                else if (cursor.PageIndex == pages.Count)
                {
                    pages.Add((result.Select(x => x.PostId).ToArray(), minLikes));
                    foreach (var p in result)
                    {
                        searchSession.AlreadyProcessed[p.PostId] = new CachedSearchResult(null, -1);
                    }
                }
                else AssertionLiteException.Throw("SearchTopPostsAsync: cursor pageIndex not within boundary");
            }
            if (tryAgainAlreadyProcessed)
            {
                return await SearchTopPostsAsync(options, ctx, limit, continuation);
            }


            await EnrichAsync(result, ctx);
            return (result, hasMorePages ? new TopPostSearchCursor(minLikes, cursor.SearchId, cursor.PageIndex + 1).Serialize() : null);
        }

        public async Task<PostsAndContinuation> SearchLatestPostsAsync(PostSearchOptions options, RequestContext ctx, int limit = 0, string? continuation = null, ConcurrentDictionary<PostId, CachedSearchResult>? alreadyProcessedPosts = null, bool enrichOutput = true)
        {
            EnsureLimit(ref limit, 30);
            options = await InitializeSearchOptionsAsync(options, ctx);
            var until = options.Until;
            var query = options.Query;
            var author = options.Author != null ? SerializeSingleDid(options.Author, ctx) : default;
            var queryWords = StringUtils.GetDistinctWords(query);
            if (queryWords.Length == 0) return ([], null);
            var queryPhrases = StringUtils.GetExactPhrases(query);
            var tags = Regex.Matches(query!, @"#\w+\b").Select(x => x.Value.Substring(1).ToLowerInvariant()).ToArray();
            Regex[] hashtagRegexes = tags.Select(x => new Regex("#" + Regex.Escape(x) + "\\b", RegexOptions.IgnoreCase)).ToArray();

            PostIdTimeFirst? continuationParsed = continuation != null ? PostIdTimeFirst.Deserialize(continuation) : null;
            if (continuationParsed != null)
            {
                var continuationDate = continuationParsed.Value.PostRKey.Date;
                if (until == null || continuationDate < until) until = continuationDate;
            }

            bool IsMatch(string postText)
            {
                var postWords = StringUtils.GetDistinctWords(postText);
                if (queryWords.Any(x => !postWords.Contains(x))) return false;
                if (hashtagRegexes.Any(r => !r.IsMatch(postText))) return false;
                if (queryPhrases.Count != 0)
                {
                    var postAllWords = StringUtils.GetAllWords(postText).ToArray();
                    if (!queryPhrases.All(queryPhrase => ContainsExactPhrase(postAllWords, queryPhrase)))
                        return false;
                }
                return true;
            }
            var coreSearchTerms = queryWords.Select(x => x.ToString()).Where(x => !tags.Contains(x)).Concat(tags.Select(x => "#" + x));
            if (options.MinLikes > BlueskyRelationships.SearchIndexPopularityMinLikes)
            {
                coreSearchTerms = coreSearchTerms.Append(BlueskyRelationships.GetPopularityIndexConstraint("likes", options.MinLikes));
            }
            if (options.MinReposts > BlueskyRelationships.SearchIndexPopularityMinReposts)
            {
                coreSearchTerms = coreSearchTerms.Append(BlueskyRelationships.GetPopularityIndexConstraint("reposts", options.MinReposts));
            }
            var posts = WithRelationshipsLock(rels =>
            {
                if (author != default && !rels.IsAccountActive(author)) return [];
                return rels
                    .SearchPosts(coreSearchTerms.ToArray(), options.Since != null ? (ApproximateDateTime32)options.Since : default, until != null ? ((ApproximateDateTime32)until).AddTicks(1) : null, author, options.Language)
                    .DistinctAssumingOrderedInput(skipCheck: true)
                    .SelectMany(approxDate =>
                    {
                        var startPostId = new PostIdTimeFirst(Tid.FromDateTime(approxDate), default);
                        var endPostId = new PostIdTimeFirst(Tid.FromDateTime(approxDate.AddTicks(1)), default);

                        // TODO: these are not sorted
                        var postsCore = rels.PostData.GetInRangeUnsorted(startPostId, endPostId)
                            .Where(x =>
                            {
                                var date = x.Key.PostRKey.Date;
                                if (date < options.Since) return false;
                                if (until != null && date >= until) return false;
                                if (continuationParsed != null)
                                {
                                    if (x.Key.CompareTo(continuationParsed.Value) >= 0) return false;
                                }
                                return true;
                            });
                        // TODO: dedupe them

                        if (options.MinLikes > 0)
                        {
                            postsCore = postsCore.Where(x => rels.Likes.HasAtLeastActorCount(x.Key, options.MinLikes));
                        }
                        if (options.MinReposts > 0)
                        {
                            postsCore = postsCore.Where(x => rels.Reposts.HasAtLeastActorCount(x.Key, options.MinReposts));
                        }

                        var posts = postsCore
                            .Where(x =>
                            {
                                if (alreadyProcessedPosts != null)
                                {
                                    if (!alreadyProcessedPosts.TryAdd(x.Key, new CachedSearchResult(null, -1))) // Will be overwritten later with actual post, if it matches
                                        return false;
                                }
                                return true;
                            })
                            .Where(x => !rels.PostDeletions.ContainsKey(x.Key))
                            .Where(x => author != default ? x.Key.Author == author : true)
                            .Select(x => rels.GetPost(x.Key, BlueskyRelationships.DeserializePostData(x.Values.AsSmallSpan(), x.Key), ctx))
                            .Where(x => x.Data?.Error == null);

                        if (options.MediaOnly)
                            posts = posts.Where(x => x.Data!.Media != null);

                        if (options.Language != LanguageEnum.Unknown)
                            posts = posts.Where(x => x.Data!.Language == options.Language);

                        posts = posts
                            .Where(x => !x.Author.IsBlockedByAdministrativeRule && !rels.IsKnownMirror(x.Author.Did))
                            .Where(x => x.Data!.Text != null && IsMatch(x.Data.Text));
                        return posts;
                    })
                    .Select(x =>
                    {
                        x.InReplyToUser = x.InReplyToPostId != null ? rels.GetProfile(x.InReplyToPostId.Value.Author, ctx) : null;
                        return x;
                    })
                    .DistinctBy(x => x.PostId)
                    .Take(limit + 1)
                    .ToArray();
            }, ctx);
            if (enrichOutput)
                await EnrichAsync(posts, ctx);
            return (posts, posts.Length > limit ? posts.LastOrDefault()?.PostId.Serialize() : null);
        }

        private async Task<PostSearchOptions> InitializeSearchOptionsAsync(PostSearchOptions options, RequestContext ctx)
        {
            var q = options.Query;
            string? author = options.Author;
            DateTime? since = options.Since;
            DateTime? until = options.Until;
            int minReposts = options.MinReposts;
            int minLikes = options.MinLikes;

            StringUtils.ParseQueryModifiers(ref q, (k, v) =>
            {
                if (string.IsNullOrEmpty(v)) return false;
                if (k == "from")
                    author = v;
                else if (k == "since")
                    since = ParseDate(v);
                else if (k == "until")
                    until = ParseDate(v);
                else if (k == "min_reposts" || k == "min_retweets")
                    minReposts = int.Parse(v);
                else if (k == "min_likes" || k == "min_faves")
                    minLikes = int.Parse(v);
                else
                    return false;

                return true;
            });

            if (author == "me" && ctx.Session.IsLoggedIn == true)
                author = ctx.Session?.Did;

            if (author != null && author.StartsWith('@'))
                author = author.Substring(1);

            author = !string.IsNullOrEmpty(author) ? await this.ResolveHandleAsync(author, ctx) : null;
            return options with
            {
                Query = q,
                Author = author,
                Since = since,
                Until = until,
                MinLikes = minLikes,
                MinReposts = minReposts,
            };
        }

        private static DateTime? ParseDate(string v)
        {
            return DateTime.ParseExact(v, "yyyy-MM-dd", CultureInfo.InvariantCulture);
        }

        private static bool ContainsExactPhrase(string[] haystack, string[] needle)
        {
            for (int i = 0; i < haystack.Length - needle.Length; i++)
            {
                if (haystack.AsSpan(i, needle.Length).SequenceEqual(needle))
                    return true;
            }
            return false;
        }

        public async Task<BlueskyLabel[]> GetLabelerLabelsAsync(string did, RequestContext ctx)
        {
            var info = await FetchAndStoreLabelerServiceMetadataDict.GetValueAsync(did, RequestContext.CreateForTaskDictionary(ctx));
            ctx.BumpMinimumVersion(info.MinVersion);

            var labels = WithRelationshipsLock(rels => info.Labels.Select(x => rels.GetLabel(x, ctx)).ToArray(), ctx);
            await EnrichAsync(labels, ctx);
            return labels;
        }


        public async Task<PostsAndContinuation> GetUserPostsAsync(string did, bool includePosts, bool includeReplies, bool includeReposts, bool includeLikes, bool includeBookmarks, bool mediaOnly, int limit, string? continuation, RequestContext ctx)
        {
            EnsureLimit(ref limit);

            var profile = await GetProfileAsync(did, ctx);
            if (!profile.IsActive)
                return new();

            var defaultContinuation = new ProfilePostsContinuation(
                includePosts ? Tid.MaxValue : default,
                includeReposts ? Tid.MaxValue : default,
                includeLikes ? Tid.MaxValue : default,
                includeBookmarks ? Tid.MaxValue : default,
                []);

            BlueskyPost? GetPostIfRelevant(BlueskyRelationships rels, PostId postId, CollectionKind kind)
            {
                var post = rels.GetPost(postId, ctx);

                if (kind == CollectionKind.Posts)
                {
                    if (!includeReplies && !post.IsRootPost && !(post.PluggableProtocol?.ShouldIncludeFullReplyChain(post) == true)) return null;
                }

                if (mediaOnly && post.Data?.Media == null)
                    return null;
                return post;
            }

            var canFetchFromServer = BlueskyRelationships.IsNativeAtProtoDid(did);

            if (continuation == null && (includePosts || includeReposts))
            {
                var recentThreshold = Tid.FromDateTime(DateTime.UtcNow - (canFetchFromServer ? TimeSpan.FromDays(7) : BlueskyRelationships.TryGetPluggableProtocolForDid(did)!.GetProfilePageMaxPostAge()));
                var recentPosts = WithRelationshipsLock(rels =>
                {
                    var plc = rels.TrySerializeDidMaybeReadOnly(did, ctx);
                    if (plc == default) return [];
                    BlueskyProfile? profile = null;

                    var recentPosts = includePosts ? (
                        mediaOnly ? rels.EnumerateRecentMediaPosts(plc, Tid.FromDateTime(DateTime.UtcNow.AddDays(-90), 0), null).Select(x => (RKey: x.PostRKey, PostId: x, IsRepost: false)) :
                        rels.EnumerateRecentPosts(plc, recentThreshold, null).Select(x => (RKey: x.PostId.PostRKey, x.PostId, IsRepost: false))
                    ) : [];
                    var recentReposts = includeReposts ? rels.EnumerateRecentReposts(plc, recentThreshold, null).Select(x => (RKey: x.RepostRKey, x.PostId, IsRepost: true)) : [];

                    return rels.EnumerateFeedWithNormalization(
                        SimpleJoin.ConcatPresortedEnumerablesKeepOrdered([recentPosts, recentReposts], x => x.RKey, new ReverseComparer<Tid>())
                        .Select(x =>
                        {
                            var post = GetPostIfRelevant(rels, x.PostId, x.IsRepost ? CollectionKind.Reposts : CollectionKind.Posts);
                            if (post != null && x.IsRepost)
                            {
                                post.RepostedBy = profile ??= rels.GetProfile(plc, ctx);
                                post.RepostDate = x.RKey.Date;
                            }
                            return post;
                        })
                        .WhereNonNull()
                        .Take(canFetchFromServer && !mediaOnly ? 10 : 50),
                        ctx
                        )
                        .ToArray();
                }, ctx);

                await EnrichAsync(recentPosts, ctx);
                if (recentPosts.Length != 0)
                {

                    return (recentPosts, canFetchFromServer ? (defaultContinuation with { FastReturnedPosts = recentPosts.Select(x => x.PostIdStr).ToArray() }).Serialize() : null);
                }


            }

            var isSelf = ctx.IsLoggedIn && ctx.Session.Did == did;

            ProfilePostsContinuation parsedContinuation = continuation != null ? ProfilePostsContinuation.Deserialize(continuation) : defaultContinuation;
            var fastReturnedPostsSet = parsedContinuation.FastReturnedPosts.ToHashSet();

            MergeablePostEnumerator[] mergeableEnumerators = [

                new MergeablePostEnumerator(parsedContinuation.MaxTidPosts, async max =>
                {
                    var posts = await ListRecordsAsync(did, Post.RecordType, limit, max != Tid.MaxValue ? max.ToString() : null, ctx);
                    return posts.Records.Select(x =>
                    {
                        return TryGetPostReference(() => new PostReference(x.Uri.Rkey, new PostIdString(did, x.Uri.Rkey), (Post)x.Value));
                    }).Where(x => x != default).ToArray();
                }, CollectionKind.Posts),
                new MergeablePostEnumerator(parsedContinuation.MaxTidReposts, async max =>
                {
                    var posts = await ListRecordsAsync(did, Repost.RecordType, limit, max != Tid.MaxValue ? max.ToString() : null, ctx);
                    return posts.Records.Select(x =>
                    {
                        return TryGetPostReference(() => new PostReference(x.Uri.Rkey, BlueskyRelationships.GetPostIdStr(((Repost)x.Value).Subject!)));
                    }).Where(x => x != default).ToArray();
                }, CollectionKind.Reposts),
                new MergeablePostEnumerator(parsedContinuation.MaxTidLikes, async max =>
                {
                    var posts = await ListRecordsAsync(did, Like.RecordType, limit, max != Tid.MaxValue ? max.ToString() : null, ctx);

                    var likeRecords = posts.Records.Select(x =>
                    {
                        return (Record: x.Value as Like, Reference: TryGetPostReference(() => new PostReference(x.Uri.Rkey, BlueskyRelationships.GetPostIdStr(((Like)x.Value).Subject!))));
                    }).Where(x => x.Reference != default).ToArray();

                    if (isSelf)
                    {
                        var missing = WithRelationshipsLockForDids(likeRecords.Select(x => x.Reference.PostId.Did).ToArray(), (_, rels) => likeRecords.Where(x =>
                        {
                            return !rels.Likes.creations.Contains(new PostId(rels.SerializeDid(x.Reference.PostId.Did, ctx), Tid.Parse(x.Reference.PostId.RKey)), new Models.Relationship(ctx.LoggedInUser, Tid.Parse(x.Reference.RKey)));
                        }).ToArray(), ctx);
                        if(missing.Length != 0)
                        {

                            var indexer = new Indexer(this);
                            foreach(var item in missing)
                            {
                                indexer.OnRecordCreated(did, Like.RecordType + "/" + item.Reference.RKey, item.Record!);
                            }
                            ctx.BumpMinimumVersion(this.relationshipsUnlocked.Version);
                        }
                    }
                    return likeRecords.Select(x => x.Reference).ToArray();

                }, CollectionKind.Likes),
                new MergeablePostEnumerator(parsedContinuation.MaxTidBookmarks, max =>
                {
                    if(!ctx.IsLoggedIn || ctx.LoggedInUser != SerializeSingleDid(did, ctx)) return Task.FromResult<PostReference[]>([]);
                    var posts = GetBookmarks(limit, max != Tid.MaxValue ? max : null, ctx);
                    return Task.FromResult(posts.Select(x =>
                    {
                        return new PostReference(x.Bookmark.BookmarkRKey.ToString()!, new PostIdString(x.Did, x.Bookmark.PostId.PostRKey.ToString()!));
                    }).Where(x => x != default).ToArray());
                }, CollectionKind.Bookmarks),

            ];

            var iterationCount = 0;
            while (true)
            {
                iterationCount++;

                var nextPages = await Task.WhenAll(mergeableEnumerators.Select(x => x.GetNextPageAsync()));
                if (nextPages.All(x => x.Length == 0)) return ([], null);
                var safeOldest = nextPages.Where(x => x.Length != 0).Max(x => x[^1].RKey);

                var merged = nextPages
                    .SelectMany(x => x)
                    .Where(x => x.RKey.CompareTo(safeOldest) >= 0)
                    .DistinctBy(x => x.PostId)
                    .OrderByDescending(x => x.RKey)
                    .ToArray();


                for (int i = 0; i < mergeableEnumerators.Length; i++)
                {
                    var enumerator = mergeableEnumerators[i];
                    var lastPage = nextPages[i];
                    var hasMore =
                        enumerator.LastReturnedTid != default &&
                        (
                            (lastPage.Length != 0 && lastPage[^1].RKey.CompareTo(safeOldest) < 0) ||
                            !enumerator.RemoteEnumerationExhausted
                        );
                    if (hasMore)
                        enumerator.LastReturnedTid = safeOldest;
                    else
                        enumerator.LastReturnedTid = default;
                }

                var alreadyHasAllPosts = WithRelationshipsLock(rels =>
                {
                    return merged.All(x =>
                    {
                        var plc = rels.TrySerializeDidMaybeReadOnly(x.PostId.Did, ctx);
                        if (plc == default) return false;
                        if (x.PostRecord == null) return true; // Repost of a post we don't have. Nothing to write right now (we'll do it later in EnrichAsync)
                        return rels.PostData.ContainsKey(new PostIdTimeFirst(x.RKey, plc));
                    });
                }, ctx);


                BlueskyPost[] StoreAndGetAllPosts(BlueskyRelationships rels)
                {
                    var plc = rels.SerializeDid(did, ctx);
                    BlueskyProfile? profile = null;
                    return merged.Select(x =>
                    {
                        var postId = rels.GetPostId(x.PostId.Did, x.PostId.RKey, ctx);

                        if (!alreadyHasAllPosts && x.PostRecord != null && !rels.PostData.ContainsKey(postId))
                        {
                            rels.StorePostInfo(postId, x.PostRecord, x.PostId.Did, ctx);
                        }


                        var post = GetPostIfRelevant(rels, postId, x.Kind);
                        if (post != null)
                        {
                            if (fastReturnedPostsSet.Contains(post.PostIdStr)) return null;
                            if (x.Kind == CollectionKind.Reposts)
                            {
                                post.RepostedBy = (profile ??= rels.GetProfile(plc, ctx));
                                post.RepostDate = x.RKey.Date;
                            }
                            else if (x.Kind == CollectionKind.Likes)
                            {
                                post.RepostDate = x.RKey.Date;
                            }
                        }

                        return post;
                    }).WhereNonNull().ToArray();

                }

                var posts =
                    alreadyHasAllPosts
                        ? WithRelationshipsLock(StoreAndGetAllPosts, ctx)
                        : WithRelationshipsWriteLock(StoreAndGetAllPosts, ctx);


                var exceededIterations = iterationCount >= 5;

                if (posts.Length != 0 || mergeableEnumerators.All(x => x.LastReturnedTid == default) || exceededIterations)
                {
                    ProfilePostsContinuation? nextContinuation = mergeableEnumerators.Any(x => x.LastReturnedTid != default) ? new ProfilePostsContinuation(
                            mergeableEnumerators[0].LastReturnedTid,
                            mergeableEnumerators[1].LastReturnedTid,
                            mergeableEnumerators[2].LastReturnedTid,
                            mergeableEnumerators[3].LastReturnedTid,
                            parsedContinuation.FastReturnedPosts
                            ) : null;

                    if (posts.Length == 0 && exceededIterations)
                        nextContinuation = null;

                    posts = WithRelationshipsLock(rels => rels.EnumerateFeedWithNormalization(posts, ctx).ToArray(), ctx);
                    await EnrichAsync(posts, ctx);
                    return (posts, nextContinuation?.Serialize());
                }
            }




        }

        public (BookmarkDateFirst Bookmark, string Did)[] GetBookmarks(int limit, Tid? maxExclusive, RequestContext ctx)
        {
            return WithRelationshipsLock(rels =>
            {
                DangerousHugeReadOnlyMemory<Tid>[]? deletedBookmarks = null;
                return rels.RecentBookmarks.GetValuesSortedDescending(ctx.LoggedInUser, null, maxExclusive != null ? new BookmarkDateFirst(maxExclusive.Value, default) : null)
                    .Where(c =>
                    {
                        if (deletedBookmarks == null)
                        {
                            deletedBookmarks = rels.BookmarkDeletions.GetValuesChunked(ctx.LoggedInUser).ToArray();
                        }

                        if (deletedBookmarks.Any(chunk => chunk.AsSpan().BinarySearch(c.BookmarkRKey) >= 0))
                            return false;

                        return true;
                    })
                    .Take(limit)
                    .Select(x => (x, rels.GetDid(x.PostId.Author)))
                    .ToArray();
            }, ctx);
        }

        private static PostReference TryGetPostReference(Func<PostReference> func)
        {
            try
            {
                var reference = func();
                if (!Tid.TryParse(reference.RKey, out _)) return default;
                if (!Tid.TryParse(reference.PostId.RKey, out _)) return default;
                return reference;
            }
            catch (Exception ex)
            {
                LogLowImportanceException(ex);
                return default;
            }
        }

        public async Task<PostsAndContinuation> GetPostThreadAsync(string did, string rkey, int limit, string? continuation, RequestContext ctx)
        {
            EnsureLimit(ref limit, 100);
            var thread = new List<BlueskyPost>();
            var focalPost = GetSinglePost(did, rkey, ctx);
            if (focalPost!.Data == null && !BlueskyRelationships.IsNativeAtProtoDid(did))
                throw new Exception("Post not found.");

            var focalPostId = new PostId(new Plc(focalPost.Author.PlcId), Tid.Parse(focalPost.RKey));

            if (continuation == null)
            {
                thread.Add(focalPost);

                await EnrichAsync([focalPost], ctx);

                var loadedBefore = 0;
                var before = 0;
                while (thread[0].InReplyToPostId is { } inReplyToPostId)
                {
                    var prepend = WithRelationshipsLock(rels => rels.GetPost(inReplyToPostId, ctx), ctx);
                    if (before++ >= 20) break;
                    if (prepend.Data == null)
                    {
                        if (loadedBefore++ < 3)
                            await EnrichAsync([prepend], ctx);
                        else
                            break;
                    }
                    thread.Insert(0, prepend);

                }
                var opReplies = new List<BlueskyPost>();
                WithRelationshipsLock(rels =>
                {
                    void AddOpExhaustiveReplies(PostId p)
                    {
                        var children = rels.DirectReplies.GetDistinctValuesSorted(p).Where(x => x.Author == focalPostId.Author).OrderBy(x => x.PostRKey).ToArray();

                        foreach (var child in children)
                        {
                            opReplies.Add(rels.GetPost(child, ctx));
                            AddOpExhaustiveReplies(child);
                        }
                    }
                    AddOpExhaustiveReplies(focalPostId);
                }, ctx);
                thread.AddRange(opReplies);
            }


            var wantMore = Math.Max(1, limit - thread.Count) + 1;

            PostId? parsedContinuation = continuation != null ? PostIdTimeFirst.Deserialize(continuation) : null;
            var otherReplyGroups = WithRelationshipsLock(rels =>
            {
                var threadgate = focalPostId.Author == focalPost.RootPostId.Author ? rels.TryGetThreadgate(focalPost.RootPostId) : null;

                var groups = new List<List<BlueskyPost>>();

                var otherReplies = rels.DirectReplies.GetValuesSorted(focalPostId, parsedContinuation).Where(x => x.Author != focalPostId.Author).Take(wantMore).Select(x => rels.GetPost(x, ctx)).ToArray();
                foreach (var otherReply in otherReplies)
                {
                    var group = new List<BlueskyPost>();
                    group.Add(otherReply);
                    groups.Add(group);

                    if (rels.IsThreadReplyFullyVisible(otherReply, threadgate, ctx))
                    {
                        var lastAdded = otherReply;
                        while (lastAdded.ReplyCount != 0)
                        {
                            var subReplies = rels.DirectReplies.GetValuesUnsorted(lastAdded.PostId);
                            var bestSubReply = subReplies
                                .Where(x => x.Author == focalPostId.Author || x.Author == otherReply.AuthorId || otherReplies.Length == 1)
                                .Select(x => (PostId: x, LikeCount: rels.Likes.GetApproximateActorCount(x)))
                                .OrderByDescending(x => x.PostId.Author == focalPostId.Author)
                                .ThenByDescending(x => x.LikeCount)
                                .ThenByDescending(x => x.PostId.PostRKey.Date)
                                .Select(x => rels.GetPost(x.PostId, ctx))
                                .FirstOrDefault(x => rels.IsThreadReplyFullyVisible(x, threadgate, ctx));
                            if (bestSubReply == null) break;
                            lastAdded = bestSubReply;
                            group.Add(lastAdded);
                            if (otherReplies.Length >= 2 && group.Count >= 4) break;
                            if (otherReplies.Length >= 3 && group.Count >= 2) break;
                        }
                    }
                }

                return groups;
            }, ctx);

            string? nextContinuation = null;
            if (otherReplyGroups.Count == wantMore)
            {
                otherReplyGroups.RemoveAt(otherReplyGroups.Count - 1);
                nextContinuation = otherReplyGroups[^1][0].PostId.Serialize(); // continuation is exclusive, so UI-last instead of core-last
            }

            thread.AddRange(otherReplyGroups.OrderByDescending(x => x[0].LikeCount).ThenByDescending(x => x[0].Date).SelectMany(x => x));


            await EnrichAsync(thread.ToArray(), ctx, focalPostAuthor: focalPostId.Author);

            Task.Run(() =>
            {
                HashSet<Plc> wantFollowsFor = new();
                if (focalPost?.RootPostId is { } rootPostId)
                {
                    WithRelationshipsLock(rels =>
                    {
                        var threadgate = rels.TryGetThreadgate(rootPostId);
                        if (threadgate != null && threadgate.AllowlistedOnly)
                        {
                            if (threadgate.AllowFollowing)
                            {
                                wantFollowsFor.Add(rootPostId.Author);
                            }
                            if (threadgate.AllowFollowers)
                            {
                                foreach (var item in thread.Select(x => x.AuthorId).Where(x => x != rootPostId.Author))
                                {
                                    wantFollowsFor.Add(item);
                                }
                            }
                        }
                    }, ctx);
                }
                var nonUrgentCtx = RequestContext.ToNonUrgent(ctx);

                EnsureHaveCollectionsAsync(wantFollowsFor, RepositoryImportKind.Follows, nonUrgentCtx).FireAndForget();
                EnsureHaveBlocksForPostsAsync(thread, nonUrgentCtx).FireAndForget();

            }).FireAndForget(); // It would take too much time to wait

            return new(thread.ToArray(), nextContinuation);
        }

        public Task EnsureHaveBlocksForPostsAsync(List<BlueskyPost> thread, RequestContext ctx)
        {
            var users = thread.Select(x => x.AuthorId).Concat(thread.Where(x => x.QuotedPost != null).Select(x => x.QuotedPost!.AuthorId)).ToArray();
            return EnsureHaveBlocksForUsersAsync(users, ctx);
        }
        public async Task EnsureHaveBlocksForUsersAsync(Plc[] users, RequestContext ctx)
        {
            var tasks = users.Distinct().Select(x => EnsureHaveBlocksForUserAsync(x, ctx)).ToArray();
            await Task.WhenAll(tasks);
        }
        public async Task EnsureHaveBlocksForUserAsync(Plc user, RequestContext ctx)
        {
            var subscriptionsTasks = EnsureHaveCollectionAsync(user, RepositoryImportKind.BlocklistSubscriptions, ctx);
            var blockTask = EnsureHaveCollectionAsync(user, RepositoryImportKind.Blocks, ctx);
            await subscriptionsTasks;

            var subscribedBlocklistAuthors = WithRelationshipsLock(rels => rels.GetSubscribedBlockLists(user, ctx), ctx)
                .Select(x => x.Actor)
                .Distinct();

            var tasksToAwait = new List<Task>()
            {
                blockTask,
            };
            foreach (var blocklistAuthor in subscribedBlocklistAuthors)
            {
                tasksToAwait.Add(EnsureHaveCollectionAsync(blocklistAuthor, RepositoryImportKind.ListMetadata, ctx));
                tasksToAwait.Add(EnsureHaveCollectionAsync(blocklistAuthor, RepositoryImportKind.ListEntries, ctx));
            }

            await Task.WhenAll(tasksToAwait);
        }

        public BlueskyPost GetSinglePost(string did, string rkey, RequestContext ctx)
        {
            return WithRelationshipsLockForDid(did, (plc, rels) => rels.GetPost(plc, Tid.Parse(rkey), ctx), ctx);
        }

        public BlueskyProfile GetSingleProfile(string did, RequestContext ctx)
        {
            return WithRelationshipsLockForDid(did, (plc, rels) => rels.GetProfile(plc, ctx), ctx);
        }

        public Plc SerializeSingleDid(string did, RequestContext ctx)
        {
            return WithRelationshipsLockForDid(did, (plc, rels) => plc, ctx);
        }

        //private Dictionary<(string Did, string RKey), (BlueskyFeedGeneratorData Info, DateTime DateCached)> FeedDomainCache = new();

        public async Task<(BlueskyPost[] Posts, BlueskyFeedGenerator? Info, string? NextContinuation)> GetFeedAsync(string? did, string? rkey, string? continuation, RequestContext ctx, bool forGrid = false, int limit = default, Uri? customEndpoint = null)
        {
            EnsureLimit(ref limit, 30);
            var feedGenInfo = did != null ? await GetFeedGeneratorAsync(did, rkey!, ctx) : null;
            if (customEndpoint == null && !feedGenInfo!.Data!.ImplementationDid!.StartsWith("did:web:", StringComparison.Ordinal)) throw new NotSupportedException("Only did:web: feed implementations are supported.");

            var proxyViaPds = ctx.IsLoggedIn && customEndpoint == null;



            AtFeedSkeletonResponse postsJson;
            ATUri[] postsJsonParsed;


            try
            {
                if (proxyViaPds)
                {
                    var feed = (await PerformPdsActionAsync(x =>
                    {
                        return x.GetFeedAsync(new ATUri($"at://{did}/app.bsky.feed.generator/{rkey}"), limit, continuation);
                    }, ctx)).HandleResult()!;
                    postsJson = new AtFeedSkeletonResponse
                    {
                        cursor = feed.Cursor,
                        feed = feed.Feed.Select(x => new AtFeedSkeletonPost { post = x.Post.Uri.ToString() }).ToArray()
                    };
                }
                else
                {
                    var suffix = $"limit={limit}" + (continuation != null ? "&cursor=" + Uri.EscapeDataString(continuation) : null);
                    var endpoint = customEndpoint?.AbsoluteUri ?? $"https://{feedGenInfo!.Data!.ImplementationDid!.Substring(8)}/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://{Uri.EscapeDataString(did!)}/app.bsky.feed.generator/{Uri.EscapeDataString(rkey!)}";
                    var skeletonUrl = endpoint + (endpoint.Contains('?') ? '&' : '?') + suffix;

                    postsJson = (await DefaultHttpClient.GetFromJsonAsync<AtFeedSkeletonResponse>(skeletonUrl))!;
                }

                postsJsonParsed = postsJson.feed?.Select(x => new ATUri(x.post)).ToArray() ?? [];
            }
            catch (Exception ex)
            {
                throw await CreateExceptionMessageForExternalServerErrorAsync($"The feed provider", ex, did, null, ctx);
            }

            var posts = WithRelationshipsLockWithPreamble(
                rels =>
                {
                    var postIds = new List<PostId>();
                    foreach (var item in postsJsonParsed)
                    {
                        if (item.Collection != Post.RecordType) throw new UnexpectedFirehoseDataException("Incorrect collection for feed skeleton entry");
                        var author = rels.TrySerializeDidMaybeReadOnly(item.Did!.Handler, ctx);
                        if (author == default) return default;
                        postIds.Add(new PostId(author, Tid.Parse(item.Rkey)));
                    }
                    return PreambleResult.Create(postIds);
                },
                (p, rels) =>
                {
                    var posts = p.Select(x => rels.GetPost(x, ctx));
                    posts = rels.EnumerateFeedWithNormalization(posts, ctx, omitIfMuted: true);
                    return posts.ToArray();
                }, ctx);
            if (continuation == null && posts.Length == 0)
                throw new UnexpectedFirehoseDataException("The feed provider didn't return any results.");

            if (forGrid)
                ctx.IncreaseTimeout(TimeSpan.FromSeconds(3)); // the grid doesn't support automatic refresh
            return (await EnrichAsync(posts, ctx), feedGenInfo, !string.IsNullOrEmpty(postsJson.cursor) ? postsJson.cursor : null);
        }


        private async Task<Exception> CreateExceptionMessageForExternalServerErrorAsync(string subjectDisplayText, Exception ex, string? did, string? pds, RequestContext ctx)
        {
            if (ex is PermissionException) return ex;
            return new UnexpectedFirehoseDataException(await GetExceptionMessageForExternalServerErrorAsync(subjectDisplayText, ex, did, pds, ctx), ex);
        }
        private async Task<string?> GetExceptionMessageForExternalServerErrorAsync(string subjectDisplayText, Exception ex, string? did, string? pds, RequestContext ctx)
        {
            if (ex is ATNetworkErrorException at)
            {
                var code = at.AtError.Detail?.Error;
                if (code == "RecordNotFound")
                    return "This record was not found.";

                var message = at.AtError.Detail?.Message ?? at.AtError.Detail?.Error;
                if (string.IsNullOrEmpty(message)) return subjectDisplayText + " returned error " + at.AtError.StatusCode;

                if (message == "Repo not found" || message.StartsWith("Could not find repo:", StringComparison.Ordinal))
                {
                    if (did != null)
                    {
                        try
                        {
                            var accountState = await FetchAndStoreAccountStateFromPdsDict.GetValueAsync(did, RequestContext.CreateForTaskDictionary(ctx, possiblyUrgent: true));
                            if (!BlueskyRelationships.IsAccountActive(accountState.Value))
                                return DefaultLabels.GetErrorForAccountState(accountState.Value, pds);
                        }
                        catch (Exception ex2)
                        {
                            LogNonCriticalException(ex2);
                        }
                    }
                    return "This user no longer exists at the specified PDS.";
                }

                return subjectDisplayText + " returned error " + message;
            }
            if (ex is TaskCanceledException)
            {
                return subjectDisplayText + " did not respond in a timely fashion.";
            }
            if (ex is HttpRequestException http)
            {
                if (http.StatusCode != null)
                {
                    return subjectDisplayText + " returned status code " + (int)http.StatusCode + " " + http.StatusCode;
                }
                else
                {
                    return subjectDisplayText + " could not be reached: " + http.HttpRequestError;
                }
            }
            return subjectDisplayText + " could not be reached: " + ex.Message;
        }

        private async Task<BlueskyFeedGeneratorData> GetFeedGeneratorDataAsync(string did, string rkey, RequestContext ctx)
        {
            var (plc, result) = WithRelationshipsLockForDid(did, (plc, rels) =>
            {
                return (plc, rels.TryGetFeedGeneratorData(new(plc, rkey)));
            }, ctx);

            var now = DateTime.UtcNow;

            if (result == null || ((now - result.RetrievalDate).TotalHours > 6 && !IsReadOnly))
            {
                var recordOutput = await GetRecordAsync(did, Generator.RecordType, rkey, ctx);
                var generator = (Generator)recordOutput!.Value!;
                WithRelationshipsWriteLock(rels =>
                {
                    rels.IndexFeedGenerator(plc, rkey, (Generator)recordOutput.Value, DateTime.UtcNow);
                    result = rels.TryGetFeedGeneratorData(new(plc, rkey));
                }, ctx);
            }

            return result!;
        }

        public async Task<PostsAndContinuation> GetFirehosePostsAsync(DateTime? maxDate, bool includeReplies, string? continuation, RequestContext ctx)
        {
            var limit = 30;
            PostIdTimeFirst? maxPostIdExclusive = continuation != null ? PostIdTimeFirst.Deserialize(continuation) : maxDate != null ? new PostIdTimeFirst(Tid.FromDateTime(maxDate.Value), default) : null;
            var posts = WithRelationshipsLock(rels =>
            {
                var now = DateTime.UtcNow;
                var enumerables = rels.PostData.slices.Select(slice =>
                {
                    return rels.GetFirehosePosts(slice, maxPostIdExclusive, now, ctx); //.AssertOrderedAllowDuplicates(x => (PostIdTimeFirst)x.PostId, new DelegateComparer<PostIdTimeFirst>((a, b) => b.CompareTo(a)));
                })
                .Append(rels.PostData.QueuedItems.Where(x => x.Key.PostRKey.Date < now).Where(x => (!maxPostIdExclusive.HasValue || x.Key.CompareTo(maxPostIdExclusive.Value) < 0) && !rels.PostDeletions.ContainsKey(x.Key)).OrderByDescending(x => x.Key).Take(limit).Select(x => rels.GetPost((PostId)x.Key, BlueskyRelationships.DeserializePostData(x.Values.AsUnsortedSpan(), x.Key), ctx)))
                .ToArray();

                var merged = SimpleJoin.ConcatPresortedEnumerablesKeepOrdered(enumerables, x => (PostIdTimeFirst)x.PostId, new DelegateComparer<PostIdTimeFirst>((a, b) => b.CompareTo(a)))
                    .Where(x => !x.Author.IsBlockedByAdministrativeRule);

                //  .AssertOrderedAllowDuplicates(x => (PostIdTimeFirst)x.PostId, new DelegateComparer<PostIdTimeFirst>((a, b) => b.CompareTo(a)));
                if (!includeReplies)
                    merged = merged.Where(x => x.IsRootPost && (x.Data?.IsReplyToUnspecifiedPost != true));
                return merged
                    .Take(limit)
                    .ToArray();
            }, ctx);
            await EnrichAsync(posts, ctx);
            return (posts, posts.LastOrDefault()?.PostId.Serialize());
        }

        public async Task<ProfilesAndContinuation> GetPostLikersAsync(string did, string rkey, string? continuation, int limit, RequestContext ctx)
        {
            EnsureLimit(ref limit, 50);
            var profiles = WithRelationshipsLockForDid(did, (plc, rels) => rels.GetPostLikers(plc, rkey, DeserializeRelationshipContinuation(continuation), limit + 1), ctx);
            var nextContinuation = SerializeRelationshipContinuation(profiles, limit);
            SortByDescendingRelationshipRKey(ref profiles);
            //DeterministicShuffle(profiles, did + rkey);
            await EnrichAsync(profiles, ctx);
            return (profiles, nextContinuation);
        }

        private static void SortByDescendingRelationshipRKey(ref BlueskyProfile[] profiles)
        {
            // Only a best effort approach (pagination will return items sorted by PLC)
            // Within a page, we sort by date instead.
            profiles = profiles.OrderByDescending(x => x.RelationshipRKey!.Value).ToArray();
        }
        private static void SortByDescendingRelationshipRKey(ref BlueskyPost[] posts)
        {
            // Only a best effort approach (pagination will return items sorted by PLC)
            // Within a page, we sort by date instead.
            posts = posts.OrderByDescending(x => x.Date).ToArray();
        }

        public async Task<ProfilesAndContinuation> GetPostRepostersAsync(string did, string rkey, string? continuation, int limit, RequestContext ctx)
        {
            EnsureLimit(ref limit, 50);
            var profiles = WithRelationshipsLockForDid(did, (plc, rels) => rels.GetPostReposts(plc, rkey, DeserializeRelationshipContinuation(continuation), limit + 1), ctx);
            var nextContinuation = SerializeRelationshipContinuation(profiles, limit);
            SortByDescendingRelationshipRKey(ref profiles);
            //DeterministicShuffle(profiles, did + rkey);
            await EnrichAsync(profiles, ctx);
            return (profiles, nextContinuation);
        }

        public async Task<PostsAndContinuation> GetPostQuotesAsync(string did, string rkey, string? continuation, int limit, RequestContext ctx)
        {
            EnsureLimit(ref limit, 30);
            var posts = WithRelationshipsLockForDid(did, (plc, rels) => rels.GetPostQuotes(plc, rkey, continuation != null ? PostId.Deserialize(continuation) : default, limit + 1, ctx), ctx);
            var nextContinuation = SerializeRelationshipContinuationPlcFirst(posts, limit);
            SortByDescendingRelationshipRKey(ref posts);
            //DeterministicShuffle(posts, did + rkey);
            await EnrichAsync(posts, ctx, sideWithQuotee: true);
            return (posts, nextContinuation);
        }


        public async Task<ProfilesAndContinuation> GetFollowersAsync(string did, string? continuation, int limit, RequestContext ctx)
        {
            EnsureLimit(ref limit, 50);
            var profiles = WithRelationshipsLockForDid(did, (plc, rels) => rels.GetFollowers(plc, DeserializeRelationshipContinuation(continuation), limit + 1), ctx);
            var nextContinuation = SerializeRelationshipContinuation(profiles, limit);
            SortByDescendingRelationshipRKey(ref profiles);
            //DeterministicShuffle(profiles, did);
            await EnrichAsync(profiles, ctx);
            return (profiles, nextContinuation);
        }

        public async Task<ProfilesAndContinuation> GetBlockedByAsync(string did, string? continuation, int limit, RequestContext ctx)
        {
            EnsureLimit(ref limit, 50);
            var profiles = WithRelationshipsLockForDid(did, (plc, rels) => rels.GetBlockedBy(plc, DeserializeRelationshipContinuation(continuation), limit + 1), ctx);
            var nextContinuation = SerializeRelationshipContinuation(profiles, limit);
            SortByDescendingRelationshipRKey(ref profiles);
            await EnrichAsync(profiles, ctx);
            return (profiles, nextContinuation);
        }


        private static void DeterministicShuffle<T>(T[] items, string seed)
        {
            // The values in the multidictionary are sorted by (Plc,RKey) for each key, and we don't want to prioritize always the same accounts
            // This is not a perfect solution, since if there are more than "limit" accounts in such value list, we'll always prioritize those "limit" first.
            new Random((int)System.IO.Hashing.XxHash32.HashToUInt32(MemoryMarshal.AsBytes<char>(seed))).Shuffle(items);
        }

        private static void EnsureLimit(ref int limit, int defaultLimit = 50)
        {
            if (limit <= 0) limit = defaultLimit;
            limit = Math.Min(limit, 200);
        }

        private static string? SerializeRelationshipContinuation<T>(T[] items, int limit, Func<T, string> serialize)
        {
            if (items.Length == 0) return null;
            if (items.Length <= limit) return null; // we request limit + 1
            var last = items[^1];
            return serialize(last);
        }
        private static string? SerializeRelationshipContinuation(BlueskyProfile[] actors, int limit)
        {
            return SerializeRelationshipContinuation(actors, limit, last => new Models.Relationship(new Plc(last.PlcId), last.RelationshipRKey!.Value).Serialize());
        }

        private static string? SerializeRelationshipContinuationPlcFirst(BlueskyPost[] posts, int limit)
        {
            return SerializeRelationshipContinuation(posts, limit, last => last.PostId.Serialize());
        }

        private static Models.Relationship DeserializeRelationshipContinuation(string? continuation)
        {
            return continuation != null ? Models.Relationship.Deserialize(continuation) : default;
        }

        public async Task<BlueskyFullProfile> GetFullProfileAsync(string did, RequestContext ctx, int followersYouFollowToLoad)
        {
            RssRefreshInfo? rssFeedInfo = null;
            if (did.StartsWith(AppViewLite.PluggableProtocols.Rss.RssProtocol.DidPrefix, StringComparison.Ordinal))
            {
                rssFeedInfo = await AppViewLite.PluggableProtocols.Rss.RssProtocol.Instance!.MaybeRefreshFeedAsync(did, ctx);
            }

            if (WithRelationshipsLockForDid(did, (plc, rels) => !rels.IsAccountActive(plc), ctx))
            {
                // double check from the PDS, in case we missed reactivation events.
                try
                {
                    var accountState = await FetchAndStoreAccountStateFromPdsDict.GetValueAsync(did, RequestContext.CreateForTaskDictionary(ctx, possiblyUrgent: true));
                    accountState.BumpMinimumVersion(ctx);
                }
                catch (Exception ex)
                {
                    LogNonCriticalException("Could not check with PDS if account is actually still deactivated", ex);
                }
            }

            var profile = WithRelationshipsLockForDid(did, (plc, rels) => rels.GetFullProfile(plc, ctx, followersYouFollowToLoad), ctx);

            Task? hasListsOrFeedsTask = null;
            if (profile.Profile.PluggableProtocol == null)
            {
                var fetchListsTask = EnsureHaveCollectionAsync(profile.Profile.Plc, RepositoryImportKind.ListMetadata, ctx);
                var fetchFeedGeneratorsTask = EnsureHaveCollectionAsync(profile.Profile.Plc, RepositoryImportKind.FeedGenerators, ctx);
                fetchListsTask.GetAwaiter().OnCompleted(() =>
                {
                    if (fetchListsTask is { IsCompletedSuccessfully: true, Result: { TotalRecords: > 0 } })
                        profile.HasLists = true;
                });
                fetchFeedGeneratorsTask.GetAwaiter().OnCompleted(() =>
                {
                    if (fetchFeedGeneratorsTask is { IsCompletedSuccessfully: true, Result: { TotalRecords: > 0 } })
                        profile.HasFeeds = true;
                });
                hasListsOrFeedsTask = Task.WhenAny(Task.WhenAll(fetchListsTask, fetchFeedGeneratorsTask), Task.Delay(700));
            }
            await EnrichAsync([profile.Profile, .. profile.FollowedByPeopleYouFollow?.Take(followersYouFollowToLoad) ?? []], ctx);
            if (profile.Profile.BasicData == null)
            {
                ctx.IncreaseTimeout();
                await EnrichAsync([profile.Profile], ctx);
            }
            if (hasListsOrFeedsTask != null)
                await hasListsOrFeedsTask;
            profile.RssFeedInfo = rssFeedInfo;
            if (profile.Profile.PluggableProtocol == null)
                Task.Run(() => EnsureHaveBlocksForUserAsync(profile.Profile.Plc, RequestContext.ToNonUrgent(ctx))).FireAndForget();
            return profile;
        }
        public async Task<BlueskyProfile> GetProfileAsync(string did, RequestContext ctx)
        {
            var profile = GetSingleProfile(did, ctx);
            await EnrichAsync([profile], ctx);
            return profile;
        }
        public async Task<BlueskyProfile[]> GetProfilesAsync(string[] dids, RequestContext ctx, Action<BlueskyProfile>? onProfileDataAvailable = null)
        {
            var profiles = WithRelationshipsUpgradableLock(rels => dids.Select(x => rels.GetProfile(rels.SerializeDid(x, ctx), ctx)).ToArray(), ctx);
            await EnrichAsync(profiles, ctx, onProfileDataAvailable);
            return profiles;
        }
        public async Task PopulateFullInReplyToAsync(BlueskyPost[] posts, RequestContext ctx)
        {
            WithRelationshipsLock(rels =>
            {
                foreach (var post in posts)
                {
                    if (post.IsReply)
                    {
                        post.InReplyToFullPost = rels.GetPost(post.InReplyToPostId!.Value);
                        if (post.Data!.RootPostId == post.InReplyToPostId)
                        {
                            post.RootFullPost = post.InReplyToFullPost;
                        }
                        else
                        {
                            post.RootFullPost = rels.GetPost(post.Data!.RootPostId);
                        }

                    }
                }
            }, ctx);
            await EnrichAsync([.. posts.Select(x => x.InReplyToFullPost).WhereNonNull(), .. posts.Select(x => x.RootFullPost).WhereNonNull()!], ctx);
        }

        public async Task<BlueskyFeedGenerator> GetFeedGeneratorAsync(string did, string rkey, RequestContext ctx)
        {
            var data = await GetFeedGeneratorDataAsync(did, rkey, ctx);
            return WithRelationshipsLockForDid(did, (plc, rels) => rels.GetFeedGenerator(plc, data, ctx), ctx);
        }

        public async Task<(BlueskyNotification[] NewNotifications, BlueskyNotification[] OldNotifications, Notification NewestNotification)> GetNotificationsAsync(RequestContext ctx, bool dark)
        {
            if (!ctx.IsLoggedIn) return ([], [], default);
            var session = ctx.Session;
            var user = session.LoggedInUser!.Value;

            var notifications = WithRelationshipsLock(rels => rels.GetNotificationsForUser(user, ctx, dark), ctx);
            var nonHiddenNotifications = notifications.NewNotifications.Concat(notifications.OldNotifications).Where(x => !x.Hidden).ToArray();
            await Task.WhenAll([
                EnrichAsync(nonHiddenNotifications.Select(x => x.Post).WhereNonNull().ToArray(), ctx),
                EnrichAsync(nonHiddenNotifications.Select(x => x.Profile).WhereNonNull().ToArray(), ctx),
                EnrichAsync(nonHiddenNotifications.Select(x => x.List).WhereNonNull().ToArray(), ctx),
                EnrichAsync(nonHiddenNotifications.Select(x => x.Feed).WhereNonNull().ToArray(), ctx)
            ]);
            return (notifications.NewNotifications, notifications.OldNotifications, notifications.NewestNotification);
        }

        public async Task<(CoalescedNotification[] NewNotifications, CoalescedNotification[] OldNotifications, Notification NewestNotification)> GetCoalescedNotificationsAsync(RequestContext ctx, bool dark)
        {
            var rawNotifications = await GetNotificationsAsync(ctx, dark);

            return (
                CoalesceNotifications(rawNotifications.NewNotifications, areNew: true),
                CoalesceNotifications(rawNotifications.OldNotifications, areNew: false),
                rawNotifications.NewestNotification
            );

        }

        private static CoalescedNotification[] CoalesceNotifications(BlueskyNotification[] rawNotifications, bool areNew)
        {
            if (rawNotifications.Length == 0) return [];
            var coalescedList = new List<CoalescedNotification>();

            foreach (var raw in rawNotifications)
            {
                if (raw.Hidden) continue;
                var key = raw.CoalesceKey;

                bool reused = false;
                var inspected = 0;
                for (int i = coalescedList.Count - 1; i >= 0; i--)
                {
                    var reuse = coalescedList[i];
                    if ((reuse.LatestDate - raw.EventDate).TotalHours > 18) break;
                    inspected++;
                    if (reuse.CoalesceKey == key)
                    {
                        if (raw.Profile != null)
                            reuse.Profiles.Add(raw.Profile);
                        reused = true;
                        break;
                    }
                    if (inspected >= 20) break;
                }

                if (reused) continue;

                var c = new CoalescedNotification
                {
                    PostId = key.PostId,
                    Kind = key.Kind,
                    FeedRKeyHash = key.FeedRKeyHash,
                    ListRKey = key.ListRKey,

                    LatestDate = raw.EventDate,
                    Post = raw.Post,
                    Feed = raw.Feed,
                    List = raw.List,
                    IsNew = areNew,
                    Profiles = raw.Profile != null ? [raw.Profile] : [],
                };
                coalescedList.Add(c);


            }
            return coalescedList.ToArray();
        }

        public string? GetCustomEmojiUrl(CustomEmoji emoji, ThumbnailSize size)
        {
            var url = new Uri(emoji.Url);
            return GetImageUrl(size, "host:" + url.Host, Encoding.UTF8.GetBytes(url.PathAndQuery), null, emoji.ShortCode?.Replace(":", null));
        }

        public string? GetAvatarUrl(string did, byte[]? avatarCid, string? pds, string? fileNameForDownload = null)
        {
            return GetImageUrl(ThumbnailSize.avatar_thumbnail, did, avatarCid, pds, fileNameForDownload);
        }
        public string? GetImageThumbnailUrl(string did, byte[]? cid, string? pds, string? fileNameForDownload = null)
        {
            return GetImageUrl(ThumbnailSize.feed_thumbnail, did, cid, pds, fileNameForDownload);
        }
        public string? GetImageBannerUrl(string did, byte[]? cid, string? pds, string? fileNameForDownload = null)
        {
            return GetImageUrl(ThumbnailSize.banner, did, cid, pds, fileNameForDownload);
        }
        public string? GetImageFullUrl(string did, byte[] cid, string? pds, string? fileNameForDownload = null)
        {
            return GetImageUrl(ThumbnailSize.feed_fullsize, did, cid, pds, fileNameForDownload);
        }
        public string? GetVideoThumbnailUrl(string did, byte[] cid, string? pds, string? fileNameForDownload = null)
        {
            return GetImageUrl(ThumbnailSize.video_thumbnail, did, cid, pds, fileNameForDownload);
        }
        public string? GetVideoBlobUrl(string did, byte[] cid, string? pds, string? fileNameForDownload = null)
        {
            return GetImageUrl(ThumbnailSize.feed_video_blob, did, cid, pds, fileNameForDownload);
        }


        public static string? CdnPrefix = AppViewLiteConfiguration.GetString(AppViewLiteParameter.APPVIEWLITE_CDN) is string { } s ? (s.Contains('/') ? s : "https://" + s) : null;
        public static bool ServeImages = AppViewLiteConfiguration.GetBool(AppViewLiteParameter.APPVIEWLITE_SERVE_IMAGES) ?? (CdnPrefix != null);

        public string? GetImageUrl(ThumbnailSize size, string did, byte[]? cid, string? pds, string? fileNameForDownload = null, bool forceProxy = false)
        {
            if (cid == null) return null;
            var cdn = CdnPrefix;

            if (AdministrativeBlocklist.ShouldBlockOutboundConnection(did)) return null;
            if (AdministrativeBlocklist.ShouldBlockOutboundConnection(DidDocProto.GetDomainFromPds(pds))) return null;

            var isNativeAtProto = BlueskyRelationships.IsNativeAtProtoDid(did);

            if (!ServeImages)
            {
                if (
                    isNativeAtProto &&
                    !forceProxy &&
                    cdn == null &&
                    size != ThumbnailSize.feed_video_blob &&
                    !DidDocOverrides.GetValue().CustomDidDocs.ContainsKey(did)
                    ) cdn = "https://cdn.bsky.app";
            }

            string cidString;
            if (isNativeAtProto)
            {
                try
                {
                    cidString = Cid.Read(cid).ToString();
                }
                catch (Exception)
                {
                    return null;
                }
            }
            else
            {
                cidString = Ipfs.Base32.ToBase32(cid);
            }

            if (size is ThumbnailSize.video_thumbnail or ThumbnailSize.feed_video_playlist or ThumbnailSize.feed_video_blob)
            {

                if (isNativeAtProto && size is ThumbnailSize.feed_video_playlist or ThumbnailSize.video_thumbnail)
                {
                    cdn = "https://video.bsky.app";
                }
                else if (size == ThumbnailSize.feed_video_playlist)
                {
                    if (!BlueskyRelationships.TryGetPluggableProtocolForDid(did)!.ShouldUseM3u8ForVideo(did, cid))
                        size = ThumbnailSize.feed_video_blob;
                }

                string format = (size == ThumbnailSize.video_thumbnail ? "thumbnail.jpg" : size == ThumbnailSize.feed_video_blob ? "video.mp4" : "playlist.m3u8");

                return $"{cdn}/watch/{Uri.EscapeDataString(did)}/{cidString}/{format}" + GetQueryStringForImageUrl(pds, fileNameForDownload, cdn);
            }

            return $"{cdn}/img/{size}/plain/{did}/{cidString}@jpeg" + GetQueryStringForImageUrl(pds, fileNameForDownload, cdn);
        }

        private static string? GetQueryStringForImageUrl(string? pds, string? fileNameForDownload, string? cdn)
        {
            if (cdn != null && cdn.EndsWith(".bsky.app", StringComparison.Ordinal)) return null;

            if (pds != null && pds.StartsWith("https://", StringComparison.Ordinal)) pds = pds.Substring(8);

            return "?pds=" + Uri.EscapeDataString(pds ?? string.Empty) + "&name=" + Uri.EscapeDataString(fileNameForDownload ?? string.Empty);
        }

        public long GetNotificationCount(AppViewLiteSession ctx, RequestContext reqCtx, bool dark)
        {
            if (!ctx.IsLoggedIn) return 0;
            return WithRelationshipsLock(rels => rels.GetNotificationCount(ctx.LoggedInUser!.Value, dark), reqCtx);
        }



        public async Task<PostsAndContinuation> GetFollowingFeedAsync(string? continuation, int limit, bool atProtoOnlyPosts, RequestContext ctx)
        {
            ctx.IsStillFollowedCached ??= new();
            EnsureLimit(ref limit, 50);
            Tid? maxTid = continuation != null ? Tid.Parse(continuation) : null;
            var alreadyReturned = new HashSet<PostId>();
            var posts = WithRelationshipsLock(rels =>
            {
                var posts = rels.EnumerateFollowingFeed(ctx, DateTime.Now.AddDays(-7), maxTid, atProtoOnlyPosts);
                var normalized = rels.EnumerateFeedWithNormalization(posts, ctx, alreadyReturned, omitIfMuted: true);
                return normalized.Take(limit).ToArray();
            }, ctx);
            await EnrichAsync(posts, ctx);
            return new PostsAndContinuation(posts, posts.Length != 0 ? posts[^1].PostId.PostRKey.ToString() : null);
        }



        record struct ScoredBlueskyPostWithSource(ScoredBlueskyPost Post, QueueWithOwner<ScoredBlueskyPost> Source);

        public async Task<PostsAndContinuation> GetBalancedFollowingFeedAsync(string? continuation, int limit, RequestContext ctx)
        {
            ctx.IsStillFollowedCached ??= new();

            EnsureLimit(ref limit, 20);
            var scoredSampler = GetScorer(ctx);

            var now = DateTime.UtcNow;
            var minDate = now - BlueskyRelationships.BalancedFeedMaximumAge;

            var loggedInUser = ctx.LoggedInUser;

            var (possibleFollows, users) = WithRelationshipsLock(rels =>
            {

                var possibleFollows = rels.GetFollowingFast(ctx);
                var isPostSeen = rels.GetIsPostSeenFuncForUserRequiresLock(ctx);

                var userPosts = possibleFollows.PossibleFollows.Select(pair =>
                {
                    return GetBalancedFollowingFeedCandidatesForFollowee(rels, pair.Plc, pair.IsPrivate, minDate, loggedInUser, possibleFollows, isPostSeen);
                }).Where(x => x.Plc != default).ToArray();
                return (possibleFollows, userPosts);
            }, ctx);


            var alreadySampledPost = new HashSet<PostId>();
            // last few posts from the previous page that aren't marked as read yet
            var alreadyReturnedPosts = continuation != null ? (continuation.Split(",").Skip(1).Select(x => StringUtils.DeserializeFromString<PostId>(x)!.Value)).ToHashSet() : new();


            var finalPosts = new List<BlueskyPost>();
            bool ProducedEnoughPosts() => finalPosts.Count >= limit;

            var bestOriginalPostsByUser = users
                .Select(
                    user =>
                    {
                        var userScore = scoredSampler(user.Plc);
                        return user.Posts
                            .Select(x => new ScoredBlueskyPost(new(user.Plc, x.PostRKey), Repost: default, IsAuthorFollowed: true, x.LikeCount, GetBalancedFeedPerUserScore(x.LikeCount, now - x.PostRKey.Date), GetBalancedFeedGlobalScore(x.LikeCount, now - x.PostRKey.Date, userScore)))
                            .OrderByDescending(x => x.PerUserScore)
                            .ToQueueWithOwner(user.Plc);
                    })
                .Where(x => x.Count != 0)
                .ToList();
            var bestRepostsByUser = users
                .Select(
                    user =>
                    {
                        var userScore = scoredSampler(user.Plc);
                        return user.Reposts
                                .Select(x => new ScoredBlueskyPost(x.PostId, Repost: new Models.Relationship(user.Plc, x.RepostRKey), x.IsReposteeFollowed, x.LikeCount, GetBalancedFeedPerUserScore(x.LikeCount, now - x.RepostRKey.Date), GetBalancedFeedGlobalScore(x.LikeCount, now - x.RepostRKey.Date, userScore)))
                                .OrderByDescending(x => x.PerUserScore)
                                .ToQueueWithOwner(user.Plc);
                    })
                .Where(x => x.Count != 0)
                .ToList();

            var allOriginalPostsAndReplies = bestOriginalPostsByUser.SelectMany(x => x).Select(x => x.PostId).ToHashSet();


            var mergedFollowedPosts = new Queue<ScoredBlueskyPostWithSource>();
            var mergedNonFollowedPosts = new Queue<ScoredBlueskyPostWithSource>();

            while ((bestOriginalPostsByUser.Count != 0 || bestRepostsByUser.Count != 0) && !ProducedEnoughPosts())
            {

                var followedPostsToEnqueue = new List<ScoredBlueskyPostWithSource>();
                var nonFollowedRepostsToEnqueue = new List<ScoredBlueskyPostWithSource>();

                void SampleEachUser(IEnumerable<QueueWithOwner<ScoredBlueskyPost>> users)
                {
                    foreach (var user in users)
                    {
                        while (user.TryDequeue(out var post))
                        {
                            if (alreadySampledPost.Add(post.PostId))
                            {
                                if (post.IsAuthorFollowed)
                                    followedPostsToEnqueue.Add(new(post, user));
                                else
                                    nonFollowedRepostsToEnqueue.Add(new(post, user));

                                break;
                            }
                        }
                    }
                }

                SampleEachUser(bestOriginalPostsByUser);
                SampleEachUser(bestRepostsByUser);


                bestOriginalPostsByUser.RemoveAll(x => x.Count == 0);
                bestRepostsByUser.RemoveAll(x => x.Count == 0);

                mergedFollowedPosts.EnqueueRange(followedPostsToEnqueue.OrderByDescending(x => x.Post.GlobalScore));
                mergedNonFollowedPosts.EnqueueRange(nonFollowedRepostsToEnqueue.OrderByDescending(x => x.Post.GlobalScore));

                var populateFollowedFrom = mergedFollowedPosts;
                var populateNonFollowedPostsFrom = mergedNonFollowedPosts;

                var enqueueEverything = false;

                var extraIterations = 0;
                while (true)
                {
                    extraIterations++;

                    var usersDeservingFollowedPostResampling = new List<QueueWithOwner<ScoredBlueskyPost>>();
                    var usersDeservingNonFollowedPostResampling = new List<QueueWithOwner<ScoredBlueskyPost>>();

                    WithRelationshipsLock(rels =>
                    {
                        var isPostSeen = rels.GetIsPostSeenFuncForUserRequiresLock(ctx);

                        bool IsPostSeenOrAlreadyReturned(PostId postId) => alreadyReturnedPosts.Contains(postId) || isPostSeen(postId);

                        bool ShouldInclude(BlueskyPost post)
                        {
                            var result = ShouldIncludeCore(post);
                            if (!result) BlueskyRelationships.DiscardPost(post.PostId, ctx);
                            return result;
                        }
                        bool ShouldIncludeCore(BlueskyPost post)
                        {
                            var shouldInclude = rels.ShouldIncludeLeafPostInFollowingFeed(post, ctx);
                            if (shouldInclude != null) return shouldInclude.Value;

                            if (post.RootPostId is { Author: var rootAuthor } && rootAuthor != post.AuthorId)
                            {
                                if (!possibleFollows.IsStillFollowed(rootAuthor, rels) && rootAuthor != loggedInUser)
                                    return false;
                            }
                            if (post.InReplyToPostId is { Author: var inReplyTo } && inReplyTo != post.AuthorId)
                            {
                                if (!possibleFollows.IsStillFollowed(inReplyTo, rels) && inReplyTo != loggedInUser)
                                    return false;
                            }
                            return true;
                        }

                        BlueskyPost? TryGetBestReply(BlueskyPost post)
                        {
                            if (post.ReplyCount == 0) return null;

                            var replies = new List<ScoredBlueskyPost>();
                            foreach (var chunk in rels.DirectReplies.GetValuesChunked(post.PostId))
                            {
                                foreach (var reply in chunk.AsSmallSpan())
                                {
                                    if (allOriginalPostsAndReplies.Contains(reply) && !alreadyReturnedPosts.Contains(reply) && !isPostSeen(reply))
                                    {
                                        var likeCount = rels.Likes.GetApproximateActorCount(reply);
                                        replies.Add(new ScoredBlueskyPost(reply, default, true, likeCount, GetBalancedFeedPerUserScore(likeCount, now - reply.PostRKey.Date), 0));
                                    }
                                }
                            }
                            if (replies.Count == 0) return null;
                            foreach (var reply in replies.OrderByDescending(x => (x.PostId.Author == post.AuthorId, x.PerUserScore)).Where(x => !isPostSeen(x.PostId)))
                            {
                                var p = rels.GetPost(reply.PostId, ctx);
                                if (ShouldInclude(p)) return p;
                            }

                            return null;
                        }

                        bool MaybeAddToFinalPostList(ScoredBlueskyPost postScore)
                        {
                            if (!alreadyReturnedPosts.Add(postScore.PostId)) return false;

                            var post = rels.GetPostAndMaybeRepostedBy(postScore.PostId, postScore.Repost, ctx);
                            if (!ShouldInclude(post)) return false;

                            var threadLength = 0;

                            void AddCore(BlueskyPost post)
                            {
                                if (threadLength != 0)
                                {
                                    post.RepostedBy = null;
                                    post.RepostDate = null;
                                }
                                alreadyReturnedPosts.Add(post.PostId);
                                alreadySampledPost.Add(post.PostId);
                                finalPosts.Add(post);
                                threadLength++;
                            }

                            var shouldIncludeFullReplyChain = post.PluggableProtocol?.ShouldIncludeFullReplyChain(post) == true;
                            if (post.InReplyToPostId is { } inReplyToPostId && (!post.IsRepost || shouldIncludeFullReplyChain || allOriginalPostsAndReplies.Contains(inReplyToPostId)))
                            {
                                if (
                                    post.RootPostId != inReplyToPostId &&
                                    IsPostSeenOrAlreadyReturned(inReplyToPostId) &&
                                    IsPostSeenOrAlreadyReturned(post.RootPostId) &&
                                    (post.Date - inReplyToPostId.PostRKey.Date).TotalHours < 36
                                    )
                                {
                                    return false;
                                }

                                if (shouldIncludeFullReplyChain)
                                {
                                    foreach (var (index, item) in rels.MakeFullReplyChainExcludingLeaf(post, ctx).Index())
                                    {
                                        if (index == 0 && post != item && rels.ShouldIncludeLeafOrRootPostInFollowingFeed(item, ctx) == false)
                                            return false;
                                        AddCore(item);
                                    }
                                }
                                else
                                {

                                    var parent = rels.GetPost(inReplyToPostId, ctx);

                                    BlueskyPost rootPost;

                                    if (post.RootPostId != inReplyToPostId)
                                    {
                                        if (rels.ShouldIncludeLeafOrRootPostInFollowingFeed(parent, ctx) == false)
                                            return false;
                                        rootPost = rels.GetPost(post.RootPostId, ctx);
                                    }
                                    else
                                    {
                                        rootPost = parent;
                                    }

                                    if (rels.ShouldIncludeLeafOrRootPostInFollowingFeed(rootPost, ctx) == false)
                                        return false;

                                    if (rootPost.HasExternalThumbnailBestGuess && IsPostSeenOrAlreadyReturned(rootPost.PostId))
                                    {
                                        rootPost.ShouldUseCompactView = true;
                                    }
                                    AddCore(rootPost);

                                    if (rootPost != parent)
                                        AddCore(parent);


                                }
                            }

                            AddCore(post);

                            if (threadLength <= 2)
                            {
                                var bestReply = TryGetBestReply(post);
                                if (bestReply != null)
                                {
                                    AddCore(bestReply);

                                    if (threadLength <= 2)
                                    {
                                        var bestGrandReply = TryGetBestReply(bestReply);
                                        if (bestGrandReply != null)
                                        {
                                            AddCore(bestGrandReply);
                                        }
                                    }
                                }
                            }

                            return true;
                        }

                        var done = false;
                        while (
                            !ProducedEnoughPosts() &&
                            enqueueEverything
                                ? populateFollowedFrom.Count != 0 || populateNonFollowedPostsFrom.Count != 0
                                : populateFollowedFrom.Count != 0 && (!done && finalPosts.Count < 100)
                            )
                        {
                            for (int i = 0; i < 3; i++)
                            {
                                if (populateFollowedFrom.TryDequeue(out var followed))
                                {
                                    if (!MaybeAddToFinalPostList(followed.Post))
                                        usersDeservingFollowedPostResampling.Add(followed.Source);
                                    if (ProducedEnoughPosts()) { done = true; break; }
                                }
                                else
                                {
                                    done = true;
                                    break;
                                }

                            }
                            if (!done || enqueueEverything)
                            {
                                if (populateNonFollowedPostsFrom.TryDequeue(out var nonFollowed))
                                {
                                    if (!MaybeAddToFinalPostList(nonFollowed.Post))
                                        usersDeservingNonFollowedPostResampling.Add(nonFollowed.Source);
                                    if (ProducedEnoughPosts()) { done = true; break; }
                                }
                            }
                        }
                    }, ctx);

                    if (ProducedEnoughPosts()) break;
                    if (usersDeservingFollowedPostResampling.Count == 0 && usersDeservingNonFollowedPostResampling.Count == 0) break;
                    if (extraIterations >= 2)
                    {
                        lock (ctx.UserContext)
                        {
                            foreach (var queue in usersDeservingFollowedPostResampling)
                            {
                                ConsumeFollowingFeedCreditsMustHoldLock(ctx, queue.Owner, addCredits: true);
                            }
                            foreach (var queue in usersDeservingNonFollowedPostResampling)
                            {
                                ConsumeFollowingFeedCreditsMustHoldLock(ctx, queue.Owner, addCredits: true);
                            }
                        }
                        break;
                    }


                    followedPostsToEnqueue.Clear();
                    nonFollowedRepostsToEnqueue.Clear();

                    SampleEachUser(usersDeservingFollowedPostResampling);
                    SampleEachUser(usersDeservingNonFollowedPostResampling);

                    populateFollowedFrom = followedPostsToEnqueue.OrderByDescending(x => x.Post.GlobalScore).ToQueue();
                    populateNonFollowedPostsFrom = nonFollowedRepostsToEnqueue.OrderByDescending(x => x.Post.GlobalScore).ToQueue();

                    enqueueEverything = true;
                }

            }

            var posts = finalPosts.ToArray();
            await EnrichAsync(posts, ctx);
            return new PostsAndContinuation(posts, ProducedEnoughPosts() ? string.Join(",", finalPosts.TakeLast(10).Select(x => StringUtils.SerializeToString(x.PostId)).Prepend(StringUtils.SerializeToString(Guid.NewGuid()))) : null);
        }

        private static BalancedFeedCandidatesForFollowee GetBalancedFollowingFeedCandidatesForFollowee(BlueskyRelationships rels, Plc plc, bool couldBePluggablePost, DateTime minDate, Plc loggedInUser, FollowingFastResults possibleFollows, Func<PostIdTimeFirst, bool> isPostSeen)
        {
            var postsFast = rels.GetRecentPopularPosts(plc, couldBePluggablePost: couldBePluggablePost /*pluggables can only repost pluggables, atprotos can only repost atprotos*/);

            var posts = postsFast
                .Where(x => !isPostSeen(new PostIdTimeFirst(x.RKey, plc)))
                .Where(x => x.RKey.Date >= minDate)
                .ToArray();

            var reposts = rels.GetRecentReposts(plc, couldBePluggablePost: couldBePluggablePost)
                .Where(x => !isPostSeen(x.PostId) && x.PostId.Author != loggedInUser)
                .Where(x => x.RepostRKey.Date >= minDate)
                .ToArray();

            if (posts.Length == 0 && reposts.Length == 0) return default;
            if (!possibleFollows.IsStillFollowed(plc, rels)) return default;
            var isReposterOnly = reposts.Length != 0 && posts.Length == 0 && rels.ReposterOnlyProfile.ContainsKey(plc);

            return new BalancedFeedCandidatesForFollowee(
                plc,
                posts
                    .Where(x => x.InReplyTo == default || x.InReplyTo == loggedInUser || possibleFollows.IsStillFollowed(x.InReplyTo, rels))
                    //.Select(x => (PostRKey: x.RKey, LikeCount: rels.GetApproximateLikeCount(new(x.RKey, plc), pair.IsPrivate, plcToRecentPostLikes)))
                    .Select(x => (PostRKey: x.RKey, LikeCount: x.ApproximateLikeCount))
                    .ToArray(),
               reposts
                    .Select(x => (x.PostId, x.RepostRKey, IsReposteeFollowed: possibleFollows.IsStillFollowed(x.PostId.Author, rels) || isReposterOnly, LikeCount: rels.GetApproximateLikeCount(x.PostId, couldBePluggablePost /*pluggables can only repost pluggables, atprotos can only repost atprotos*/, allowImprecise: true)))
                    .ToArray()
               );
        }

        private Func<Plc, float> GetScorer(RequestContext ctx)
        {
            var userCtx = ctx.UserContext;

            Dictionary<Plc, float> feedCreditsSoftmaxed;
            lock (ctx.UserContext)
            {
                feedCreditsSoftmaxed = GetFeedCreditsSoftMaxMustHoldLock(ctx).ToDictionary(x => x.Plc, x => x.SoftmaxedCredits);
            }
            var userEngagementScores = GetUserEngagementScores(ctx);
            return (plc) =>
            {
                if (!feedCreditsSoftmaxed.TryGetValue(plc, out var softmaxedCredits))
                    softmaxedCredits = feedCreditsSoftmaxed[default];

                if (!userEngagementScores.TryGetValue(plc, out var engagementScore))
                    engagementScore = ctx.UserContext.DefaultEngagementScore;

                return softmaxedCredits * engagementScore;
            };
        }

        private IEnumerable<(Plc Plc, float SoftmaxedCredits)> GetFeedCreditsSoftMaxMustHoldLock(RequestContext ctx)
        {
            UpdateFollowingFeedMustHoldLock(ctx);

            // Plc(default) is used for authors with 0 credits.
            var keys = ctx.UserContext.FeedCredits!.Keys.Prepend(default);
            var values = ctx.UserContext.FeedCredits!.Values.Prepend(0);

            double max = values.Max();
            double[] expValues = values.Select(v => Math.Exp(v - max)).ToArray();
            double sumExp = expValues.Sum();

            return keys.Select((x, i) => (x, (float)(expValues[i] / sumExp)));

        }

        private Dictionary<Plc, float> GetUserEngagementScores(RequestContext ctx)
        {
            var userCtx = ctx.UserContext;
            var globalUserEngagementCache = DangerousUnlockedRelationships.UserPairEngagementCache;
            if (userCtx.UserEngagementCacheVersion < globalUserEngagementCache.Version)
            {
                lock (userCtx)
                {
                    if (userCtx.UserEngagementCacheVersion < globalUserEngagementCache.Version)
                    {
                        var subCtx = RequestContext.CreateForRequest(ctx.Session, urgent: ctx.IsUrgent);
                        subCtx.AllowStale = false; // cache only exists in primary
                        WithRelationshipsLock(rels =>
                        {
                            var newdict = new Dictionary<Plc, float>();
                            var totalFeedEngagement = 0;
                            var totalFeedSeenPosts = 0;
                            var scores = rels.GetUserEngagementScoresForUser(userCtx.LoggedInUser!.Value).ToArray();
                            foreach (var user in scores)
                            {
                                totalFeedEngagement += user.FollowingEngagedPosts;
                                totalFeedSeenPosts += user.FollowingSeenPosts;
                            }
                            userCtx.AverageEngagementRatio = BlueskyRelationships.WeightedMeanWithPrior(totalFeedEngagement, totalFeedSeenPosts, 0.03, 50);

                            foreach (var item in scores)
                            {
                                newdict.Add(item.Target, BlueskyRelationships.GetUserEngagementScore(item, userCtx.AverageEngagementRatio));
                            }

                            userCtx.UserEngagementCache = newdict;
                            userCtx.DefaultEngagementScore = BlueskyRelationships.GetUserEngagementScore(default, userCtx.AverageEngagementRatio);
                            userCtx.UserEngagementCacheVersion = globalUserEngagementCache.Version;
                        }, subCtx);
                    }
                }
            }
            return userCtx.UserEngagementCache;
        }

        public UserEngagementStats[] GetUserEngagementRawScores(RequestContext ctx, bool followeesOnly)
        {
            ctx.AllowStale = false;
            GetUserEngagementScores(ctx); // Populates AverageEngagementRatio
            return WithRelationshipsWriteLock(rels =>
            {
                var result = rels.GetUserEngagementScoresForUser(ctx.LoggedInUser);
                if (followeesOnly)
                {
                    var followees = rels.GetFollowingFast(ctx);
                    result = result.Where(x => followees.IsStillFollowed(x.Target, rels));
                }
                return result.ToArray();
            }, ctx);
        }
        public async Task<ProfilesAndContinuation> GetUserEngagementScoresAsync(RequestContext ctx, string? continuation, int limit = default, string? onlyDid = null, bool followeesOnly = false)
        {
            EnsureLimit(ref limit, 100);
            var onlyPlc = onlyDid != null ? SerializeSingleDid(onlyDid, ctx) : default;
            var offset = continuation != null ? int.Parse(continuation) : 0;

            var pageScores = GetUserEngagementRawScores(ctx, followeesOnly)
                .Where(x => onlyPlc == default || onlyPlc == x.Target)
                .Select(x => (Raw: x, Score: BlueskyRelationships.GetUserEngagementScore(x, ctx.UserContext.AverageEngagementRatio)))
                .OrderByDescending(x => x.Score)
                .Skip(offset)
                .Take(limit + 1)
                .ToArray();
            var dict = pageScores.ToDictionary(x => x.Raw.Target, x => x);

            void SetBadge(BlueskyProfile profile)
            {
                var s = dict[profile.Plc];
                profile.Labels = [new AdditionalDataBadge(s.Score.ToString("0.00") + ": " + s.Raw.FollowingEngagedPosts + " (+" + s.Raw.EngagedPosts + ")" + " / " + s.Raw.FollowingSeenPosts), .. profile.Labels];
            }
            var page = WithRelationshipsLock(rels => pageScores.Select(x => rels.GetProfile(x.Raw.Target, ctx)).ToArray(), ctx);
            await EnrichAsync(page, ctx, SetBadge);
            foreach (var item in page)
            {
                SetBadge(item);
            }
            return GetPageAndNextPaginationFromLimitPlus1(page, limit, _ => (offset + limit).ToString());
        }



        //private static Dictionary<PostId, DateTime> GetMostRecentRepostDates(ScoredBlueskyPost[] candidates)
        //{
        //    var postToMostRecentRepostDate = new Dictionary<PostId, DateTime>();
        //    foreach (var candidate in candidates)
        //    {
        //        ref var date = ref CollectionsMarshal.GetValueRefOrAddDefault(postToMostRecentRepostDate, candidate.PostId, out var exists);
        //        if (!exists)
        //            date = candidate.Post.Date;
        //        if (candidate.Post.RepostDate is { } repostDate && repostDate > date)
        //            date = repostDate;
        //    }

        //    return postToMostRecentRepostDate;
        //}

        record struct ScoredBlueskyPost(PostId PostId, Models.Relationship Repost, bool IsAuthorFollowed, long LikeCount, float PerUserScore, float GlobalScore)
        {
            public override string ToString()
            {
                return $"{PerUserScore:0.000} | {GlobalScore:0.000} | +{LikeCount} | {PostId}";
            }
        }

        private static float GetBalancedFeedPerUserScore(long likeCount, TimeSpan age) => GetDecayedScore(likeCount, age, 0.3);
        private static float GetBalancedFeedGlobalScore(long likeCount, TimeSpan age, float userScore) => GetDecayedScore(Math.Pow(likeCount, 0.1), age, 1.8) * userScore;

        private static float GetDecayedScore(double likeCount, TimeSpan age, double gravity, double baseHours = 2)
        {
            // https://medium.com/hacking-and-gonzo/how-hacker-news-ranking-algorithm-works-1d9b0cf2c08d
            // HackerNews uses gravity=1.8
            if (age < TimeSpan.Zero) age = TimeSpan.Zero;
            var ageHours = age.TotalHours;
            var score = (likeCount + 1) / Math.Pow(ageHours + baseHours, gravity);
            return (float)score;
        }

        public ConcurrentSet<RepositoryImportEntry> RunningCarImports = new();

        public async Task<RepositoryImportEntry?> ImportCarIncrementalAsync(Plc plc, RepositoryImportKind kind, RequestContext ctx, Func<RepositoryImportEntry, bool>? ignoreIfPrevious = null, bool incremental = true, CancellationToken ct = default, bool slowImport = false)
        {
            if (IsQuickBackfillCollection(kind))
            {
                if (AppViewLiteConfiguration.GetString(AppViewLiteParameter.APPVIEWLITE_QUICK_REVERSE_BACKFILL_INSTANCE) == "-")
                    throw new Exception("Quick reverse backfill is not enabled.");
            }
            async Task<RepositoryImportEntry?> CoreAsync()
            {
                RepositoryImportEntry? previousImport = null;
                bool isRegisteredUser = false;
                string? did = null;
                var isNativeAtProto = true;
                WithRelationshipsLock(rels =>
                {
                    did = rels.GetDid(plc);
                    if (!BlueskyRelationships.IsNativeAtProtoDid(did))
                    {
                        isNativeAtProto = false;
                        return;
                    }
                    isRegisteredUser = rels.IsRegisteredForNotifications(plc);
                    previousImport = rels.GetRepositoryImports(plc).Where(x => x.Kind == kind).MaxBy(x => (x.LastRevOrTid, x.StartDate));
                }, ctx);
                if (!isNativeAtProto) return null;
                if (previousImport != null && ignoreIfPrevious != null && ignoreIfPrevious(previousImport))
                    return previousImport;

                RequestContext? authenticatedCtx = ctx.IsLoggedIn && ctx.UserContext.Plc == plc ? ctx : null;
                var result = await CarImportDict.GetValueAsync((plc, kind, incremental), (previousImport, did!, isRegisteredUser, slowImport, AuthenticatedCtx: authenticatedCtx, Ctx: RequestContext.CreateForTaskDictionary(ctx)));
                ctx.BumpMinimumVersion(result.MinVersion);
                return result;
            }

            if (!ctx.IsUrgent)
            {
                return await Indexer.RunOnFirehoseProcessingThreadpool(CoreAsync);
            }
            return await CoreAsync();

        }

        private async Task<RepositoryImportEntry> ImportCarIncrementalCoreAsync(string did, RepositoryImportKind kind, Plc plc, Tid since, CancellationToken ct, RequestContext ctx, RequestContext? authenticatedCtx, bool slowImport)
        {
            var startDate = DateTime.UtcNow;
            var summary = new RepositoryImportEntry
            {
                Kind = kind,
                Plc = plc,
                StartDate = startDate,
                StartRevOrTid = since.TidValue,
                StillRunning = true,
            };
            RunningCarImports.TryAdd(summary);
            try
            {
                var sw = Stopwatch.StartNew();
                var indexer = new Indexer(this);
                Tid lastTid;
                Exception? exception = null;

                var progress = new Action<CarImportProgress>(x =>
                {
                    summary.DownloadedBytes = x.DownloadedBytes;
                    summary.InsertedRecordCount = x.InsertedRecords;
                    summary.EstimatedTotalBytes = x.EstimatedTotalBytes;
                    summary.TotalRecords = x.TotalRecords;
                    if (kind != RepositoryImportKind.CAR)
                        summary.LastRevOrTid = x.LastRecordRkey.TidValue;
                });
                if (kind == RepositoryImportKind.CAR)
                {
                    try
                    {
                        lastTid = await indexer.ImportCarAsync(did, since, ctx, progress, ct).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        exception = ex;
                        lastTid = default;
                    }
                }
                else if (kind == RepositoryImportKind.FollowersBskyAppBackfill)
                {
                    try
                    {
                        lastTid = await indexer.ImportFollowersBackfillAsync(did, authenticatedCtx, since, ctx, progress, ct).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        lastTid = default;
                        exception = ex;
                    }
                }
                else
                {
                    var recordType = kind switch
                    {
                        RepositoryImportKind.Posts => Post.RecordType,
                        RepositoryImportKind.Likes => Like.RecordType,
                        RepositoryImportKind.Reposts => Repost.RecordType,
                        RepositoryImportKind.Follows => Follow.RecordType,
                        RepositoryImportKind.Blocks => Block.RecordType,
                        RepositoryImportKind.ListMetadata => List.RecordType,
                        RepositoryImportKind.ListEntries => Listitem.RecordType,
                        RepositoryImportKind.BlocklistSubscriptions => Listblock.RecordType,
                        RepositoryImportKind.Threadgates => Threadgate.RecordType,
                        RepositoryImportKind.Postgates => Postgate.RecordType,
                        RepositoryImportKind.FeedGenerators => Generator.RecordType,
                        _ => throw new Exception("Unknown collection kind.")
                    };
                    (lastTid, exception) = await indexer.IndexUserCollectionAsync(did, recordType, since, ctx, ct, progress, slowImport).ConfigureAwait(false);
                }
                summary.DurationMillis = (long)sw.Elapsed.TotalMilliseconds;
                summary.LastRevOrTid = lastTid.TidValue;
                summary.Error = exception != null ? GetErrorDetails(exception) : null;
                summary.StillRunning = false;

                WithRelationshipsWriteLock(rels =>
                {
                    rels.CarImports.AddRange(new RepositoryImportKey(plc, startDate), BlueskyRelationships.SerializeProto(summary));
                }, ctx);

                RunningCarImports.Remove(summary);

                summary.MinVersion = ctx.MinVersion;
                return summary;
            }
            catch (Exception ex)
            {
                summary.MinVersion = ctx.MinVersion;
                summary.StillRunning = false;
                summary.Error = ex.Message;
                Task.Delay(TimeSpan.FromMinutes(1)).ContinueWith(_ => RunningCarImports.Remove(summary)).FireAndForget();
                throw;
            }
        }

        private static string GetErrorDetails(Exception exception)
        {
            while (true)
            {
                if (exception is ATNetworkErrorException at)
                {
                    return at.AtError.Detail?.Error ?? at.AtError.StatusCode.ToString();
                }
                if (exception.InnerException != null)
                {
                    exception = exception.InnerException;
                }
                else
                {
                    return exception.Message;
                }
            }
        }



        public async Task<Tid> CreateRecordAsync(ATObject record, RequestContext ctx, string? rkey = null)
        {
            var tid = await PerformPdsActionAsync(async session => Tid.Parse((await session.CreateRecordAsync(new ATDid(session.Session!.Did.Handler), record.Type, record, rkey: rkey)).HandleResult()!.Uri.Rkey), ctx);

            var indexer = new Indexer(this);
            indexer.OnRecordCreated(ctx.UserContext.Did!, record.Type + "/" + tid.ToString(), record, ctx: ctx);
            return tid;
        }

        public async Task<T> PerformPdsActionAsync<T>(Func<ATProtocol, Task<T>> func, RequestContext ctx)
        {
            var session = ctx.Session;
            using var sessionProtocol = await GetSessionProtocolAsync(ctx);
            if (sessionProtocol.AuthSession!.Session.ExpiresIn.AddMinutes(-5) > DateTime.UtcNow)
            {
                try
                {
                    return await func(sessionProtocol);
                }
                catch (Exception ex) when (ex.AnyInnerException(x => x is ATNetworkErrorException at && at.AtError.Detail?.Error == "ExpiredToken"))
                {
                    // continue
                }
            }

            AuthSession authSession;
            try
            {
                authSession = (await sessionProtocol.RefreshAuthSessionResultAsync()).HandleResult()!;
                if (authSession == null) throw new Exception("Null authSession");
            }
            catch (Exception ex)
            {
                LogOut(ctx.Session.SessionToken, ctx.Session.Did!, ctx);
                throw new LoggedOutException("You have been logged out.", ex);
            }

            var userCtx = ctx.UserContext;
            userCtx.PrivateProfile!.PdsSessionCbor = SerializeAuthSession(authSession!);
            userCtx.PdsSession = authSession!.Session;
            userCtx.UpdateRefreshTokenExpireDate();

            SaveAppViewLiteProfile(ctx);

            using var sessionProtocol2 = await GetSessionProtocolAsync(ctx);
            return await func(sessionProtocol2);
        }

        public void SaveAppViewLiteProfile(RequestContext ctx)
        {
            WithRelationshipsWriteLock(rels =>
            {
                rels.SaveAppViewLiteProfile(ctx.UserContext);
            }, ctx);
        }

        public static byte[] SerializeAuthSession(AuthSession authSession)
        {
            return CBORObject.FromJSONString(authSession.ToString()).EncodeToBytes();
        }
        public static AuthSession DeserializeAuthSession(byte[] bytes)
        {
            return AuthSession.FromString(CBORObject.DecodeFromBytes(bytes).ToJSONString())!;
        }

        public async Task<ATProtocol> GetSessionProtocolAsync(RequestContext ctx)
        {
            if (!ctx.IsLoggedIn) throw AssertionLiteException.Throw("Cannot create own PDS client when not logged in.");
            if (ctx.Session.IsReadOnlySimulation) throw new InvalidOperationException("Read only simulation.");
            var pdsSession = ctx.UserContext.PdsSession!;
            var sessionProtocol = await CreateProtocolForDidAsync(pdsSession.Did.Handler, ctx);
            (await sessionProtocol.AuthenticateWithPasswordSessionResultAsync(new AuthSession(pdsSession))).HandleResult();
            return sessionProtocol;

        }

        public async Task<Session> LoginToPdsAsync(string did, string password, RequestContext ctx)
        {
            var sessionProtocol = await CreateProtocolForDidAsync(did, ctx);
            var session = (await sessionProtocol.AuthenticateWithPasswordResultAsync(did, password)).HandleResult()!;
            return session;
        }

        public async Task<ATProtocol?> TryCreateProtocolForDidAsync(string did, RequestContext ctx)
        {
            if (!BlueskyRelationships.IsNativeAtProtoDid(did)) return null;
            return await CreateProtocolForDidAsync(did, ctx)!;
        }


        [System.Diagnostics.CodeAnalysis.SuppressMessage("Performance", "CA1822:Mark members as static", Justification = "Consistency with other factory methods")]
        public ATProtocol CreateQuickBackfillProtocol()
        {
            var instance = AppViewLiteConfiguration.GetString(AppViewLiteParameter.APPVIEWLITE_QUICK_REVERSE_BACKFILL_INSTANCE) ?? "https://public.api.bsky.app";
            if (instance == "-") throw new Exception("Quick reverse backfill is not enabled.");
            return CreateUnauthenticatedProtocol(new Uri(instance));
        }

        public static ATProtocol CreateUnauthenticatedProtocol(Uri instanceUrl)
        {
            var builder = new ATProtocolBuilder()
                .WithInstanceUrl(instanceUrl)
                .WithLogger(CreateClientAtProtocolLogger());
            return builder.Build();
        }

        public async Task<ATProtocol> CreateProtocolForDidAsync(string did, RequestContext ctx)
        {
            var diddoc = await GetDidDocAsync(did, ctx);
            AdministrativeBlocklist.ThrowIfBlockedOutboundConnection(did, diddoc);

            var pds = diddoc.Pds;
            if (pds == null) throw new UnexpectedFirehoseDataException("No PDS is specified in the DID doc of this user.");
            var builder = new ATProtocolBuilder()
                .WithInstanceUrl(new Uri(pds))
                .WithLogger(CreateClientAtProtocolLogger());
            var dict = new Dictionary<ATDid, Uri>
            {
                { new ATDid(did), new Uri(pds) }
            };
            builder.WithATDidCache(dict);
            return builder.Build();
        }

        private static LogWrapper CreateClientAtProtocolLogger()
        {
            return new LogWrapper(Microsoft.Extensions.Logging.LogLevel.Warning, Microsoft.Extensions.Logging.LogLevel.Warning, Microsoft.Extensions.Logging.LogLevel.Information)
            {
                IsLowImportanceException = x => x.AnyInnerException(x => x is HttpRequestException),
                IsLowImportanceMessage = x => x.StartsWith("ATError:", StringComparison.Ordinal)
            };
        }

        public async Task<Tid> CreateFollowAsync(string did, RequestContext ctx)
        {
            return await CreateRecordAsync(new Follow { Subject = new ATDid(did), CreatedAt = UtcNowMillis() }, ctx);
        }
        public async Task<Tid> CreateBlockAsync(string did, RequestContext ctx)
        {
            return await CreateRecordAsync(new Block { Subject = new ATDid(did), CreatedAt = UtcNowMillis() }, ctx);
        }

        private static DateTime UtcNowMillis()
        {
            return new DateTime(DateTime.UtcNow.Ticks / TimeSpan.TicksPerMillisecond * TimeSpan.TicksPerMillisecond, DateTimeKind.Utc);
        }

        public async Task DeleteFollowAsync(Tid rkey, RequestContext ctx)
        {
            await DeleteRecordAsync(Follow.RecordType, rkey, ctx);
        }
        public async Task DeleteBlockAsync(Tid rkey, RequestContext ctx)
        {
            await DeleteRecordAsync(Block.RecordType, rkey, ctx);
        }
        public async Task<Tid> CreatePostLikeAsync(string did, Tid rkey, RequestContext ctx)
        {
            var cid = await GetCidAsync(did, Post.RecordType, rkey, ctx);
            return await CreateRecordAsync(new Like { Subject = new StrongRef(new ATUri("at://" + did + "/" + Post.RecordType + "/" + rkey), cid) { Type = null! }, CreatedAt = UtcNowMillis() }, ctx);
        }
        public async Task<Tid> CreateRepostAsync(string did, Tid rkey, RequestContext ctx)
        {
            var cid = await GetCidAsync(did, Post.RecordType, rkey, ctx);
            return await CreateRecordAsync(new Repost { Subject = new StrongRef(new ATUri("at://" + did + "/" + Post.RecordType + "/" + rkey), cid) { Type = null! }, CreatedAt = UtcNowMillis() }, ctx);
        }

        public Tid CreatePostBookmark(string did, Tid rkey, RequestContext ctx)
        {
            return WithRelationshipsWriteLock(rels =>
            {
                var bookmarkRkey = Tid.FromDateTime(DateTime.UtcNow);
                var plc = rels.SerializeDid(did, ctx);
                var loggedInUser = ctx.LoggedInUser;
                var postId = new PostId(rels.SerializeDid(did, ctx), rkey);
                rels.Bookmarks.Add(loggedInUser, new BookmarkPostFirst(postId, bookmarkRkey));
                rels.RecentBookmarks.Add(loggedInUser, new BookmarkDateFirst(bookmarkRkey, postId));
                rels.NotifyPostStatsChange(postId, ctx.LoggedInUser);
                return bookmarkRkey;
            }, ctx);
        }
        public void DeletePostBookmark(string postDid, Tid postRkey, Tid bookmarkRKey, RequestContext ctx)
        {
            WithRelationshipsWriteLock(rels =>
            {
                rels.BookmarkDeletions.Add(ctx.LoggedInUser, bookmarkRKey);
                rels.NotifyPostStatsChange(new PostId(rels.SerializeDid(postDid, ctx), postRkey), ctx.LoggedInUser);
            }, ctx);
        }
        public async Task DeletePostLikeAsync(Tid likeRKey, RequestContext ctx)
        {
            await DeleteRecordAsync(Like.RecordType, likeRKey, ctx);
        }
        public async Task DeletePostAsync(Tid postRkey, RequestContext ctx)
        {
            await DeleteRecordAsync(Post.RecordType, postRkey, ctx);
        }
        public async Task DeleteRepostAsync(Tid repostRKey, RequestContext ctx)
        {
            await DeleteRecordAsync(Repost.RecordType, repostRKey, ctx);
        }
        public async Task DeleteRecordAsync(string collection, Tid rkey, RequestContext ctx)
        {
            await PerformPdsActionAsync(session => session.DeleteRecordAsync(session.Session!.Did, collection, rkey.ToString()!), ctx);
            var indexer = new Indexer(this);
            indexer.OnRecordDeleted(ctx.UserContext.Did!, collection + "/" + rkey, ctx: ctx);
        }

        public async Task<Tid> CreatePostAsync(string? text, PostIdString? inReplyTo, PostIdString? quotedPost, IReadOnlyList<BlobToUpload> attachments, RequestContext ctx, CancellationToken ct = default)
        {
            if (string.IsNullOrEmpty(text)) text = null;
            var embedRecord = quotedPost != null ? new EmbedRecord((await GetPostStrongRefAsync(quotedPost, ctx)).StrongRef) : null;
            ReplyRefDef? replyRefDef = null;
            if (inReplyTo != null)
            {
                var inReplyToRef = await GetPostStrongRefAsync(inReplyTo, ctx);
                replyRefDef = new ReplyRefDef
                {
                    Parent = inReplyToRef.StrongRef,
                    Root = inReplyToRef.Record.Reply?.Root ?? inReplyToRef.StrongRef,
                };
            }

            var processedAttachments = new List<Image>();
            foreach (var attachment in attachments)
            {
                var processedImage = await ImageUploadProcessor.ProcessAsync(attachment.UploadedBytes, ct);
                var processedContentType = "image/webp";

                var response = await PerformPdsActionAsync(async protocol =>
                {
                    var streamContent = new StreamContent(processedImage.ProcessedBytes);
                    streamContent.Headers.ContentType = new(processedContentType);
                    var response = (await protocol.UploadBlobAsync(streamContent, ct)).HandleResult()!;
                    return response;
                }, ctx);
                processedAttachments.Add(new Image
                {
                    Alt = string.IsNullOrWhiteSpace(attachment.AltText) ? string.Empty : attachment.AltText.Trim(),
                    AspectRatio = new AspectRatio
                    {
                        Height = processedImage.Height,
                        Width = processedImage.Width,
                    },
                    ImageValue = new Blob
                    {
                        MimeType = processedContentType,
                        Size = (int)processedImage.ProcessedBytes.Length,
                        Ref = response.Blob.Ref,
                    },
                });
            }


            var embedImages = processedAttachments.Count != 0 ? new EmbedImages(processedAttachments) : null;

            ATObject? embed = null;
            if (embedImages != null && embedRecord != null)
            {
                embed = new RecordWithMedia(embedRecord, embedImages);
            }
            else
            {
                embed = (ATObject?)embedRecord ?? embedImages;
            }

            var postRecord = new Post
            {
                Text = text,
                Reply = replyRefDef,
                Embed = embed,
                CreatedAt = UtcNowMillis()
            };
            return await CreateRecordAsync(postRecord, ctx);
        }

        private async Task<(StrongRef StrongRef, Post Record)> GetPostStrongRefAsync(PostIdString post, RequestContext ctx)
        {
            var info = await GetRecordAsync(post.Did, Post.RecordType, post.RKey, ctx);
            return (new StrongRef(info.Uri, info.Cid!), (Post)info.Value);

        }

        internal Task<string> GetCidAsync(string did, string collection, Tid rkey, RequestContext ctx)
        {
            return GetCidAsync(did, collection, rkey.ToString()!, ctx);
        }
        internal async Task<string> GetCidAsync(string did, string collection, string rkey, RequestContext ctx)
        {
            return (await GetRecordAsync(did, collection, rkey, ctx)).Cid!;
        }

        public async Task<BlueskyPost> GetPostAsync(string did, string rkey, RequestContext ctx)
        {
            var post = GetSinglePost(did, rkey, ctx);
            await EnrichAsync([post], ctx);
            return post;
        }




        public async Task<(BlueskyList[] Lists, string? NextContinuation)> GetBlocklistSubscriptionsAsync(string did, string? continuation, int limit, RequestContext ctx)
        {
            EnsureLimit(ref limit, 50);
            var response = await ListRecordsAsync(did, Listblock.RecordType, limit: limit + 1, cursor: continuation, ctx);
            var blocklistSubscriptions = WithRelationshipsUpgradableLock(rels =>
            {
                return response!.Records!.TrySelect(x =>
                {
                    var listblock = (Listblock)x.Value;
                    return rels.GetList(new Models.Relationship(rels.SerializeDid(listblock.Subject!.Did!.Handler, ctx), Tid.Parse(listblock.Subject.Rkey)), ctx: ctx);
                }).ToArray();
            }, ctx);
            ctx.IncreaseTimeout(TimeSpan.FromSeconds(3));
            await EnrichAsync(blocklistSubscriptions, ctx);
            return (blocklistSubscriptions, response.Records.Count > limit ? response!.Cursor : null);
        }
        public async Task<(BlueskyList[] Lists, string? NextContinuation)> GetMemberOfListsAsync(string did, string? continuation, int limit, RequestContext ctx)
        {
            EnsureLimit(ref limit, 10);

            ListMembership? parsedContinuation = continuation != null ? ListMembership.Deserialize(continuation) : null;

            var lists = WithRelationshipsLockForDid(did, (plc, rels) =>
            {
                return rels.ListMemberships.GetValuesSorted(plc, parsedContinuation)
                    .Where(x => !rels.ListItemDeletions.ContainsKey(new(x.ListAuthor, x.ListItemRKey)))
                    .Select(x =>
                    {
                        var list = rels.GetList(new(x.ListAuthor, x.ListRKey), ctx: ctx);
                        list.MembershipRkey = x.ListItemRKey;
                        return list;
                    })
                    .Where(x => x.Data?.Deleted != true)
                    .Take(limit + 1)
                    .ToArray();
            }, ctx);

            ctx.IncreaseTimeout(TimeSpan.FromSeconds(3)); // no live reload for lists
            await EnrichAsync(lists, ctx);
            return GetPageAndNextPaginationFromLimitPlus1(lists, limit, x => new ListMembership(x.Moderator!.Plc, x.ListId.RelationshipRKey, x.MembershipRkey!.Value).Serialize());
        }

        public async Task<BlueskyFeedGenerator[]> EnrichAsync(BlueskyFeedGenerator[] feeds, RequestContext ctx, CancellationToken ct = default)
        {
            if (feeds.Length == 0) return feeds;

            await EnrichAsync(feeds.Select(x => x.Author).ToArray(), ctx, ct: ct);
            return feeds;
        }

        public async Task<IReadOnlyList<BlueskyModerationBase>> EnrichAsync(IReadOnlyList<BlueskyModerationBase> labels, RequestContext ctx)
        {
            var task1 = EnrichAsync(labels.OfType<BlueskyLabel>().ToArray(), ctx);
            var task2 = EnrichAsync(labels.OfType<BlueskyList>().ToArray(), ctx);
            await task1;
            await task2;
            return labels;
        }

        public async Task<BlueskyLabel[]> EnrichAsync(BlueskyLabel[] labels, RequestContext ctx)
        {
            if (labels.Length == 0) return labels;

            await EnrichAsync(labels.Select(x => x.Moderator!).ToArray(), ctx, omitLabelsAndViewerFlags: true /* avoid infinite recursion */);
            if (!IsReadOnly)
            {
                await AwaitWithShortDeadline(Task.WhenAll(labels.Where(x => x.Data == null).Select(async label =>
                {
                    var version = await FetchAndStoreLabelerServiceMetadataDict.GetValueAsync(label.ModeratorDid!, RequestContext.CreateForTaskDictionary(ctx));
                    ctx.BumpMinimumVersion(version.MinVersion);
                    WithRelationshipsLock(rels =>
                    {
                        label.Data = rels.TryGetLabelData(label.LabelId);
                    }, ctx);
                })), ctx);
            }
            return labels;
        }


        private async Task<BlueskyList[]> EnrichAsync(BlueskyList[] lists, RequestContext ctx, CancellationToken ct = default)
        {
            if (lists.Length == 0) return lists;

            if (!IsReadOnly)
            {
                await AwaitWithShortDeadline(Task.WhenAll(lists.Where(x => x.Data == null).Select(async list =>
                {
                    var version = await FetchAndStoreListMetadataDict.GetValueAsync(list.ListIdStr, RequestContext.CreateForTaskDictionary(ctx));
                    ctx.BumpMinimumVersion(version);
                    WithRelationshipsLock(rels =>
                    {
                        list.Data = rels.TryGetListData(list.ListId);
                    }, ctx);

                })), ctx);
            }
            await EnrichAsync(lists.Select(x => x.Moderator!).ToArray(), ctx, ct: ct, omitLabelsAndViewerFlags: true /* avoid infinite recursion */);
            return lists;
        }


        public async Task<(BlueskyList List, BlueskyProfile[] Page, string? NextContinuation)> GetListSubscribersAsync(string did, string rkey, string? continuation, int limit, RequestContext ctx)
        {
            EnsureLimit(ref limit);
            var listId = WithRelationshipsLockForDid(did, (plc, rels) => new Models.Relationship(plc, Tid.Parse(rkey)), ctx);
            var list = WithRelationshipsLock(rels => rels.GetList(listId, ctx: ctx), ctx);

            await EnrichAsync([list], ctx);

            Relationship? parsedContinuation = continuation != null ? Relationship.Deserialize(continuation) : null;
            var pagePlusOne = WithRelationshipsLock(rels => rels.ListSubscribers.GetValuesSorted(listId, parsedContinuation).Where(x => !rels.ListBlockDeletions.ContainsKey(x)).Take(limit + 1).Select(x => rels.GetProfile(x.Actor, x.RelationshipRKey, ctx)).ToArray(), ctx);
            var nextContinuation = pagePlusOne.Length > limit ? new Relationship(pagePlusOne[^2].Plc, pagePlusOne[^2].RelationshipRKey!.Value).Serialize() : null;
            var page = pagePlusOne.Take(limit).ToArray();
            await EnrichAsync(page, ctx);
            return (list, page, nextContinuation);
        }

        public async Task<(BlueskyLabel Label, BlueskyProfile[] Page, string? NextContinuation)> GetLabelMembersAsync(string did, string shortname, string? continuation, int limit, RequestContext ctx)
        {
            EnsureLimit(ref limit, 20);
            var labelId = WithRelationshipsLockForDid(did, (plc, rels) => new LabelId(plc, BlueskyRelationships.HashLabelName(shortname)), ctx);
            var label = WithRelationshipsLock(rels => rels.GetLabel(labelId, ctx), ctx);

            await EnrichAsync([label], ctx);

            Plc? parsedContinuation = continuation != null ? StringUtils.DeserializeFromString<Plc>(continuation) : null;
            HashSet<LabelId> labelSet = [labelId];
            var members = WithRelationshipsLock(rels => rels.LabelToProfiles.GetValuesSortedDescending(labelId, null, parsedContinuation).Distinct().Where(x => rels.GetProfileLabels(x, labelSet).Length != 0).Take(limit + 1).Select(x => rels.GetProfile(x)).ToArray(), ctx);
            var hasMore = members.Length > limit;
            if (hasMore)
                members = members.AsSpan(0, limit).ToArray();
            await EnrichAsync(members, ctx);
            return (label, members, hasMore ? StringUtils.SerializeToString(members[^2].Plc) : null);

        }

        public async Task<(BlueskyLabel Label, BlueskyPost[] Page, string? NextContinuation)> GetLabelPostsAsync(string did, string shortname, string? continuation, int limit, RequestContext ctx)
        {
            EnsureLimit(ref limit, 20);
            var labelId = WithRelationshipsLockForDid(did, (plc, rels) => new LabelId(plc, BlueskyRelationships.HashLabelName(shortname)), ctx);
            var label = WithRelationshipsLock(rels => rels.GetLabel(labelId, ctx), ctx);

            await EnrichAsync([label], ctx);

            PostIdTimeFirst? parsedContinuation = continuation != null ? StringUtils.DeserializeFromString<PostIdTimeFirst>(continuation) : null;
            HashSet<LabelId> labelSet = [labelId];
            var posts = WithRelationshipsLock(rels => rels.LabelToPosts.GetValuesSortedDescending(labelId, null, parsedContinuation).Distinct().Where(x => rels.GetPostLabels(x, labelSet).Length != 0).Take(limit + 1).Select(x => rels.GetPost(x)).ToArray(), ctx);
            var hasMore = posts.Length > limit;
            if (hasMore)
                posts = posts.AsSpan(0, limit).ToArray();
            await EnrichAsync(posts, ctx);
            return (label, posts, hasMore ? StringUtils.SerializeToString((PostIdTimeFirst)posts[^2].PostId) : null);

        }

        public async Task<(BlueskyList List, BlueskyProfile[] Page, string? NextContinuation)> GetListMembersAsync(string did, string rkey, string? continuation, int limit, RequestContext ctx)
        {
            EnsureLimit(ref limit);
            var listId = WithRelationshipsLockForDid(did, (plc, rels) => new Models.Relationship(plc, Tid.Parse(rkey)), ctx);
            var list = WithRelationshipsLock(rels => rels.GetList(listId, ctx: ctx), ctx);

            await EnrichAsync([list], ctx);
#if false
            ListEntry? parsedContinuation = continuation != null ? ListEntry.Deserialize(continuation) : null;
            var members = WithRelationshipsLock(rels => rels.ListItems.GetValuesSorted(listId, parsedContinuation).Take(limit + 1).Select(x => rels.GetProfile(x.Member, x.ListItemRKey)).ToArray());
            var hasMore = members.Length > limit;
            if (hasMore)
                members = members.AsSpan(0, limit).ToArray();
            await EnrichAsync(members, ctx);
            return (list, members, hasMore ? new ListEntry(members[^1].Plc, members[^1].RelationshipRKey!.Value).Serialize() : null);
#else

            var response = await ListRecordsAsync(did, Listitem.RecordType, limit, cursor: continuation, ctx);
            var members = WithRelationshipsUpgradableLock(rels =>
            {
                return response!.Records!.TrySelect(x =>
                {
                    var listItem = (FishyFlip.Lexicon.App.Bsky.Graph.Listitem)x.Value!;
                    if (listItem.List!.Rkey != rkey) return null;
                    var profile = rels.GetProfile(rels.SerializeDid(listItem.Subject!.Handler, ctx), Tid.Parse(x.Uri.Rkey), ctx);
                    return profile;
                }).WhereNonNull().ToArray();
            }, ctx);
            await EnrichAsync(members, ctx);
            return (list, members, response.Cursor);

#endif
        }

        public async Task<ListRecordsOutput> ListRecordsAsync(string did, string collection, int limit, string? cursor, RequestContext ctx, bool descending = true, CancellationToken ct = default)
        {
            using var proto = await TryCreateProtocolForDidAsync(did, ctx);
            if (proto == null) return new ListRecordsOutput(null, []);
            try
            {
                return (await proto.ListRecordsAsync(GetAtId(did), collection, limit, cursor, descending ? null : true, cancellationToken: ct)).HandleResult()!;
            }
            catch (Exception ex)
            {
                throw await CreateExceptionMessageForExternalServerErrorAsync($"The PDS of this user", ex, did, GetPds(proto), ctx);
            }

        }

        private static string? GetPds(ATProtocol proto)
        {
            return proto.Options.Url.AbsoluteUri;
        }

        public async Task<GetRecordOutput> GetRecordAsync(string did, string collection, string rkey, RequestContext ctx, CancellationToken ct = default)
        {
            using var proto = await CreateProtocolForDidAsync(did, ctx);
            try
            {
                return (await proto.GetRecordAsync(GetAtId(did), collection, rkey, cancellationToken: ct)).HandleResult()!;
            }
            catch (Exception ex)
            {
                throw await CreateExceptionMessageForExternalServerErrorAsync($"The PDS of this user", ex, did, GetPds(proto), ctx);
            }

        }


        public async Task<BlueskyFeedGenerator[]> GetPinnedFeedsAsync(RequestContext ctx)
        {
            var feeds = WithRelationshipsLock(rels => ctx.PrivateProfile.FeedSubscriptions.Select(x => rels.GetFeedGenerator(new Plc(x.FeedPlc), x.FeedRKey, ctx)).ToArray(), ctx);
            await EnrichAsync(feeds, ctx);
            return feeds.OrderBy(x => x.DisplayNameOrFallback).ToArray();
        }
        public async Task<(BlueskyFeedGenerator[] Feeds, string? NextContinuation)> GetPopularFeedsAsync(string? continuation, int limit, RequestContext ctx)
        {
            EnsureLimit(ref limit);
            var feeds = WithRelationshipsLock(rels =>
            {
                var minLikes = 1024;
                var best = new PriorityQueue<RelationshipHashedRKey, long>();
                var added = new HashSet<RelationshipHashedRKey>();
                while (minLikes >= BlueskyRelationships.SearchIndexFeedPopularityMinLikes && best.Count < limit)
                {
                    var results = rels.SearchFeeds(["%likes-" + minLikes], RelationshipHashedRKey.MaxValue);
                    foreach (var result in results)
                    {
                        if (added.Add(result))
                        {
                            best.Enqueue(result, -rels.FeedGeneratorLikes.GetActorCount(result));
                        }
                    }
                    minLikes /= 2;
                }


                var list = new List<BlueskyFeedGenerator>();
                for (int i = 0; i < limit; i++)
                {
                    if (best.TryDequeue(out var r, out _))
                    {
                        var f = rels.TryGetFeedGenerator(r, ctx);
                        if (f != null)
                            list.Add(f);
                    }
                }
                return list;

            }, ctx);

            return (await EnrichAsync(feeds.ToArray(), ctx), null);
        }

        public async Task<(BlueskyFeedGenerator[] Feeds, string? NextContinuation)> SearchFeedsAsync(string query, string? continuation, int limit, RequestContext ctx)
        {
            EnsureLimit(ref limit);

            RelationshipHashedRKey? parsedContinuation = continuation != null ? RelationshipHashedRKey.Deserialize(continuation) : null;
            var queryWords = StringUtils.GetDistinctWords(query);
            if (queryWords.Length == 0) return ([], null);
            var feeds = WithRelationshipsLock(rels =>
            {
                return rels.SearchFeeds(queryWords, parsedContinuation ?? RelationshipHashedRKey.MaxValue)
                .Select(x => rels.TryGetFeedGenerator(x, ctx)!)
                .Where(x =>
                {
                    var words = StringUtils.GetAllWords(x.Data?.DisplayName).Concat(StringUtils.GetAllWords(x.Data?.Description)).Distinct().ToArray();
                    return queryWords.All(x => words.Contains(x));
                })
                .Where(x => x != null && x.Data?.Deleted != true)
                .Take(limit + 1)
                .ToArray();
            }, ctx);
            await EnrichAsync(feeds, ctx);
            return GetPageAndNextPaginationFromLimitPlus1(feeds, limit, x => x.FeedId.Serialize());
        }



        public async Task<ProfilesAndContinuation> SearchProfilesAsync(string query, bool allowPrefixForLastWord, string? continuation, int limit, RequestContext ctx, Action<BlueskyProfile>? onLateDataAvailable = null)
        {
            EnsureLimit(ref limit);

            ProfileSearchContinuation parsedContinuation = continuation != null ? ProfileSearchContinuation.Deserialize(continuation) : new ProfileSearchContinuation(Plc.MaxValue, false);

            var profiles = new List<BlueskyProfile>();
            var alreadyReturned = new HashSet<Plc>();


            var (queryWords, wordPrefix) = StringUtils.GetDistinctWordsAndLastPrefix(query, allowPrefixForLastWord);
            if (queryWords.Length == 0 && wordPrefix == null) return ([], null);

            var followerCount = new Dictionary<Plc, long>();

            while (true)
            {
                var items = WithRelationshipsLock(rels =>
                {
                    return rels.SearchProfiles(queryWords, SizeLimitedWord8.Create(wordPrefix, out _), parsedContinuation.MaxPlc, alsoSearchDescriptions: parsedContinuation.AlsoSearchDescriptions)
                    .Select(x => rels.GetProfile(x, ctx))
                    .Where(x => rels.ProfileMatchesSearchTerms(x, parsedContinuation.AlsoSearchDescriptions, queryWords, wordPrefix))
                    .Where(x => alreadyReturned.Add(x.Plc))
                    .Select(x =>
                    {
                        followerCount[x.Plc] = rels.Follows.GetActorCount(x.Plc);
                        return x;
                    })
                    .Take(limit + 1 - profiles.Count)
                    .ToArray();
                }, ctx);

                profiles.AddRange(items);
                if (!parsedContinuation.AlsoSearchDescriptions && !(profiles.Count > limit))
                {
                    parsedContinuation = new ProfileSearchContinuation(Plc.MaxValue, true);
                }
                else
                {
                    break;
                }
            }



            var result = GetPageAndNextPaginationFromLimitPlus1(profiles.ToArray(), limit, x => new ProfileSearchContinuation(x.Plc, parsedContinuation.AlsoSearchDescriptions).Serialize());
            if (continuation == null)
            {
                var wordCount = queryWords.Length + (wordPrefix != null ? 1 : 0);
                if (wordCount >= 2)
                {
                    // we might not have display names for every user. retry by guessing handle.
                    var concatenated = string.Join(null, queryWords) + wordPrefix;
                    var (updatedSearchTerms, updatedWordPrefix) = wordPrefix != null ?
                        (Array.Empty<string>(), concatenated) :
                        ([concatenated], null);
                    alreadyReturned = result.Items.Select(x => x.Plc).ToHashSet();
                    var extra = WithRelationshipsLock(rels =>
                    {
                        return
                            rels.SearchProfiles(updatedSearchTerms, SizeLimitedWord8.Create(updatedWordPrefix, out _), Plc.MaxValue, false)
                            .Where(x => !alreadyReturned.Contains(x))
                            .Select(x => rels.GetProfile(x, ctx))
                            .Where(x => x.DisplayName == null) // otherwise should've matched earlier
                            .Where(x => rels.ProfileMatchesSearchTerms(x, alsoSearchDescriptions: false, updatedSearchTerms, updatedWordPrefix))
                            .Select(x =>
                            {
                                followerCount[x.Plc] = rels.Follows.GetActorCount(x.Plc);
                                return x;
                            })
                            .Take(limit)
                            .Where(x => alreadyReturned.Add(x.Plc))
                            .ToArray();
                    }, ctx);
                    result.Items = result.Items.Concat(extra).ToArray();
                }
            }

            await EnrichAsync(result.Items, ctx, onLateDataAvailable: onLateDataAvailable);


            return (result.Items.OrderByDescending(x => followerCount[x.Plc]).ToArray(), result.NextContinuation);
        }

        private static (T[] Items, string? NextContinuation) GetPageAndNextPaginationFromLimitPlus1<T>(T[] itemsPlusOne, int limit, Func<T, string> serialize)
        {
            var hasMore = itemsPlusOne.Length > limit;
            if (hasMore)
            {
                var items = itemsPlusOne.AsSpan(0, limit).ToArray();
                return (items, serialize(items[^1]));
            }
            else
            {
                return (itemsPlusOne, null);
            }
        }


        public async Task<(BlueskyList[] Lists, string? NextContinuation)> GetProfileListsAsync(string did, string? continuation, int limit, RequestContext ctx)
        {
            EnsureLimit(ref limit);

            var response = await ListRecordsAsync(did, List.RecordType, limit: limit + 1, cursor: continuation, ctx);
            var lists = WithRelationshipsLockForDid(did, (plc, rels) =>
            {
                return response!.Records!.TrySelect(x =>
                {
                    var listId = new Models.Relationship(plc, Tid.Parse(x.Uri.Rkey));
                    return rels.GetList(listId, BlueskyRelationships.ListToProto((List)x.Value), ctx: ctx);
                }).ToArray();
            }, ctx);
            await EnrichAsync(lists, ctx);
            return GetPageAndNextPaginationFromLimitPlus1(lists, limit, x => x.RKey);

        }


        public async Task<(BlueskyFeedGenerator[] Feeds, string? NextContinuation)> GetProfileFeedsAsync(string did, string? continuation, int limit, RequestContext ctx)
        {
            EnsureLimit(ref limit);

            var response = await ListRecordsAsync(did, Generator.RecordType, limit: limit + 1, cursor: continuation, ctx, descending: false);
            var feeds = WithRelationshipsUpgradableLock(rels =>
            {
                var plc = rels.SerializeDid(did, ctx);
                return response!.Records!.TrySelect(x =>
                {
                    var feedId = new RelationshipHashedRKey(plc, x.Uri.Rkey);
                    if (!rels.FeedGenerators.ContainsKey(feedId))
                    {
                        rels.WithWriteUpgrade(() => rels.IndexFeedGenerator(plc, x.Uri.Rkey, (Generator)x.Value, DateTime.UtcNow), ctx);
                    }
                    return rels.TryGetFeedGenerator(feedId, ctx)!;
                }).ToArray();
            }, ctx);
            await EnrichAsync(feeds, ctx);
            var result = GetPageAndNextPaginationFromLimitPlus1(feeds, limit, x => x.RKey);
            result.Items = result.Items.OrderBy(x => x.DisplayNameOrFallback, StringComparer.InvariantCultureIgnoreCase).ToArray();
            return result;

        }

        public static TimeSpan HandleToDidMaxStale = TimeSpan.FromHours(Math.Max(1, AppViewLiteConfiguration.GetInt32(AppViewLiteParameter.APPVIEWLITE_HANDLE_TO_DID_MAX_STALE_HOURS) ?? (10 * 24)));
        public static TimeSpan DidDocMaxStale = TimeSpan.FromHours(Math.Max(1, AppViewLiteConfiguration.GetInt32(AppViewLiteParameter.APPVIEWLITE_DID_DOC_MAX_STALE_HOURS) ?? (2 * 24)));

        public async Task<string> ResolveHandleOrUrlAsync(string handleOrUrl, Uri baseUrl, RequestContext ctx)
        {
            if (handleOrUrl.StartsWith("http://", StringComparison.Ordinal) || handleOrUrl.StartsWith("https://", StringComparison.Ordinal))
            {
                var urlOrDid = await TryResolveUrlToDidAsync(new Uri(handleOrUrl), baseUrl, ctx);
                if (urlOrDid == null) throw new Exception("Could not resolve to DID.");
                return urlOrDid;
            }
            return await ResolveHandleAsync(handleOrUrl, ctx);
        }

        public Task<string> ResolveHandleAsync(string handle, RequestContext ctx, string? activityPubInstance)
        {

            if (activityPubInstance != null)
                handle += "@" + activityPubInstance;
            return ResolveHandleAsync(handle, ctx);
        }
        public async Task<string> ResolveHandleAsync(string handle, RequestContext ctx, bool forceRefresh = false, bool allowUnendorsed = true)
        {

            handle = StringUtils.NormalizeHandle(handle);
            if (handle.StartsWith('@')) handle = handle.Substring(1);

            AdministrativeBlocklist.ThrowIfBlockedDisplay(handle);

            if (handle.StartsWith("did:", StringComparison.Ordinal))
            {
                EnsureValidDid(handle);
                WarmUpDidAssignment(handle, ctx);
                return handle;
            }


            foreach (var pluggableProtocol in AppViewLite.PluggableProtocols.PluggableProtocol.RegisteredPluggableProtocols)
            {
                if (pluggableProtocol.TryHandleToDid(handle) is { } pluggableDid)
                {
                    AdministrativeBlocklist.ThrowIfBlockedDisplay(pluggableDid);

                    var bridged = await TryGetBidirectionalAtProtoBridgeForFediverseProfileAsync(pluggableDid, ctx);
                    if (bridged != null)
                        return bridged;

                    WarmUpDidAssignment(pluggableDid, ctx);
                    return pluggableDid;
                }
            }

            EnsureValidDomain(handle);
            var handleUuid = StringUtils.HashUnicodeToUuid(handle);

            var (handleToDidVerificationDate, plc, did) = WithRelationshipsLock(rels =>
            {
                var lastVerification = rels.HandleToDidVerifications.TryGetLatestValue(handleUuid, out var r) ? r : default;

                var plc = lastVerification.Plc;
                var did = plc != default ? rels.GetDid(plc) : null;

                if (plc != default)
                {
                    // Check if we're aware of PLC directory updates for this handle. If so, force a refresh.
                    foreach (var possiblePlc in rels.HandleToPossibleDids.GetValuesUnsorted(BlueskyRelationships.HashWord(handle)).Distinct())
                    {

                        var didDoc = rels.TryGetLatestDidDoc(possiblePlc);
                        if (didDoc != null && didDoc.Date > lastVerification.VerificationDate && (!didDoc.HasHandle(handle) || possiblePlc != plc))
                        {
                            forceRefresh = true;
                            break;
                        }

                    }
                }

                return (lastVerification.VerificationDate, plc, did);
            }, ctx);



            if (forceRefresh || plc == default || (DateTime.UtcNow - handleToDidVerificationDate) > HandleToDidMaxStale)
            {
                var versioned = await HandleToDidAndStoreDict.GetValueAsync(handle, RequestContext.CreateForTaskDictionary(ctx, possiblyUrgent: true));
                versioned.BumpMinimumVersion(ctx);
                (handleToDidVerificationDate, plc, did) = WithRelationshipsLock(rels =>
                {
                    if (!rels.HandleToDidVerifications.TryGetLatestValue(handleUuid, out var lastVerification))
                        BlueskyRelationships.ThrowFatalError("Entry was not added to HandleToDidVerifications");
                    BlueskyRelationships.Assert(lastVerification.Plc != default);

                    return (lastVerification.VerificationDate, lastVerification.Plc, rels.GetDid(lastVerification.Plc));

                }, ctx);
            }
            if (plc == default) throw AssertionLiteException.Throw("ResolveHandleAsync plc is still default(Plc)");
            var didDoc = WithRelationshipsLock(rels =>
            {
                return rels.TryGetLatestDidDoc(plc);
            }, ctx);



            if (forceRefresh || IsDidDocStale(did!, didDoc))
            {
                // if this is did:plc, the did-doc will be retrieved from plc.directory (as trustworthy as RetrievePlcDirectoryAsync())
                // otherwise did:web, but they're in a different namespace
                didDoc = DidDocOverrides.GetValue().TryGetOverride(did!) ?? await FetchAndStoreDidDocNoOverrideAsync(plc, did!, ctx);
            }
            if (!didDoc!.HasHandle(handle))
            {
                if (!forceRefresh)
                {
                    return await ResolveHandleAsync(handle, forceRefresh: true, ctx: ctx);
                }


                if ("did:web:" + handle != did)
                {
                    // oldhandle.example => did:plc:123456 => newhandle.example
                    // oldhandle.example is NOT in diddoc.
                    // we don't know if did:plc:123456 endorses being referred to as oldhandle.example
                    // but at least we can provide a redirect (bsky.app does the same)

                    if (allowUnendorsed)
                        return did!;
                    throw new UnexpectedFirehoseDataException($"Bidirectional handle verification failed: {handle} => {did} => {didDoc.Handle}");
                }
            }

            foreach (var extraHandle in didDoc.AllHandlesAndDomains)
            {
                AdministrativeBlocklist.ThrowIfBlockedDisplay(extraHandle);
            }

            return did!;
        }

        private void WarmUpDidAssignment(string did, RequestContext ctx)
        {
            // assign a Plc (in case not all future code paths properly pass ctx to SerializeDid)
            var plc2 = WithRelationshipsLock(rels => rels.TrySerializeDidMaybeReadOnly(did, ctx), ctx);
            if (plc2 == default) WithRelationshipsWriteLock(rels => rels.SerializeDid(did, ctx), ctx);
        }

        private async Task<DidDocProto> FetchAndStoreDidDocNoOverrideCoreAsync(string did, Plc plc, RequestContext anyCtx)
        {
            var didDoc = await GetDidDocCoreNoOverrideAsync(did);
            didDoc.Date = DateTime.UtcNow;
            return WithRelationshipsWriteLock(rels =>
            {
                var prev = rels.TryGetLatestDidDoc(plc);
                // /did:plc:xxxx and /did:plc:xxxx/data endpoints don't return dates, only /did:plc/log/audit does.
                // let's not bother fetching earliest date from plc.directory, it will be overwritten once the plc directory is fully synced.
                if (prev != null)
                {
                    didDoc.EarliestDateApprox16 = prev.EarliestDateApprox16;
                }
                rels.CompressDidDoc(didDoc);
                rels.DidDocs.AddRange(plc, didDoc.SerializeToBytes());
                return rels.TryGetLatestDidDoc(plc)!;
            }, anyCtx);
        }

        private bool IsDidDocStale(string did, DidDocProto? didDoc)
        {
            if (didDoc == null) return true;

            if ((DateTime.UtcNow - didDoc.Date) <= DidDocMaxStale)
            {
                return false;
            }
            if (did.StartsWith("did:plc:", StringComparison.Ordinal) && PlcDirectoryStaleness <= DidDocMaxStale)
            {
                return false;
            }

            return true;
        }

        public DateTime PlcDirectorySyncDate => relationshipsUnlocked.PlcDirectorySyncDate;
        public TimeSpan PlcDirectoryStaleness => relationshipsUnlocked.PlcDirectoryStaleness;


        private readonly static string DnsForTxtResolution = AppViewLiteConfiguration.GetString(AppViewLiteParameter.APPVIEWLITE_DNS_SERVER) ?? "1.1.1.1";
        private readonly static bool DnsUseHttps = AppViewLiteConfiguration.GetBool(AppViewLiteParameter.APPVIEWLITE_USE_DNS_OVER_HTTPS) ?? true;


        private async Task<Versioned<string>> HandleToDidAndStoreCoreAsync(string handle, RequestContext ctx)
        {
            AdministrativeBlocklist.ThrowIfBlockedOutboundConnection(handle);


            var attemptingWellKnown = false;

            try
            {
                // Is it valid to have multiple TXTs listing different DIDs? bsky.app seems to support that.
                //LogInfo("ResolveHandleCoreAsync: " + handle);

                if (!handle.EndsWith(".bsky.social", StringComparison.Ordinal)) // avoid wasting time, bsky.social uses .well-known
                {
                    string? record;
                    if (DnsUseHttps)
                    {
                        using var request = new HttpRequestMessage(HttpMethod.Get, "https://" + DnsForTxtResolution + "/dns-query?name=_atproto." + Uri.EscapeDataString(handle) + "&type=TXT");
                        request.Headers.Accept.Clear();
                        request.Headers.Accept.ParseAdd("application/dns-json");
                        using var response = await DefaultHttpClient.SendAsync(request);
                        var result = await response.Content.ReadFromJsonAsync<DnsOverHttpsResponse>();
                        record = result!.Answer?.Where(x => x.type == 16).Select(x => Regex.Match(x.data, @"did=[^\\""]+").Value?.Trim()).FirstOrDefault(x => !string.IsNullOrEmpty(x));
                    }
                    else
                    {
                        var lookup = new LookupClient(System.Net.IPAddress.Parse(DnsForTxtResolution), 53);
                        var result = await lookup.QueryAsync("_atproto." + handle, QueryType.TXT);
                        record = result.Answers.TxtRecords()
                            .Select(x => x.Text.Select(x => x.Trim()).FirstOrDefault(x => !string.IsNullOrEmpty(x)))
                            .FirstOrDefault(x => x != null && x.StartsWith("did=", StringComparison.Ordinal));
                    }
                    if (record != null)
                    {
                        var did = record.Substring(4);
                        EnsureValidDid(did);
                        return WithRelationshipsWriteLock(rels =>
                        {
                            rels.IndexHandle(handle, did, ctx);
                            rels.AddHandleToDidVerification(handle, rels.SerializeDid(did, ctx));
                            return rels.AsVersioned(did);
                        }, ctx);
                    }
                }
                attemptingWellKnown = true;
                var s = (await DefaultHttpClient.GetStringAsync("https://" + handle + "/.well-known/atproto-did")).Trim();
                EnsureValidDid(s);
                return WithRelationshipsWriteLock(rels =>
                {
                    rels.IndexHandle(handle, s, ctx);
                    rels.AddHandleToDidVerification(handle, rels.SerializeDid(s, ctx));
                    return rels.AsVersioned(s);
                }, ctx);
            }
            catch (Exception ex)
            {
                WithRelationshipsWriteLock(rels =>
                {
                    rels.AddHandleToDidVerification(handle, default);
                }, ctx);
                throw new UnexpectedFirehoseDataException($"Could not resolve handle: " + (attemptingWellKnown && ex is HttpRequestException {StatusCode: { } sc } hre ? "HTTP " + (int)sc + " " + sc : ex.Message), ex);
            }
        }


        private readonly static SearchValues<char> DidWebAllowedChars = SearchValues.Create("0123456789abcdefghijklmnopqrstuvwxyz-.");


        public static bool IsValidDid(string? did)
        {
            if (string.IsNullOrEmpty(did)) return false;
            try
            {
                EnsureValidDid(did);
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }
        public static void EnsureValidDid(string did)
        {
            if (string.IsNullOrEmpty(did)) throw new UnexpectedFirehoseDataException("The provided DID is null or empty.");
            if (did.StartsWith("did:plc:", StringComparison.Ordinal))
            {
                if (did.Length != 32) throw new UnexpectedFirehoseDataException("Invalid did:plc: length.");
                if (did.AsSpan(8).ContainsAnyExcept(AtProtoS32.Base32SearchValues)) throw new UnexpectedFirehoseDataException("did:plc: contains invalid base32 characters.");

            }
            else if (did.StartsWith("did:web:", StringComparison.Ordinal))
            {
                var domain = did.AsSpan(8);
                EnsureValidDomain(domain);
                //var domain2 = did.Substring(8);
                // this is actually ok
                //if (domain != domain2) throw new Exception("Mismatching domain for did:web: and .well-known/TXT");
            }
            else
            {
                if (did.Contains('%')) throw new UnexpectedFirehoseDataException("Invalid DID: " + did);
                var colon = did.IndexOf(':', 4);
                if (colon != -1)
                {
                    var pluggable = BlueskyRelationships.TryGetPluggableProtocolForDid(did);
                    if (pluggable != null)
                    {
                        pluggable.EnsureValidDid(did);
                        return;
                    }
                    throw new UnexpectedFirehoseDataException(string.Concat("Invalid did or no pluggable protocol registered for ", did.AsSpan(0, colon)));
                }
                throw new UnexpectedFirehoseDataException("Invalid did.");
            }
        }


        public static bool IsValidDomain(ReadOnlySpan<char> domain)
        {
            if (domain.IsEmpty || !domain.Contains('.')) return false; // fast path
            if (domain.Contains('ü')) return false; // avoid frequent noisy exception while debugging
            try
            {
                EnsureValidDomain(domain);
                return true;
            }
            catch (ArgumentException)
            {
                return false;
            }

        }
        public static void EnsureValidDomain(ReadOnlySpan<char> domain)
        {
            if (domain.IsEmpty) throw new ArgumentException("Empty domain or handle.");
            if (domain.Length > 253) throw new ArgumentException("Domain or handle is too long.");
            if (domain.ContainsAnyExcept(DidWebAllowedChars)) throw new ArgumentException("Domain or handle contains invalid characters.");
            if (domain[0] == '.' || domain[^1] == '.') throw new ArgumentException("Domain or handle starts or ends with a dot.");
            if (!domain.Contains('.')) throw new ArgumentException("Domain or handle should contain at least one dot.");
            if (domain.Contains("..", StringComparison.Ordinal)) throw new ArgumentException("Domain or handle contains multiple consecutive dots.");
            if (domain[0] == '-' || domain[^1] == '-' || domain.Contains(".-", StringComparison.Ordinal) || domain.Contains("_.", StringComparison.Ordinal))
                throw new ArgumentException("Domain or handle contains parts that start or end with dashes.");
        }

        internal static async Task<DidDocProto> GetDidDocCoreNoOverrideAsync(string did)
        {
            LogInfo("GetDidDocAsync: " + did);
            string didDocUrl;
            if (did.StartsWith("did:web:", StringComparison.Ordinal))
            {
                var host = did.Substring(8);
                didDocUrl = "https://" + host + "/.well-known/did.json";
            }
            else if (did.StartsWith("did:plc:", StringComparison.Ordinal))
            {
                didDocUrl = PlcDirectoryPrefix + "/" + did;
            }
            else throw new ArgumentException("Unsupported did method: " + did);


            var didDocJson = await DefaultHttpClient.GetFromJsonAsync<DidWebRoot>(didDocUrl);
            var didDoc = Indexer.DidDocToProto(didDocJson!);
            return didDoc;
        }

        public readonly static string PlcDirectoryPrefix = AppViewLiteConfiguration.GetString(AppViewLiteParameter.APPVIEWLITE_PLC_DIRECTORY) ?? "https://plc.directory";



        public async Task<BlobResult> GetBlobAsync(string did, string cid, string? pds, ThumbnailSize preferredSize, RequestContext ctx, CancellationToken ct)
        {
            AdministrativeBlocklist.ThrowIfBlockedOutboundConnection(did);
            AdministrativeBlocklist.ThrowIfBlockedOutboundConnection(DidDocProto.GetDomainFromPds(pds));


            if (did.StartsWith("host:", StringComparison.Ordinal))
            {
                var url = new Uri(string.Concat("https://", did.AsSpan(5), Encoding.UTF8.GetString(Base32.FromBase32(cid))));
                return await GetBlobFromUrl(url, ct: ct);
            }
            else
            {

                var pluggable = BlueskyRelationships.TryGetPluggableProtocolForDid(did);
                if (pluggable != null)
                {
                    return await pluggable.GetBlobAsync(did, Ipfs.Base32.FromBase32(cid), preferredSize, ct: ct);
                }

                if (pds != null && !pds.Contains(':'))
                {
                    pds = "https://" + pds;
                }
                if (pds == null)
                {
                    pds = (await GetDidDocAsync(did, ctx)).Pds;
                }
                return await GetBlobFromUrl(new Uri($"{pds}/xrpc/com.atproto.sync.getBlob?did={did}&cid={cid}"), ignoreFileName: true, ct: ct, preferredSize: preferredSize);
            }
        }

        public async Task<DidDocProto> GetDidDocAsync(string did, RequestContext ctx)
        {
            var didDocOverride = DidDocOverrides.GetValue().TryGetOverride(did);
            if (didDocOverride != null) return didDocOverride;

            DidDocProto? doc;
            (var plc, doc) = WithRelationshipsLockForDid(did, (plc, rels) =>
            {
                return (plc, rels.TryGetLatestDidDoc(plc));
            }, ctx);
            if (IsDidDocStale(did, doc))
            {
                doc = await FetchAndStoreDidDocNoOverrideAsync(plc, did, ctx);
            }

            return doc!;
        }
        public async Task<ATUri> ResolveUriAsync(string uri, RequestContext ctx)
        {
            var aturi = new ATUri(uri);
            if (aturi.Did != null) return aturi;

            var did = await ResolveHandleAsync(aturi.Handle!.Handle, ctx);
            return new ATUri("at://" + did + aturi.Pathname + aturi.Hash);
        }

        public void RegisterPluggableProtocol(Type type)
        {
            var protocol = AppViewLite.PluggableProtocols.PluggableProtocol.Register(type);
            protocol.Apis = this;
        }

        public void MaybeAddCustomEmojis(CustomEmoji[]? emojis, RequestContext ctx)
        {
            if (emojis == null || emojis.Length == 0) return;

            var missingEmojis = WithRelationshipsLock(rels => emojis.Where(x => !rels.CustomEmojis.ContainsKey(x.Hash)).ToArray(), ctx);

            if (missingEmojis.Length == 0) return;
            WithRelationshipsWriteLock(rels =>
            {
                foreach (var emoji in missingEmojis)
                {
                    rels.CustomEmojis.AddRange(emoji.Hash, BlueskyRelationships.SerializeProto(emoji));
                }
            }, ctx);
        }

        public ConcurrentFullEvictionCache<DuckDbUuid, CustomEmoji?> CustomEmojiCache = new(64 * 1024);
        public CustomEmoji? TryGetCustomEmoji(DuckDbUuid hash, RequestContext ctx)
        {
            if (CustomEmojiCache.TryGetValue(hash, out var result))
                return result;
            result = WithRelationshipsLock(rels =>
            {
                if (rels.CustomEmojis.TryGetPreserveOrderSpanAny(hash, out var bytes))
                    return BlueskyRelationships.DeserializeProto<CustomEmoji>(bytes.AsSmallSpan());
                return null;
            }, ctx);
            CustomEmojiCache.Add(hash, result);
            return result;
        }

        public readonly TaskDictionary<(Plc Plc, RepositoryImportKind Kind, bool Incremental), (RepositoryImportEntry? Previous, string Did, bool IsRegisteredUser, bool SlowImport, RequestContext? AuthenticatedCtx, RequestContext Ctx), RepositoryImportEntry> CarImportDict;
        public readonly static HttpClient DefaultHttpClient;
        public readonly static HttpClient DefaultHttpClientForRss;
        public readonly static HttpClient DefaultHttpClientNoDefaultHeaders;
        public readonly static HttpClient DefaultHttpClientNoAutoRedirect;
        public readonly static HttpClient DefaultHttpClientOpenGraph;

        static BlueskyEnrichedApis()
        {
            DefaultHttpClient = CreateHttpClient(autoredirect: true);
            DefaultHttpClientNoDefaultHeaders = CreateHttpClient(autoredirect: true, defaultHeaders: false);
            DefaultHttpClientNoAutoRedirect = CreateHttpClient(autoredirect: false);
            DefaultHttpClientForRss = CreateHttpClient(autoredirect: false, rateLimitingRealm: "Rss", timeoutIncludingRateLimiting: TimeSpan.FromHours(2));
            DefaultHttpClientOpenGraph = CreateHttpClient(autoredirect: false, defaultHeaders: true, userAgent: "facebookexternalhit/1.1");
            Instance = null!;
        }

        private static HttpClient CreateHttpClient(bool autoredirect, bool defaultHeaders = true, string? userAgent = null, string? rateLimitingRealm = null, TimeSpan timeoutIncludingRateLimiting = default)
        {
            var client = new HttpClient(new BlocklistableHttpClientHandler(new SocketsHttpHandler
            {
                AllowAutoRedirect = autoredirect,
                AutomaticDecompression = System.Net.DecompressionMethods.All,
            }, true)
            {
                Timeout = TimeSpan.FromSeconds(10),
                RateLimitingRealm = rateLimitingRealm,
            }, true);
            if (defaultHeaders)
                client.DefaultRequestHeaders.UserAgent.ParseAdd(userAgent ?? "Mozilla/5.0");
            client.MaxResponseContentBufferSize = 10 * 1024 * 1024;
            //client.Timeout = TimeSpan.FromSeconds(10);
            if (timeoutIncludingRateLimiting == default)
                timeoutIncludingRateLimiting = TimeSpan.FromMinutes(5);
            client.Timeout = timeoutIncludingRateLimiting; // HttpClient launches its own timeout cancellation, even if the delay is intentional from us (max QPS)
            return client;
        }

#pragma warning disable CA1822
        public AdministrativeBlocklist AdministrativeBlocklist => AdministrativeBlocklist.Instance.GetValue();
#pragma warning restore CA1822

        public async Task<string?> TryGetBidirectionalAtProtoBridgeForFediverseProfileAsync(string maybeFediverseDid, RequestContext ctx)
        {
            if (!maybeFediverseDid.StartsWith(AppViewLite.PluggableProtocols.ActivityPub.ActivityPubProtocol.DidPrefix, StringComparison.Ordinal))
                return null;

            var userId = AppViewLite.PluggableProtocols.ActivityPub.ActivityPubProtocol.ParseDid(maybeFediverseDid);

            var possibleHandle = userId.UserName + "." + userId.Instance + ".ap.brid.gy";
            if (!WithRelationshipsLock(rels => rels.HandleToPossibleDids.ContainsKey(BlueskyRelationships.HashWord(possibleHandle)), ctx))
                return null;

            try
            {
                var resolved = await ResolveHandleAsync(possibleHandle, ctx, allowUnendorsed: false);
                return resolved;
            }
            catch
            {
                return null;
            }
        }

        public static async Task<BlobResult> GetBlobFromUrl(Uri url, bool ignoreFileName = false, bool? stream = null, ThumbnailSize preferredSize = default, CancellationToken ct = default)
        {
            stream ??= preferredSize == ThumbnailSize.feed_video_blob;
            var response = await DefaultHttpClient.GetAsync(url, HttpCompletionOption.ResponseHeadersRead, ct);
            var responseToDispose = response;

            try
            {
                var contentLength = response.Content.Headers.ContentLength;
                response.EnsureSuccessStatusCode();

                string? fileName = null;
                if (!ignoreFileName)
                {
                    var disposition = response.Content.Headers.ContentDisposition;
                    fileName = disposition?.FileNameStar != null ? Uri.UnescapeDataString(disposition.FileNameStar) : disposition?.FileName;
                    fileName = fileName?.Replace("\"", null);
                    if (string.IsNullOrEmpty(fileName))
                        fileName = url.GetFileName();
                }

                if (stream.Value)
                {
                    var s = await response.Content.ReadAsStreamAsync(ct);
                    responseToDispose = null;
                    return new BlobResult(null, s, fileName);
                }
                else
                {
                    var bytes = await response.Content.ReadAsByteArrayAsync(ct);
                    if (contentLength != null && contentLength != bytes.Length)
                        throw new HttpRequestException(bytes.Length < contentLength ? "Truncated response received from the server." : "Mismatching Content-Length.");
                    return new BlobResult(bytes, null, fileName);
                }

            }
            finally
            {
                responseToDispose?.Dispose();
            }
        }

        public void MarkAsRead(PostEngagementStr[] postEngagementsStr, Plc loggedInUser, RequestContext ctx)
        {
            if (postEngagementsStr.Length == 0) return;
            var creditsToConsume = new List<Plc>();
            WithRelationshipsWriteLock(rels =>
            {
                var seenPostsSlices = rels.SeenPosts.GetValuesChunked(loggedInUser).ToArray();
                var now = DateTime.UtcNow;
                foreach (var engagementStr in postEngagementsStr)
                {
                    var postId = rels.GetPostId(engagementStr.PostId.Did, engagementStr.PostId.RKey, ctx);
                    if ((engagementStr.Kind & PostEngagementKind.SeenInFollowingFeed) != 0 && !BlueskyRelationships.IsPostSeen(postId, seenPostsSlices))
                        creditsToConsume.Add(postId.Author);
                    rels.SeenPosts.Add(loggedInUser, new PostEngagement(postId, engagementStr.Kind));
                    rels.SeenPostsByDate.Add(loggedInUser, new TimePostSeen(now, postId));
                    ctx.UserContext.RecentlySeenOrAlreadyDiscardedFromFollowingFeedPosts?.TryAdd(postId);
                    now = now.AddTicks(1);


                }

            }, ctx);

            if (creditsToConsume.Count != 0)
            {
                lock (ctx.UserContext)
                {
                    foreach (var consume in creditsToConsume)
                    {
                        ConsumeFollowingFeedCreditsMustHoldLock(ctx, consume);
                    }
                }
            }
        }

        public static async Task<Uri> GetFaviconUrlAsync(Uri pageUrl)
        {
            var dom = StringUtils.ParseHtml(await DefaultHttpClient.GetStringAsync(pageUrl));
            if (pageUrl.HasHostSuffix("tumblr.com"))
            {
                var img = dom.QuerySelectorAll("img[alt=Avatar]").FirstOrDefault(x =>
                {
                    return Uri.TryCreate(pageUrl, x.Closest("a")?.GetAttribute("href"), out var url) && url.GetSegments().FirstOrDefault() == pageUrl.Host.Replace(".tumblr.com", null);
                });
                if (img != null && StringUtils.GetSrcSetLargestImageUrl(img, pageUrl) is { } avatarUrl)
                {
                    return avatarUrl;
                }
            }
            var href = dom.QuerySelector("link[rel='icon'],link[rel='shortcut icon']")?.GetAttribute("href");
            if (!string.IsNullOrEmpty(href))
            {
                return new Uri(pageUrl, href);
            }
            return new Uri(pageUrl.GetLeftPart(UriPartial.Authority) + "/favicon.ico");
        }

        public void PopulateViewerFlags(BlueskyProfile[] profiles, RequestContext ctx)
        {
            if (!profiles.Any(x => x.PrivateFollow == null)) return;
            WithRelationshipsLock(rels =>
            {
                foreach (var profile in profiles)
                {
                    rels.PopulateViewerFlags(profile, ctx);
                }
            }, ctx);
        }

        public void TogglePrivateFollowFlag(string did, PrivateFollowFlags flag, bool enabled, RequestContext ctx)
        {
            WithRelationshipsWriteLock(rels =>
            {
                var plc = rels.SerializeDid(did, ctx);
                var info = ctx.UserContext.GetPrivateFollow(plc);
                if (enabled)
                    info.Flags |= flag;
                else
                    info.Flags &= ~flag;
                if (enabled && flag == PrivateFollowFlags.PrivateFollow)
                {
                    info.DatePrivateFollowed = DateTime.UtcNow;
                    if (did.StartsWith(AppViewLite.PluggableProtocols.Rss.RssProtocol.DidPrefix, StringComparison.Ordinal))
                    {
                        rels.RssFeedToFollowers.AddIfMissing(plc, ctx.LoggedInUser);
                    }
                }
                rels.UpdatePrivateFollow(info, ctx);
            }, ctx);
        }

        public async Task OnSessionCreatedOrRestoredAsync(AppViewLiteUserContext session, RequestContext ctx)
        {

            lock (session)
            {
                if (session.InitializeAsync == null)
                    session.InitializeAsync = OnSessionCreatedOrRestoredCoreAsync(session, ctx);

            }
            await session.InitializeAsync!;



        }

        private async Task OnSessionCreatedOrRestoredCoreAsync(AppViewLiteUserContext userContext, RequestContext ctx)
        {
            // synchronous part
            var did = userContext.Did!;
            var plc = userContext.Profile!.Plc;

            lock (userContext)
            {
                userContext.PrivateFollows = (userContext.PrivateProfile!.PrivateFollows ?? []).ToDictionary(x => new Plc(x.Plc), x => x);
            }
            userContext.PrivateProfile!.MuteRules ??= [];

            // asynchronous part
            await EnrichAsync([userContext.Profile], ctx);

            if (!userContext.PrivateProfile.ImportedFollows)
            {
                var deadline = Task.Delay(5000);
                var importFollows = ImportFollowsForRegisteredUserAsync(userContext, ctx);
                await Task.WhenAny(deadline, importFollows);
                ctx.BumpMinimumVersion(long.MaxValue); // so that we see the latest follow imports
            }

            ImportLowPriorityCollectionsForRegisteredUserAsync(userContext, RequestContext.ToNonUrgent(ctx)).FireAndForget();
        }

        private async Task<RepositoryImportEntry> ImportFollowsForRegisteredUserAsync(AppViewLiteUserContext userContext, RequestContext ctx)
        {

            var dictKey = (userContext.Plc, RepositoryImportKind.Follows, false);
            if (CarImportDict.TryGetExtraArgs(dictKey, out var previousExtraArgs) && !previousExtraArgs.IsRegisteredUser)
            {
                // Unlikely case that a cached task is lingering (< 30s ago) that started before the first login
                CarImportDict.Remove(dictKey);
            }

            var loadFollows = await ImportCarIncrementalAsync(userContext.Plc, Models.RepositoryImportKind.Follows, ctx, ignoreIfPrevious: x => false, incremental: false);
            if (loadFollows!.Error == null)
            {
                userContext.PrivateProfile!.ImportedFollows = true;
                SaveAppViewLiteProfile(ctx);
            }
            return loadFollows;
        }

        private async Task ImportLowPriorityCollectionsForRegisteredUserAsync(AppViewLiteUserContext userContext, RequestContext ctx)
        {
            await EnsureHaveBlocksForUserAsync(userContext.Plc, ctx);
            await EnsureHaveCollectionAsync(userContext.Plc, RepositoryImportKind.FollowersBskyAppBackfill, ctx);
        }


        public static bool IsQuickBackfillCollection(RepositoryImportKind kind) => kind is RepositoryImportKind.FollowersBskyAppBackfill;
        public static bool RepositoryImportKindIncludesCollection(RepositoryImportKind importKind, RepositoryImportKind collection)
        {
            if (importKind == RepositoryImportKind.CAR && !IsQuickBackfillCollection(collection)) return true;
            return importKind == collection;
        }

        public Task EnsureHaveCollectionsAsync(IEnumerable<Plc> plcs, RepositoryImportKind kind, RequestContext ctx)
        {
            return Parallel.ForEachAsync(plcs.Distinct(), new ParallelOptions { MaxDegreeOfParallelism = 8 }, async (plc, ct) =>
            {
                await EnsureHaveCollectionAsync(plc, kind, ctx);
            });
        }
        public async Task<RepositoryImportEntry?> EnsureHaveCollectionAsync(Plc plc, RepositoryImportKind kind, RequestContext ctx, bool slowImport = false, bool ignoreIfDeactivated = true)
        {
            if (WithRelationshipsLock(rels =>
            {
                if (ignoreIfDeactivated && !rels.IsAccountActive(plc))
                    return true;
                return rels.HaveCollectionForUser(plc, kind);
            }, ctx))
                return null;
            return await ImportCarIncrementalAsync(plc, kind, ctx, ignoreIfPrevious: x => true, slowImport: slowImport);
        }

        public static string? TryGetSessionIdFromCookie(string? cookie, out string? unverifiedDid)
        {
            if (cookie != null)
            {
                var r = cookie.Split('=');
                unverifiedDid = r[1];
                return r[0];
            }
            unverifiedDid = null;
            return null;
        }

        public AppViewLiteSession? TryGetSessionFromCookie(string? sessionIdCookie)
        {
            if (sessionIdCookie == null) return null;
            var apis = BlueskyEnrichedApis.Instance;
            var now = DateTime.UtcNow;
            var sessionId = TryGetSessionIdFromCookie(sessionIdCookie, out var unverifiedDid);
            if (sessionId != null)
            {
                if (!SessionDictionary.TryGetValue(sessionId, out var session))
                {

                    var temporaryCtx = RequestContext.CreateForRequest(AppViewLiteSession.CreateAnonymous());
                    var unverifiedUserContext = GetOrCreateUserContext(unverifiedDid!, temporaryCtx);

                    var sessionProto = unverifiedUserContext.TryGetAppViewLiteSession(sessionId);
                    if (sessionProto == null) return null;

                    session = new AppViewLiteSession
                    {
                        UserContext = unverifiedUserContext, // now verified
                        IsReadOnlySimulation = sessionProto!.IsReadOnlySimulation,
                        SessionToken = sessionId,
                        LastSeen = now,
                        LoginDate = sessionProto.LogInDate,
                    };
                    temporaryCtx.Session = session;
                    apis.OnSessionCreatedOrRestoredAsync(session.UserContext, temporaryCtx).FireAndForget();
                    SessionDictionary[sessionId] = session;
                }

                session.LastSeen = now;

                return session;
            }

            return null;
        }


        private Dictionary<string, AppViewLiteUserContext> UserContexts = new();

        public AppViewLiteUserContext GetOrCreateUserContext(string did, RequestContext ctx)
        {
            lock (UserContexts)
            {
                if (UserContexts.TryGetValue(did, out var userContext))
                    return userContext;

                // NOTE: at this point, we don't know for sure if did actually belongs to ctx.
                return WithRelationshipsLockForDid(did!, (plc, rels) =>
                {
                    var profileProto = rels.AppViewLiteProfiles.TryGetPreserveOrderSpanLatest(plc, out var appviewProfileBytes) ? BlueskyRelationships.DeserializeProto<AppViewLiteProfileProto>(appviewProfileBytes.AsSmallSpan()) : new();
                    profileProto.Sessions ??= [];
                    profileProto.PrivateFollows ??= [];
                    profileProto.FeedSubscriptions ??= [];
                    profileProto.LabelerSubscriptions ??= [];

                    userContext = new AppViewLiteUserContext
                    {
                        PrivateProfile = profileProto,
                        Profile = rels.GetProfile(plc),
                        PdsSession = profileProto.PdsSessionCbor != null ? BlueskyEnrichedApis.DeserializeAuthSession(profileProto.PdsSessionCbor!).Session : null,
                    };
                    userContext.UpdateRefreshTokenExpireDate();

                    UserContexts.Add(did, userContext);
                    return userContext;
                }, ctx);
            }
        }



        public void LogOut(string cookie, RequestContext ctx)
        {
            var id = BlueskyEnrichedApis.TryGetSessionIdFromCookie(cookie, out var unverifiedDid);
            if (id != null)
            {
                LogOut(id, unverifiedDid!, ctx);
            }

        }

        public void LogOut(string? id, string unverifiedDid, RequestContext ctx)
        {
            var unverifiedUserContext = GetOrCreateUserContext(unverifiedDid, ctx);
            var session = unverifiedUserContext.TryGetAppViewLiteSession(id);
            if (session != null)
            {
                // now verified.
                var userContext = unverifiedUserContext;

                userContext.PdsSession = null;
                foreach (var other in userContext.PrivateProfile!.Sessions)
                {
                    SessionDictionary!.Remove(other.SessionToken, out _);
                }
                userContext.PrivateProfile!.Sessions = [];
                userContext.PrivateProfile.PdsSessionCbor = null;
                userContext.UpdateRefreshTokenExpireDate();
                SaveAppViewLiteProfile(ctx);
            }

        }

        public static bool AllowPublicReadOnlyFakeLogin = AppViewLiteConfiguration.GetBool(AppViewLiteParameter.APPVIEWLITE_ALLOW_PUBLIC_READONLY_FAKE_LOGIN) ?? false;

        public async Task<(AppViewLiteSession Session, string Cookie)> LogInAsync(string handle, string password, RequestContext ctx)
        {
            var apis = BlueskyEnrichedApis.Instance;
            if (string.IsNullOrEmpty(handle) || string.IsNullOrEmpty(password)) throw new ArgumentException("Empty handle or password.");

            var isReadOnly = AllowPublicReadOnlyFakeLogin ? password == "readonly" : false;

            var did = await apis.ResolveHandleAsync(handle, ctx, allowUnendorsed: false);
            var atSession = isReadOnly ? null : await apis.LoginToPdsAsync(did, password, ctx);



            var id = RandomNumberGenerator.GetHexString(32, lowercase: true);

            var userContext = GetOrCreateUserContext(did, ctx);
            userContext.PrivateProfile!.FirstLogin ??= DateTime.UtcNow;
            var plc = userContext.LoggedInUser!.Value;

            var session = WithRelationshipsWriteLock(rels =>
            {

                if (!rels.LastSeenNotifications.ContainsKey(plc))
                {
                    userContext.PrivateProfile.LabelerSubscriptions = relationshipsUnlocked.DefaultLabelSubscriptions.ToArray();
                    rels.RegisterForNotifications(plc);
                }

                if (!isReadOnly)
                {
                    userContext.PrivateProfile.PdsSessionCbor = BlueskyEnrichedApis.SerializeAuthSession(new AuthSession(atSession!));
                    userContext.PdsSession = atSession;
                    userContext.UpdateRefreshTokenExpireDate();
                }
                var now = DateTime.UtcNow;
                var sessionProto = new AppViewLiteSessionProto
                {
                    LastSeen = now,
                    SessionToken = id,
                    IsReadOnlySimulation = isReadOnly,
                    LogInDate = now,
                };
                lock (userContext)
                {
                    userContext.PrivateProfile.Sessions = userContext.PrivateProfile.Sessions.Append(sessionProto).ToArray();
                }
                rels.SaveAppViewLiteProfile(userContext);
                return new AppViewLiteSession
                {
                    LastSeen = sessionProto.LastSeen,
                    IsReadOnlySimulation = sessionProto.IsReadOnlySimulation,
                    SessionToken = sessionProto.SessionToken,
                    UserContext = userContext,
                    LoginDate = sessionProto.LogInDate
                };
            }, ctx);

            ctx.Session = session;
            await OnSessionCreatedOrRestoredAsync(session.UserContext, ctx);

            SessionDictionary[id] = session;

            return (session, id + "=" + did);
        }


        public ConcurrentDictionary<string, AppViewLiteSession> SessionDictionary = new();

        public async Task<string> ResolveUrlToDidAsync(Uri url, Uri? baseUrl, RequestContext ctx)
        {
            var resolved = await TryResolveUrlToDidAsync(url, baseUrl, ctx);
            if (resolved == null)
                throw new Exception("The specified DID or URL could not be resolved.");
            return resolved;
        }
        public async Task<string?> TryResolveUrlToDidAsync(Uri url, Uri? baseUrl, RequestContext ctx)
        {
            // bsky.app links
            if (url.Host == "bsky.app")
            {
                var segments = url.GetSegments();
                if (segments.Length >= 2 && segments[0] == "profile")
                    return await ResolveHandleAsync(segments[1], ctx);
                return null;
            }
            // clearsky links
            if (url.Host == "clearsky.app")
            {
                var segments = url.GetSegments();
                if (segments.Length != 0)
                    return await ResolveHandleAsync(segments[0], ctx);
                return null;
            }


            // recursive appviewlite links
            if (url.Host == baseUrl?.Host)
            {
                var segment = url.GetSegments()?.FirstOrDefault();
                if (segment != null && segment.StartsWith('@'))
                    return await ResolveHandleAsync(segment, ctx);
            }

            // atproto profile from custom domain
            if (url.PathAndQuery == "/")
            {
                var handle = url.Host;
                try
                {
                    var did = await ResolveHandleAsync(handle, ctx);
                    return did;
                }
                catch
                {
                }
            }

            foreach (var protocol in PluggableProtocol.RegisteredPluggableProtocols)
            {
                var did = await protocol.TryGetDidOrLocalPathFromUrlAsync(url);
                if (did != null)
                    return did.StartsWith("did:", StringComparison.Ordinal) ? did : null;
            }

            return null;
        }


        private readonly static TimeSpan FeedCreditRefreshInterval = TimeSpan.FromMinutes(30);
        private readonly static double FeedCreditDecayPerHour = 0.9;


        public void UpdateFollowingFeedMustHoldLock(RequestContext ctx)
        {
            // Must hold userCtx lock

            var userCtx = ctx.UserContext;
            var now = DateTime.UtcNow;
            if (userCtx.FeedCredits == null)
            {
                userCtx.FeedCredits = new();
                var accountedPosts = new HashSet<PostId>();
                WithRelationshipsLock(rels =>
                {
                    // Replay recent history
                    foreach (var seenPost in rels.SeenPostsByDate.GetValuesSorted(ctx.LoggedInUser, new TimePostSeen(now.AddDays(-2), default)))
                    {
                        if (accountedPosts.Contains(seenPost.PostId)) continue;
                        // let's avoid a binary lookup for every single post, pretend that everything comes from following feed

                        if (ctx.UserContext.LastFeedCreditsTimeDecayAdjustment == default)
                            ctx.UserContext.LastFeedCreditsTimeDecayAdjustment = seenPost.Date;

                        ConsumeFollowingFeedCreditsMustHoldLockCore(ctx, seenPost.PostId.Author);
                        MaybeDecayFollowingFeedCreditsMustHoldCtxLock(ctx.UserContext, seenPost.Date);
                        accountedPosts.Add(seenPost.PostId);
                    }
                }, ctx);

                if (userCtx.LastFeedCreditsTimeDecayAdjustment == default)
                    userCtx.LastFeedCreditsTimeDecayAdjustment = now;
            }



            MaybeDecayFollowingFeedCreditsMustHoldCtxLock(ctx.UserContext, now);





        }

        private static void MaybeDecayFollowingFeedCreditsMustHoldCtxLock(AppViewLiteUserContext userCtx, DateTime now)
        {
            var elapsed = now - userCtx.LastFeedCreditsTimeDecayAdjustment;

            var elapsedRefreshIntervals = (double)elapsed.Ticks / FeedCreditRefreshInterval.Ticks;
            if (elapsedRefreshIntervals < 1) return;

            userCtx.LastFeedCreditsTimeDecayAdjustment = now;

            var multiplier = Math.Pow(FeedCreditDecayPerHour, elapsed.TotalHours);
            var creditsDict = userCtx.FeedCredits!;
            foreach (var entry in creditsDict.ToArray())
            {
                var updatedCredits = (float)(entry.Value * multiplier);
                if (Math.Abs(updatedCredits) < 0.01)
                    creditsDict.Remove(entry.Key);
                else
                    creditsDict[entry.Key] = updatedCredits;
            }
            userCtx.LastFeedCreditsTimeDecayAdjustment = now;
        }

        public void ConsumeFollowingFeedCreditsMustHoldLock(RequestContext ctx, Plc plc, bool addCredits = false)
        {
            // Must hold userCtx lock

            UpdateFollowingFeedMustHoldLock(ctx);

            ConsumeFollowingFeedCreditsMustHoldLockCore(ctx, plc, addCredits: addCredits);
        }
        private static void ConsumeFollowingFeedCreditsMustHoldLockCore(RequestContext ctx, Plc plc, bool addCredits = false)
        {
            BlueskyRelationships.Assert(plc != default);
            // Must hold userCtx lock
            var amount = addCredits ? 1 : -1;
            CollectionsMarshal.GetValueRefOrAddDefault(ctx.UserContext.FeedCredits!, plc, out _) += amount;
        }

        public async Task<string> ResolveUrlToDidOrRedirectAsync(Uri url, Uri? baseUrl, RequestContext ctx)
        {
            // bsky.app links
            if (url.Host == "bsky.app")
                return url.PathAndQuery;

            // clearsky links
            if (url.Host == "clearsky.app" && url.GetSegments().FirstOrDefault() is { } clearskyHandle)
                return clearskyHandle;

            // recursive appviewlite links
            if (url.Host == baseUrl?.Host)
                return url.PathAndQuery;

            // atproto profile from custom domain
            if (url.PathAndQuery == "/")
            {
                var handle = url.Host;
                try
                {
                    var did = await ResolveHandleAsync(handle, ctx);
                    return "/@" + handle;
                }
                catch
                {
                }
            }

            if (url.AbsolutePath.StartsWith("/xrpc/app.bsky.feed.getFeedSkeleton", StringComparison.Ordinal))
            {
                return "/feed?endpoint=" + Uri.EscapeDataString(url.AbsoluteUri);
            }

            foreach (var protocol in PluggableProtocol.RegisteredPluggableProtocols)
            {
                var did = await protocol.TryGetDidOrLocalPathFromUrlAsync(url);
                if (did != null)
                    return did.StartsWith("did:", StringComparison.Ordinal) ? "/@" + did : did;
            }


            throw new UnexpectedFirehoseDataException("No RSS feeds were found at the specified page.");
        }

        public async Task<PostsAndContinuation> GetRecentlyViewedPosts(string? continuation, RequestContext ctx, int limit = 0)
        {
            EnsureLimit(ref limit, 30);
            var continuationParsed = continuation != null ? TimePostSeen.Deserialize(continuation) : (TimePostSeen?)null;
            var shouldTake = new HashSet<PostId>();
            var postsAndDates = WithRelationshipsLock(rels => rels.SeenPostsByDate.GetValuesSortedDescending(ctx.LoggedInUser, null, continuationParsed).Select(x => (Engagement: x, ShouldTake: shouldTake.Add(x.PostId))).TakeWhile(x => shouldTake.Count <= limit).Select(x => (x.Engagement, Post: x.ShouldTake ? rels.GetPost(x.Engagement.PostId) : null)).ToArray(), ctx);

            var posts = postsAndDates.Select(x => x.Post).WhereNonNull().ToArray();
            await EnrichAsync(posts, ctx);
            return new PostsAndContinuation(posts, postsAndDates.Length != 0 ? postsAndDates[^1].Engagement.Serialize() : null);
        }

        public void ToggleDomainMute(string domain, bool mute, RequestContext ctx)
        {
            lock (ctx.UserContext)
            {
                var profile = ctx.UserContext.PrivateProfile!;
                if (mute)
                {
                    if (profile.MuteRules.Any(x => x.AppliesToPlc == null && x.Word == domain)) return;
                    profile.MuteRules = profile.MuteRules.Append(new MuteRule { Word = domain, Id = ++profile.LastAssignedMuteRuleId }).ToArray();
                }
                else
                {
                    profile.MuteRules = profile.MuteRules.Where(x => !(x.AppliesToPlc == null && x.Word == domain)).ToArray();
                }
            }
            SaveAppViewLiteProfile(ctx);
        }

        public readonly static long TimeProcessStarted = Stopwatch.GetTimestamp();
        public object GetCountersThreadSafe(RequestContext ctx, bool cursors = true)
        {
            var gcMemoryInfo = GC.GetGCMemoryInfo();
            using var proc = Process.GetCurrentProcess();
            return new
            {
                Uptime = Stopwatch.GetElapsedTime(TimeProcessStarted),
                Pid = Environment.ProcessId,
                RepositoryVersion = AppViewLiteInit.GitCommitVersion,
                CarImportCount = this.CarImportDict.Count,
                FetchAndStoreDidDocNoOverrideDict = this.FetchAndStoreDidDocNoOverrideDict.Count,
                FetchAndStoreLabelerServiceMetadataDict = this.FetchAndStoreLabelerServiceMetadataDict.Count,
                FetchAndStoreListMetadataDict = this.FetchAndStoreListMetadataDict.Count,
                FetchAndStorePostDict = this.FetchAndStorePostDict.Count,
                FetchAndStoreOpenGraphDict = this.FetchAndStoreOpenGraphDict.Count,
                FetchAndStoreProfileDict = this.FetchAndStoreProfileDict.Count,
                recentSearches = this.recentSearches.Count,
                RunHandleVerificationDict = this.RunHandleVerificationDict.Count,
                SecondaryFirehoses = this.SecondaryFirehoses.Count,
                SessionDictionary = this.SessionDictionary.Count,
                UserContexts = this.UserContexts.Count,
                CustomEmojiCache = this.CustomEmojiCache.Count,
                CarImporter_GlobalDecodedBytes = CarImporter.GlobalDecodedBytes,
                CarDownloadSemaphore = CarDownloadSemaphore.CurrentCount,
                Primary = this.relationshipsUnlocked.GetCountersThreadSafe(),
                Secondary = this.readOnlyReplicaRelationshipsUnlocked?.GetCountersThreadSafe(),
                UserContext = ctx.IsLoggedIn ? ctx.UserContext.GetCountersThreadSafe() : null,
                DedicatedThreadPoolScheduler = Indexer.FirehoseThreadpool!.GetCountersThreadSafe(),
                DirectIoReadStatsTotalKeys = CombinedPersistentMultiDictionary.DirectIoReadStats.Where(x => x.Key.Contains("col0")).Sum(x => x.Value),
                DirectIoReadStatsTotalOffsets = CombinedPersistentMultiDictionary.DirectIoReadStats.Where(x => x.Key.Contains("col2")).Sum(x => x.Value),
                DirectIoReadStatsTotalValues = CombinedPersistentMultiDictionary.DirectIoReadStats.Where(x => x.Key.Contains("col1")).Sum(x => x.Value),
                DirectIoReadStats = new OrderedDictionary<string, long>(CombinedPersistentMultiDictionary.DirectIoReadStats.OrderByDescending(x => x.Value).Select(x => new KeyValuePair<string, long>(x.Key.Replace(".dat", null).Replace("col0", "K").Replace("col1", "V").Replace("col2", "O"), x.Value))),
                AliveBlueskyRelationshipsDatabases = BlueskyRelationships.AliveBlueskyRelationshipsDatabases,
                AlignedArenaPool = GetAllPooledObjects(AlignedArenaPool).Select(x => x.TotalAllocatedSize).ToArray(),
                UnalignedArenaPool = GetAllPooledObjects(UnalignedArenaPool).Select(x => x.TotalAllocatedSize).ToArray(),
                EfficientTextCompressor_TokenizerPool = GetAllPooledObjects(EfficientTextCompressor.TokenizerPool).Count,
                GCMemoryInfo = new  
                {
                    gcMemoryInfo.FinalizationPendingCount,
                    gcMemoryInfo.FragmentedBytes,
                    gcMemoryInfo.HeapSizeBytes,
                    gcMemoryInfo.HighMemoryLoadThresholdBytes,
                    gcMemoryInfo.Generation,
                    gcMemoryInfo.Index,
                    gcMemoryInfo.MemoryLoadBytes,
                    gcMemoryInfo.PauseTimePercentage,
                    gcMemoryInfo.PinnedObjectsCount,
                    gcMemoryInfo.PromotedBytes,
                    gcMemoryInfo.TotalAvailableMemoryBytes,
                    gcMemoryInfo.TotalCommittedBytes,
                },
                ProcessInfo = new 
                {
                    proc.HandleCount,
                    //MaxWorkingSet = (long)proc.MaxWorkingSet,
                    //MinWorkingSet = (long)proc.MinWorkingSet,
                    Memory = new 
                    {
                        Private = proc.PrivateMemorySize64,
                        Virtual = proc.VirtualMemorySize64,
                        WorkingSet = proc.WorkingSet64,

                        NonPagedSystem = proc.NonpagedSystemMemorySize64,
                        Paged = proc.PagedMemorySize64,
                        PagedSystem = proc.PagedSystemMemorySize64,

                        PeakPaged = proc.PeakPagedMemorySize64,
                        PeakVirtual = proc.PeakVirtualMemorySize64,
                        PeakWorkingSet = proc.PeakWorkingSet64,
                    },

                    proc.TotalProcessorTime,
                    proc.UserProcessorTime,
                    proc.PrivilegedProcessorTime,
                },
                PrimaryOnlyCounters = this.relationshipsUnlocked.GetCountersThreadSafePrimaryOnly(cursors),
            };
        }

        internal static IReadOnlyList<T> GetAllPooledObjects<T>(ObjectPool<T> pool) where T: class
        {
            var items = (ConcurrentQueue<T>)pool.GetType().GetField("_items", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!.GetValue(pool)!;
            var fastItem = (T?)pool.GetType().GetField("_fastItem", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!.GetValue(pool);
            var allItems = items.ToList();
            if (fastItem != null)
                allItems.Add(fastItem);
            return allItems;
        }

        public readonly SemaphoreSlim CarDownloadSemaphore = new SemaphoreSlim(AppViewLiteConfiguration.GetInt32(AppViewLiteParameter.APPVIEWLITE_CAR_DOWNLOAD_SEMAPHORE) ?? 8);


        public string? GetExternalThumbnailUrl(BlueskyPost post)
        {
            if (post.Data?.ExternalThumbCid is { } cid)
            {
                return GetImageThumbnailUrl(post.Did, cid, post.Author.Pds);
            }
            else if (post.LateOpenGraphData?.ExternalThumbnailUrl is { } url)
            {
                var u = new Uri(url);
                return GetImageThumbnailUrl("host:" + u.Host, Encoding.UTF8.GetBytes(u.PathAndQuery), null);
            }

            return null;
        }

        public async Task<(RssRefreshInfo[] Page, string? NextContinuation)> GetRssRefreshInfosAsync(string? continuation, RequestContext ctx, int limit = default, string? onlyDid = null)
        {
            EnsureLimit(ref limit);
            var onlyPlc = onlyDid != null ? SerializeSingleDid(onlyDid, ctx) : default;
            var page = WithRelationshipsLock(rels =>
            {
                return (onlyDid != null ? [onlyPlc] : rels.RssRefreshInfos.EnumerateKeysSortedDescending(StringUtils.DeserializeFromString<Plc>(continuation)))
                    .Take(limit + 1)
                    .Select(x =>
                    {
                        var rss = rels.GetRssRefreshInfo(x)!;
                        rss.BlueskyProfile = rels.GetProfile(x);
                        return rss;
                    })
                    .ToArray();
            }, ctx);
            await EnrichAsync(page.Select(x => x.BlueskyProfile!).ToArray(), ctx);
            return (page.Take(limit).ToArray(), page.Length <= limit ? null : StringUtils.SerializeToString(page[^1].BlueskyProfile!.Plc));
        }


        public Versioned<AccountState> SetAccountState(string did, bool active, string? status, RequestContext ctx)
        {
            var newState =
                active ? AccountState.Active :
                (
                    status switch
                    {
                        "takendown" => AccountState.TakenDown,
                        "suspended" => AccountState.Suspended,
                        "deleted" => AccountState.Deleted,
                        "deactivated" => AccountState.Deactivated,
                        "desynchronized" => AccountState.Desynchronized,
                        "throttled" => AccountState.Throttled,
                        _ => AccountState.NotActive,
                    }

                );

            if (newState == AccountState.Active)
            {
                var prevActive = WithRelationshipsLockForDid(did, (plc, rels) => rels.AsVersioned(rels.IsAccountActive(plc)), ctx);
                if (prevActive.Value)
                    return new(newState, prevActive.MinVersion);
            }

            return WithRelationshipsWriteLock(rels =>
            {
                var plc = rels.SerializeDid(did, ctx);
                var prevState = rels.GetAccountState(plc);
                if (prevState == AccountState.Unknown)
                    prevState = AccountState.Active;

                if (newState != prevState)
                {
                    rels.AccountStates.Add(plc, (byte)newState);
                }
                return rels.AsVersioned(newState);
            }, ctx);
        }




        private readonly int GlobalPeriodicFlushSeconds = AppViewLiteConfiguration.GetInt32(AppViewLiteParameter.APPVIEWLITE_GLOBAL_PERIODIC_FLUSH_SECONDS) ?? 600;
        public async Task RunGlobalPeriodicFlushLoopAsync()
        {
            var ct = ShutdownRequested;
            while (true)
            {
                await Task.Delay(TimeSpan.FromSeconds(GlobalPeriodicFlushSeconds), ct);
                GlobalFlush("GlobalPeriodicFlush");
            }
        }

        private Lock globalFlushWithFirehoseCursorCaptureLock = new();

        public void NotifyShutdownRequested()
        {

            if (relationshipsUnlocked.ShutdownRequested.IsCancellationRequested) return;
            relationshipsUnlocked.ShutdownRequestedCts.Cancel();
            DrainAndCaptureFirehoseCursors();
            primarySecondaryPair.Dispose();
            FlushLog();
        }

        public void GlobalFlush(string reason)
        {
            DrainAndCaptureFirehoseCursors();

            while (true)
            {
                var ctx = RequestContext.CreateForFirehose(reason);
                var retryLater = WithRelationshipsWriteLock(rels =>
                {
                    var f = rels.AllMultidictionaries.Select(x => (Table: x, Compactation: x.HasPendingCompactationNotReadyForCommitYet)).FirstOrDefault(x => x.Compactation != null);
                    if (f.Compactation != null)
                    {
                        Log($"Global flush requested, but one of the tables ({f.Table.Name}) is performing a compactation. Postponing the flush in order to avoid a sync wait while holding the primary lock.");
                        return f.Compactation;
                    }
                    Log("Global periodic flush...");
                    LogInfo("====== START OF GLOBAL PERIODIC FLUSH ======");
                    rels.GlobalFlushWithoutFirehoseCursorCapture();
                    LogInfo("====== END OF GLOBAL PERIODIC FLUSH ======");
                    return null;
                }, ctx);

                if (retryLater == null) break;

                retryLater.GetAwaiter().GetResult();
            }
        }

        internal void DrainAndCaptureFirehoseCursors()
        {
            lock (globalFlushWithFirehoseCursorCaptureLock)
            {
                if (AppViewLiteConfiguration.GetBool(AppViewLiteParameter.APPVIEWLITE_DRAIN_FIREHOSE_BEFORE_CURSOR_CAPTURE, false))
                {
                    Log("Draining firehose threadpool...");
                    var resume = Indexer.FirehoseThreadpool!.PauseAndDrain();
                    Indexer.CaptureFirehoseCursors?.Invoke();
                    resume();
                }
                else
                {
                    Indexer.CaptureFirehoseCursors?.Invoke();
                }
                
            }
        }

        public void LaunchLabelerListener(string[] allowedLabelerDids, string endpoint)
        {
            if (!Uri.TryCreate(endpoint, UriKind.Absolute, out var endpointUrl) || endpointUrl.Scheme != Uri.UriSchemeHttps)
            {
                Log("Invalid labeler endpoint: " + endpoint);
                return;
            }
            var indexer = new Indexer(this)
            {
                FirehoseUrl = endpointUrl,
                VerifyValidForCurrentRelay = labelerDidFromFirehose =>
                {
                    if (!allowedLabelerDids.Contains(labelerDidFromFirehose))
                        throw new UnexpectedFirehoseDataException($"Labeler firehose {endpoint} ({string.Join(", ", allowedLabelerDids)}) attempted to provide a label on behalf of labeler {labelerDidFromFirehose}.");
                }
            };
            indexer.StartListeningToAtProtoFirehoseLabels(endpoint).FireAndForget();
        }

        public async Task<BlueskyLabel[]> GetAllPostLabelsAsync(string did, string rkey, RequestContext ctx)
        {
            var labels = WithRelationshipsLockForDid(did, (plc, rels) => rels.GetPostLabels(new PostId(plc, Tid.Parse(rkey))).Select(x => rels.GetLabel(x, ctx)).ToArray(), ctx);
            ctx.IncreaseTimeout(TimeSpan.FromSeconds(3));
            await EnrichAsync(labels, ctx);
            return labels;
        }
        public async Task<BlueskyLabel[]> GetAllProfileLabelsAsync(string did, RequestContext ctx)
        {
            var labels = WithRelationshipsLockForDid(did, (plc, rels) => rels.GetProfileLabels(plc).Select(x => rels.GetLabel(x, ctx)).ToArray(), ctx);
            ctx.IncreaseTimeout(TimeSpan.FromSeconds(3));
            await EnrichAsync(labels, ctx);
            return labels;
        }

        public async Task<BlueskyList> GetListMetadataAsync(string did, string rkey, RequestContext ctx)
        {
            var list = WithRelationshipsLockForDid(did, (plc, rels) => rels.GetList(new Relationship(plc, Tid.Parse(rkey)), ctx), ctx);
            await EnrichAsync([list], ctx);
            return list;
        }

        public void MarkLastSeenNotification(Notification notification, RequestContext ctx)
        {
            var dark = BlueskyRelationships.IsDarkNotification(notification.Kind);
            WithRelationshipsWriteLock(rels => rels.GetLastSeenNotificationTable(dark).Add(ctx.LoggedInUser, notification), ctx);
            if (!dark)
                DangerousUnlockedRelationships.UserNotificationSubscribersThreadSafe.MaybeNotifyOutsideLock(ctx.LoggedInUser, handler => handler(0));
        }
    }

    internal record struct BalancedFeedCandidatesForFollowee(Plc Plc, (Tid PostRKey, int LikeCount)[] Posts, (PostId PostId, Tid RepostRKey, bool IsReposteeFollowed, long LikeCount)[] Reposts);
}



