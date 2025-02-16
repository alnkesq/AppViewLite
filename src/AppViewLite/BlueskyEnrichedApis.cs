using FishyFlip.Models;
using FishyFlip;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using FishyFlip.Lexicon;
using FishyFlip.Lexicon.App.Bsky.Feed;
using FishyFlip.Lexicon.App.Bsky.Actor;
using FishyFlip.Lexicon.Com.Atproto.Repo;
using System.Net.Http;
using Newtonsoft.Json;
using AppViewLite.Storage;
using System.Collections.Concurrent;
using System.Text.RegularExpressions;
using AppViewLite.Models;
using AppViewLite.Numerics;
using System.Runtime.InteropServices;
using FishyFlip.Tools;
using FishyFlip.Lexicon.App.Bsky.Graph;
using DuckDbSharp.Types;
using System.Diagnostics;
using System.Globalization;
using PeterO.Cbor;
using FishyFlip.Lexicon.App.Bsky.Embed;
using DnsClient;
using System.Text;
using System.Net.Http.Json;
using AppViewLite;
using System.Diagnostics.CodeAnalysis;
using System.Buffers;
using Ipfs;
using AppViewLite;
using DnsClient.Protocol;
using System.IO;

namespace AppViewLite
{
    public class BlueskyEnrichedApis : BlueskyRelationshipsClientBase
    {
        public static BlueskyEnrichedApis Instance;
        public bool IsReadOnly => relationshipsUnlocked.IsReadOnly;

        public BlueskyRelationships DangerousUnlockedRelationships => relationshipsUnlocked;

        public BlueskyEnrichedApis(BlueskyRelationships relationships)
            : base(relationships)
        {
            PendingHandleVerifications = new(handle => ResolveHandleAsync(handle));
            FetchAndStoreDidDoc = new(pair => FetchAndStoreDidDocNoOverrideAsync(pair.Did, pair.Plc));
            FetchAndStoreLabelerServiceMetadata = new(did => FetchAndStoreLabelerServiceMetadataAsync(did));


            DidDocOverrides = new ReloadableFile<DidDocOverridesConfiguration>(AppViewLiteConfiguration.GetString(AppViewLiteParameter.APPVIEWLITE_DID_DOC_OVERRIDES), path =>
            {
                var config = DidDocOverridesConfiguration.ReadFromFile(path);

                var pdsesToDids = config.CustomDidDocs.GroupBy(x => x.Value.Pds).ToDictionary(x => x.Key, x => x.Select(x => x.Key).ToArray());

                lock (SecondaryFirehoses)
                {

                    var stopListening = SecondaryFirehoses.Where(x => !pdsesToDids.ContainsKey(x.Key));
                    foreach (var oldpds in stopListening)
                    {
                        Console.Error.WriteLine("Stopping secondary firehose: " + oldpds.Key);
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
                                    throw new Exception($"Ignoring record for {did} from relay {newpds} because it's not one of the allowlisted DIDs for that PDS.");
                            };
                            indexer.FirehoseUrl = new Uri(newpds);
                            Console.Error.WriteLine("Starting secondary firehose: " + newpds);
                            indexer.StartListeningToAtProtoFirehoseRepos(cts.Token).FireAndForget();
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
            

            relationships.NotificationGenerated += Relationships_NotificationGenerated;
        }

        private void Relationships_NotificationGenerated(Plc destination, Notification notification)
        {
            // Here we're inside the lock.
            var rels = relationshipsUnlocked;


            var actor = notification.Actor;

            // Notifications don't support DOM dynamic refresh (it would be bad UX anyways). Prefetch post and profile data now.
            if (actor != default && !rels.Profiles.ContainsKey(actor) && !rels.FailedProfileLookups.ContainsKey(actor))
            {
                var profile = rels.GetProfile(actor);
                DispatchOutsideTheLock(() => EnrichAsync([profile], RequestContext.Create()).FireAndForget());
            }

            // These notifications can reference posts from the past that we don't have.
            if (notification.Kind is NotificationKind.LikedYourPost or NotificationKind.RepostedYourPost)
            {
                var postId = new PostId(destination, notification.RKey);
                if (!rels.PostData.ContainsKey(postId) && !rels.FailedPostLookups.ContainsKey(postId)) 
                {
                    var post = rels.GetPost(postId);
                    DispatchOutsideTheLock(() => EnrichAsync([post], RequestContext.Create()).FireAndForget());
                }
            }
        }

        private Dictionary<string, CancellationTokenSource> SecondaryFirehoses = new();


        public ReloadableFile<DidDocOverridesConfiguration> DidDocOverrides;




        private ConcurrentDictionary<RelationshipStr, (Task Task, DateTime DateStarted)> pendingProfileRetrievals = new();
        private ConcurrentDictionary<RelationshipStr, (Task Task, DateTime DateStarted)> pendingPostRetrievals = new();
        private ConcurrentDictionary<RelationshipStr, (Task Task, DateTime DateStarted)> pendingListRetrievals = new();
        public TaskDictionary<string, string> PendingHandleVerifications;
        public TaskDictionary<(Plc Plc, string Did), DidDocProto> FetchAndStoreDidDoc;
        public TaskDictionary<string> FetchAndStoreLabelerServiceMetadata;

  

        private async Task FetchAndStoreLabelerServiceMetadataAsync(string did)
        {
            var record = (FishyFlip.Lexicon.App.Bsky.Labeler.Service)(await GetRecordAsync(did, FishyFlip.Lexicon.App.Bsky.Labeler.Service.RecordType, "self")).Value;

            var defs = (record.Policies?.LabelValueDefinitions ?? [])?.ToDictionary(x => x.Identifier);

            WithRelationshipsWriteLock(rels =>
            {
                var plc = rels.SerializeDid(did);
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
                    rels.LabelData.AddRange(new LabelId(plc, BlueskyRelationships.HashLabelName(policy)), BlueskyRelationships.SerializeProto(labelInfo, x => x.Dummy = true));
                }
            });
        }

        public async Task<BlueskyProfile[]> EnrichAsync(BlueskyProfile[] profiles, RequestContext ctx, Action<BlueskyProfile>? onProfileDataAvailable = null, CancellationToken ct = default)
        {
            WithRelationshipsLock(rels =>
            {
                foreach (var profile in profiles)
                {
                    if (ctx.IsLoggedIn && rels.Follows.HasActor(profile.Plc, ctx.LoggedInUser, out var followRel))
                        profile.IsFollowedBySelf = followRel.RelationshipRKey;
                    profile.IsYou = profile.Plc == ctx.Session?.LoggedInUser;
                    profile.BlockReason = rels.GetBlockReason(profile.Plc, ctx);
                    profile.FollowsYou = ctx.IsLoggedIn && rels.Follows.HasActor(ctx.LoggedInUser, profile.Plc, out _);
                    profile.Labels = rels.GetProfileLabels(profile.Plc, ctx.Session?.NeedLabels).Select(x => rels.GetLabel(x)).ToArray();
                }
            });

            foreach (var profile in profiles)
            {
                if (profile.HandleIsUncertain)
                {
                    VerifyHandleAndNotifyAsync(profile.Did, profile.PossibleHandle, ctx).FireAndForget();
                }
            }

            await EnrichAsync(profiles.SelectMany(x => x.Labels ?? []).ToArray(), ctx);

            if (onProfileDataAvailable != null)
            {
                foreach (var profile in profiles)
                {
                    if (profile.BasicData != null)
                        onProfileDataAvailable(profile);
                }
            }
            
            var toLookup = profiles.Where(x => x.BasicData == null).Select(x => new RelationshipStr(x.Did, "self")).Distinct().ToArray();
            if (toLookup.Length == 0) return profiles;

            toLookup = WithRelationshipsUpgradableLock(rels => toLookup.Where(x => !rels.FailedProfileLookups.ContainsKey(rels.SerializeDid(x.Did))).ToArray());
            if (toLookup.Length == 0) return profiles;

            var plcToProfile = profiles.ToLookup(x => x.Plc);

            void OnRecordReceived(BlueskyRelationships rels, Plc plc)
            {
                foreach (var profile in plcToProfile[plc])
                {
                    if (profile.BasicData == null)
                        profile.BasicData = rels.GetProfileBasicInfo(profile.Plc);

                    if (onProfileDataAvailable != null)
                        DispatchOutsideTheLock(() => onProfileDataAvailable.Invoke(profile));
                    
                }
            }

            var task = LookupManyRecordsWithinShortDeadlineAsync<Profile>(toLookup, pendingProfileRetrievals, Profile.RecordType, ct,
                (key, profileRecord) =>
                {
                    WithRelationshipsWriteLock(rels =>
                    {
                        var plc = rels.SerializeDid(key.Did);
                        rels.StoreProfileBasicInfo(plc, profileRecord);
                        OnRecordReceived(rels, plc);
                    });
                },
                key =>
                {
                    WithRelationshipsWriteLock(rels =>
                    {
                        var plc = rels.SerializeDid(key.Did);
                        rels.FailedProfileLookups.Add(plc, DateTime.UtcNow);
                        OnRecordReceived(rels, plc);
                    });
                }, 
                key => 
                {
                    WithRelationshipsLock(rels => OnRecordReceived(rels, rels.SerializeDid(key.Did)));
                }
                , ctx);

            await task;

            return profiles;
        }

        public async Task VerifyHandleAndNotifyAsync(string did, string? handle, RequestContext ctx)
        {
            if (handle == null)
            {
                // happens when the PLC directory is not synced.
                // Note that handle-based badges won't be live-updated.
                var diddoc = await GetDidDocAsync(did);
                handle = diddoc.Handle;
                if (handle == null) return;
            }
            
            PendingHandleVerifications.StartAsync(handle, task =>
            {
                var k = task.IsCompletedSuccessfully && task.Result == did ? handle : null;
                ctx.SendSignalrAsync("HandleVerificationResult", did, BlueskyRelationships.MaybeBridyHandleToFediHandle(k));
            });
        }

        public async Task<ProfilesAndContinuation> GetFollowingAsync(string did, string? continuation, int limit, RequestContext ctx)
        {
            EnsureLimit(ref limit, 50);
            var response = await ListRecordsAsync(did, Follow.RecordType, limit: limit + 1, cursor: continuation);
            var following = WithRelationshipsUpgradableLock(rels =>
            {
                return response!.Records!.Select(x => rels.GetProfile(rels.SerializeDid(((FishyFlip.Lexicon.App.Bsky.Graph.Follow)x.Value!).Subject!.Handler))).ToArray();
            });
            await EnrichAsync(following, ctx);
            return (following, response.Records.Count > limit ? response!.Cursor : null);
        }
        public async Task<ProfilesAndContinuation> GetFollowersYouFollowAsync(string did, string? continuation, int limit, RequestContext ctx)
        {
            EnsureLimit(ref limit, 50);
            var r = WithRelationshipsLock(rels => rels.GetFollowersYouFollow(did, continuation, limit, ctx));
            await EnrichAsync(r.Profiles, ctx);
            return r;
        }

        public async Task<BlueskyPost[]> EnrichAsync(BlueskyPost[] posts, RequestContext ctx, Action<BlueskyPost>? onPostDataAvailable = null, bool loadQuotes = true, bool sideWithQuotee = false, Plc? focalPostAuthor = null, CancellationToken ct = default)
        {
            WithRelationshipsLock(rels =>
            {
                foreach (var post in posts)
                {

                    if (ctx.IsLoggedIn)
                    {
                        var loggedInUser = ctx.LoggedInUser;
                        if (rels.Likes.HasActor(post.PostId, loggedInUser, out var likeTid))
                            post.IsLikedBySelf = likeTid.RelationshipRKey;
                        if (rels.Reposts.HasActor(post.PostId, loggedInUser, out var repostTid))
                            post.IsRepostedBySelf = repostTid.RelationshipRKey;
                    }
                    post.Labels = rels.GetPostLabels(post.PostId, ctx.Session?.NeedLabels).Select(x => rels.GetLabel(x)).ToArray();
                }
            });


            var postIdToPost = posts.ToLookup(x => x.PostId);

            var toLookup = posts.Where(x => x.Data == null).Select(x => new RelationshipStr(x.Author.Did, x.RKey)).ToArray();
            toLookup = WithRelationshipsLock(rels => toLookup.Where(x => !rels.FailedPostLookups.ContainsKey(rels.GetPostId(x.Did, x.RKey))).ToArray());

            WithRelationshipsLock(rels =>
            {
                foreach (var post in posts.Where(x => x.Data != null))
                {
                    OnRecordReceived(rels, post.PostId);
                }
            });

     

            void OnRecordReceived(BlueskyRelationships rels, PostId postId)
            {
                foreach (var post in postIdToPost[postId])
                {
                    if (post.Data == null)
                    {
                        (post.Data, post.InReplyToUser) = rels.TryGetPostDataAndInReplyTo(rels.GetPostId(post.Author.Did, post.RKey));
                    }

                    if (loadQuotes && post.Data?.QuotedPlc != null && post.QuotedPost == null)
                    {
                        post.QuotedPost = rels.GetPost(new PostId(new Plc(post.Data.QuotedPlc.Value), new Tid(post.Data.QuotedRKey!.Value)));
                    }

                    if (post.Data?.InReplyToPlc != null && post.InReplyToUser == null)
                    {
                        post.InReplyToUser = rels.GetProfile(new Plc(post.Data.InReplyToPlc.Value));
                    }


                    var author = post.Author.Plc;
                    post.EmbedRecord = relationshipsUnlocked.TryGetAtObject(post.Data?.EmbedRecordUri);

                    if (focalPostAuthor != null)
                    {
                        post.FocalAndAuthorBlockReason = rels.UsersHaveBlockRelationship(focalPostAuthor.Value, postId.Author);
                    }


                    if (post.Data?.InReplyToPostId is { Author: { } inReplyToAuthor })
                    {
                        post.ParentAndAuthorBlockReason = rels.UsersHaveBlockRelationship(inReplyToAuthor, author);

                        if (post.RootPostId.Author != inReplyToAuthor)
                        {
                            post.RootAndAuthorBlockReason = rels.UsersHaveBlockRelationship(post.RootPostId.Author, author);
                        }
                    }



                    post.Threadgate = rels.TryGetThreadgate(post.RootPostId);
                    if (post.Threadgate != null)
                    {
                        if (post.Threadgate.HiddenReplies?.Any(x => x.PostId == post.PostId) == true)
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


                    if (onPostDataAvailable != null)
                    {
                        // The user callback must run outside the lock.
                        DispatchOutsideTheLock(() => onPostDataAvailable.Invoke(post));
                    }
                    

                }
            }


            var task = LookupManyRecordsWithinShortDeadlineAsync<Post>(toLookup, pendingPostRetrievals, Post.RecordType, ct,
                (key, postRecord) =>
                {
                    WithRelationshipsWriteLock(rels =>
                    {
                        rels.SuppressNotificationGeneration++;
                        try
                        {
                            var postId = rels.GetPostId(key.Did, key.RKey);
                            rels.StorePostInfo(postId, postRecord, key.Did);
                            OnRecordReceived(rels, postId);
                        }
                        finally
                        {
                            rels.SuppressNotificationGeneration--;
                        }
                    });
                },
                key =>
                {
                    WithRelationshipsWriteLock(rels =>
                    {
                        var postId = rels.GetPostId(key.Did, key.RKey);
                        rels.FailedPostLookups.Add(rels.GetPostId(key.Did, key.RKey), DateTime.UtcNow);
                        OnRecordReceived(rels, postId);
                    });
                },
                key =>
                {
                    WithRelationshipsLock(rels => OnRecordReceived(rels, rels.GetPostId(key.Did, key.RKey)));
                },
                ctx);


            await EnrichAsync(posts.SelectMany(x => x.Labels ?? []).ToArray(), ctx);
            await EnrichAsync(posts.SelectMany(x => new[] { x.Author, x.InReplyToUser, x.RepostedBy }).Where(x => x != null).ToArray(), ctx, ct: ct);
            
            if (loadQuotes)
            {
                var r = posts.Select(x => x.QuotedPost).Where(x => x != null).ToArray();
                if (r.Length != 0)
                {
                    await EnrichAsync(r, ctx, onPostDataAvailable, loadQuotes: false, focalPostAuthor: focalPostAuthor, ct: ct);
                    WithRelationshipsLock(rels =>
                    {
                        foreach (var quoter in posts)
                        {
                            var quoted = quoter.QuotedPost;
                            if (quoted == null) continue;
                            if(sideWithQuotee) quoter.QuoteeAndAuthorBlockReason = rels.UsersHaveBlockRelationship(quoter.PostId.Author, quoted.PostId.Author);
                            else quoted.QuoterAndAuthorBlockReason = rels.UsersHaveBlockRelationship(quoter.PostId.Author, quoted.PostId.Author);
                            var quotedPostgate = rels.TryGetPostgate(quoted.PostId);
                            if (quotedPostgate != null)
                            {
                                if (quotedPostgate.DetachedEmbeddings != null && quotedPostgate.DetachedEmbeddings.Any(x => x.PostId == quoter.PostId))
                                {
                                    if (sideWithQuotee) quoter.PostBlockReason = PostBlockReasonKind.RemovedByQuoteeOnQuoter;
                                    else quoted.PostBlockReason = PostBlockReasonKind.RemovedByQuotee;
                                }
                                else if (quotedPostgate.DisallowQuotes)
                                {
                                    if (sideWithQuotee) quoter.PostBlockReason = PostBlockReasonKind.DisabledQuotesOnQuoter;
                                    else quoted.PostBlockReason = PostBlockReasonKind.DisabledQuotes;
                                }
                            }

                        }
                    });
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



        private Task LookupManyRecordsWithinShortDeadlineAsync<TValue>(IReadOnlyList<RelationshipStr> keys, ConcurrentDictionary<RelationshipStr, (Task Task, DateTime DateStarted)> pendingRetrievals, string collection, CancellationToken ct, Action<RelationshipStr, TValue> onItemSuccess, Action<RelationshipStr> onItemFailure, Action<RelationshipStr> onPreexistingTaskCompleted, RequestContext ctx) where TValue : ATObject
        {
            if (relationshipsUnlocked.IsReadOnly) return Task.CompletedTask;

            if (keys.Count != 0 && (ctx.LongDeadline == null || !ctx.LongDeadline.IsCompleted))
            {
                CleanupOldRetrievals(pendingRetrievals);

                var keysToLookup = new List<RelationshipStr>();
                var allTasksToAwait = new List<Task>();
                foreach (var key in keys)
                {
                    if (!BlueskyRelationships.IsNativeAtProtoDid(key.Did)) continue;
                    if (pendingRetrievals.TryGetValue(key, out var task))
                    {
                        task.Task.GetAwaiter().OnCompleted(() => 
                        {
                            onPreexistingTaskCompleted?.Invoke(key);
                        });
                        allTasksToAwait.Add(task.Task);
                    }
                    else if (keysToLookup.Count + 1 < 100)
                        keysToLookup.Add(key);
                }

                if (keysToLookup.Count != 0)
                {
                    Console.Error.WriteLine("  Firing " + keysToLookup.Count + " lookups for " + collection);
                    allTasksToAwait.AddRange(keysToLookup.Select(key =>
                    {
                        async Task RunAsync()
                        {
                            await Task.Yield();
                            try
                            {
                                try
                                {
                                    var response = await GetRecordAsync(key.Did, collection, key.RKey, ct);
                                    var obj = (TValue)response.Value!;
                                    onItemSuccess(key, obj);
                                }
                                catch (Exception)
                                {
                                    onItemFailure(key);
                                    Console.Error.WriteLine("     > Failure: " + key);
                                }


                            }
                            catch (Exception ex)
                            {
                                // Should we cache network errors? Maybe we should save the error code to the failure dictionary
                                Console.Error.WriteLine("     > Exception: " + key);
                            }


                        }

                        var task = RunAsync();
                        pendingRetrievals[key] = (task, DateTime.UtcNow);
                        return task;
                    }));
                }

                var fullTask = Task.WhenAll(allTasksToAwait);

                fullTask = ctx.LongDeadline != null ? Task.WhenAny(fullTask, ctx.LongDeadline) : fullTask;
                fullTask = ctx.ShortDeadline != null ? Task.WhenAny(fullTask, ctx.ShortDeadline) : fullTask;
                return fullTask;
            }
            return Task.CompletedTask;

        }

        private static void CleanupOldRetrievals(ConcurrentDictionary<RelationshipStr, (Task Task, DateTime DateStarted)> pendingRetrievals)
        {
            if (pendingRetrievals.Count >= 10000)
            {
                var now = DateTime.UtcNow;
                foreach (var item in pendingRetrievals)
                {
                    if ((now - item.Value.DateStarted).TotalMinutes > 1)
                        pendingRetrievals.TryRemove(item.Key, out _);
                }
            }
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

        public async Task<PostsAndContinuation> SearchTopPostsAsync(PostSearchOptions options, int limit = 0, string? continuation = null, RequestContext ctx = default)
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
                    mandatoryPosts = WithRelationshipsLock(rels => page.Posts.Select(x => rels.GetPost(x)).ToArray());
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
                    Console.Error.WriteLine("Try top search with minLikes: " + minLikes);
                    var latest = await SearchLatestPostsAsync(options with { MinLikes = Math.Max(minLikes, options.MinLikes) }, limit: limit * 2, ctx: ctx, enrichOutput: false, alreadyProcessedPosts: searchSession.AlreadyProcessed);
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
            var result = WithRelationshipsLock(rels => resultCore.Select(x => x.Value.Post ?? rels.GetPost(x.Key)).ToArray());


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
                else throw new Exception();
            }
            if (tryAgainAlreadyProcessed)
            {
                return await SearchTopPostsAsync(options, limit, continuation, ctx);
            }


            await EnrichAsync(result, ctx);
            return (result, hasMorePages ? new TopPostSearchCursor(minLikes, cursor.SearchId, cursor.PageIndex + 1).Serialize() : null);
        }

        public async Task<PostsAndContinuation> SearchLatestPostsAsync(PostSearchOptions options, int limit = 0, string? continuation = null, RequestContext ctx = default, ConcurrentDictionary<PostId, CachedSearchResult>? alreadyProcessedPosts = null, bool enrichOutput = true)
        {
            EnsureLimit(ref limit, 30);
            options = await InitializeSearchOptionsAsync(options, ctx);
            var until = options.Until;
            var query = options.Query;
            var author = options.Author != null ? SerializeSingleDid(options.Author) : default;
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
                
                return rels
                    .SearchPosts(coreSearchTerms.ToArray(), options.Since != null ? (ApproximateDateTime32)options.Since : default, until != null ? ((ApproximateDateTime32)until).AddTicks(1) : null, author, options.Language)
                    .DistinctAssumingOrderedInput(skipCheck: true)
                    .SelectMany(approxDate =>
                    {
                        var startPostId = new PostIdTimeFirst(Tid.FromDateTime(approxDate, 0), default);
                        var endPostId = new PostIdTimeFirst(Tid.FromDateTime(approxDate.AddTicks(1), 0), default);

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
                            .Select(x => rels.GetPost(x.Key, BlueskyRelationships.DeserializePostData(x.Values.AsSmallSpan(), x.Key)));

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
                        x.InReplyToUser = x.InReplyToPostId != null ? rels.GetProfile(x.InReplyToPostId.Value.Author) : null;
                        return x;
                    })
                    .DistinctBy(x => x.PostId)
                    .Take(limit + 1)
                    .ToArray();
            });
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

            if (author == "me" && ctx.Session.IsLoggedIn)
                author = ctx.Session?.Did;

            if (author != null && author.StartsWith('@'))
                author = author.Substring(1);

            author = !string.IsNullOrEmpty(author) ? await this.ResolveHandleAsync(author) : null;
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

        private DateTime? ParseDate(string v)
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


        
        public async Task<PostsAndContinuation> GetUserPostsAsync(string did, bool includePosts, bool includeReplies, bool includeReposts, bool includeLikes, bool mediaOnly, int limit, string? continuation, RequestContext ctx)
        {
            EnsureLimit(ref limit);

            var defaultContinuation = new ProfilePostsContinuation(
                includePosts ? Tid.MaxValue : default, 
                includeReposts ? Tid.MaxValue : default,
                includeLikes ? Tid.MaxValue : default,
                []);

            BlueskyPost ? GetPostIfRelevant(BlueskyRelationships rels, PostId postId, CollectionKind kind)
            {
                var post = rels.GetPost(postId);

                if (kind == CollectionKind.Posts)
                {
                    if (!includeReplies && !post.IsRootPost) return null;
                }

                if (mediaOnly && post.Data?.Media == null)
                    return null;
                return post;
            }

            var canFetchFromServer = BlueskyRelationships.IsNativeAtProtoDid(did);

            if (continuation == null && (includePosts || includeReposts))
            {
                var recentThreshold = Tid.FromDateTime(DateTime.UtcNow.AddDays(canFetchFromServer ? -7 : -90), 0);
                var recentPosts = WithRelationshipsLock(rels =>
                {
                    var plc = rels.TrySerializeDidMaybeReadOnly(did);
                    if (plc == default) return [];
                    BlueskyProfile? profile = null;

                    var recentPosts = includePosts ? (
                        mediaOnly ? rels.EnumerateRecentMediaPosts(plc, Tid.FromDateTime(DateTime.UtcNow.AddDays(-90), 0), null).Select(x => (RKey: x.PostRKey, PostId: x, IsRepost: false)) :
                        rels.EnumerateRecentPosts(plc, recentThreshold, null).Select(x => (RKey: x.PostId.PostRKey, x.PostId, IsRepost: false))
                    ) : [];
                    var recentReposts = includeReposts ? rels.EnumerateRecentReposts(plc, recentThreshold, null).Select(x => (RKey: x.RepostRKey, x.PostId, IsRepost: true)) : [];

                    return SimpleJoin.ConcatPresortedEnumerablesKeepOrdered([recentPosts, recentReposts], x => x.RKey, new ReverseComparer<Tid>())
                        .Select(x =>
                        {
                            var post = GetPostIfRelevant(rels, x.PostId, x.IsRepost ? CollectionKind.Reposts : CollectionKind.Posts);
                            if (post != null && x.IsRepost)
                            {
                                post.RepostedBy = profile ??= rels.GetProfile(plc);
                                post.RepostDate = x.RKey.Date;
                            }
                            return post;
                        })
                        .Where(x => x != null)
                        .Take(canFetchFromServer && !mediaOnly ? 10 : 50)
                        .ToArray();
                });

                await EnrichAsync(recentPosts, ctx);
                if (recentPosts.Length != 0)
                {

                    return (recentPosts, canFetchFromServer ? (defaultContinuation with { FastReturnedPosts = recentPosts.Select(x => x.PostIdStr).ToArray() }).Serialize() : null);
                }

          
            }


            ProfilePostsContinuation parsedContinuation = continuation != null ? ProfilePostsContinuation.Deserialize(continuation) : defaultContinuation;
            var fastReturnedPostsSet = parsedContinuation.FastReturnedPosts.ToHashSet();

            MergeablePostEnumerator[] mergeableEnumerators = [

                new MergeablePostEnumerator(parsedContinuation.MaxTidPosts, async max =>
                {
                    var posts = await ListRecordsAsync(did, Post.RecordType, limit, max != Tid.MaxValue ? max.ToString() : null);
                    return posts.Records.Select(x => new PostReference(x.Uri.Rkey, new PostIdString(did, x.Uri.Rkey), (Post)x.Value)).ToArray();
                }, CollectionKind.Posts),
                new MergeablePostEnumerator(parsedContinuation.MaxTidReposts, async max =>
                {
                    var posts = await ListRecordsAsync(did, Repost.RecordType, limit, max != Tid.MaxValue ? max.ToString() : null);
                    return posts.Records.Select(x => new PostReference(x.Uri.Rkey, BlueskyRelationships.GetPostIdStr(((Repost)x.Value).Subject!))).ToArray();
                }, CollectionKind.Reposts),
                new MergeablePostEnumerator(parsedContinuation.MaxTidLikes, async max =>
                {
                    var posts = await ListRecordsAsync(did, Like.RecordType, limit, max != Tid.MaxValue ? max.ToString() : null);
                    return posts.Records.Select(x => new PostReference(x.Uri.Rkey, BlueskyRelationships.GetPostIdStr(((Like)x.Value).Subject!))).ToArray();
                }, CollectionKind.Likes),

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


                var posts = WithRelationshipsWriteLock(rels =>
                {
                    var plc = rels.SerializeDid(did);
                    BlueskyProfile? profile = null;
                    return merged.Select(x =>
                    {
                        var postId = rels.GetPostId(x.PostId.Did, x.PostId.RKey);
                        if (x.PostRecord != null && !rels.PostData.ContainsKey(postId))
                        {
                            rels.StorePostInfo(postId, x.PostRecord, x.PostId.Did);
                        }

                        var post = GetPostIfRelevant(rels, postId, x.Kind);
                        if (post != null)
                        {
                            if (fastReturnedPostsSet.Contains(post.PostIdStr)) return null;
                            if (x.Kind == CollectionKind.Reposts)
                            {
                                post.RepostedBy = (profile ??= rels.GetProfile(plc));
                                post.RepostDate = x.RKey.Date;
                            }
                            else if (x.Kind == CollectionKind.Likes)
                            {
                                post.RepostDate = x.RKey.Date;
                            }
                        }
                        
                        return post;
                    }).Where(x => x != null).ToArray();
                });


                var exceededIterations = iterationCount >= 5;

                if (posts.Length != 0 || mergeableEnumerators.All(x => x.LastReturnedTid == default) || exceededIterations)
                {
                    ProfilePostsContinuation? nextContinuation = mergeableEnumerators.Any(x => x.LastReturnedTid != default) ? new ProfilePostsContinuation(
                            mergeableEnumerators[0].LastReturnedTid,
                            mergeableEnumerators[1].LastReturnedTid,
                            mergeableEnumerators[2].LastReturnedTid,
                            parsedContinuation.FastReturnedPosts
                            ) : null;

                    if (posts.Length == 0 && exceededIterations)
                        nextContinuation = null;

                    await EnrichAsync(posts, ctx);
                    return (posts, nextContinuation?.Serialize());
                }
            }




        }


        public async Task<PostsAndContinuation> GetPostThreadAsync(string did, string rkey, int limit, string? continuation, RequestContext ctx)
        {
            EnsureLimit(ref limit, 100);
            var thread = new List<BlueskyPost>();
            var focalPost = GetSinglePost(did, rkey);
            if (focalPost!.Data == null && !BlueskyRelationships.IsNativeAtProtoDid(did))
                throw new Exception("Post not found.");

            var focalPostId = new PostId(new Plc(focalPost.Author.PlcId), Tid.Parse(focalPost.RKey));

            if (continuation == null)
            {
                thread.Add(focalPost);

                await EnrichAsync([focalPost], ctx);

                var loadedBefore = 0;
                var before = 0;
                while (thread[0].IsReply)
                {
                    var p = thread[0];
                    var prepend = WithRelationshipsLock(rels => rels.GetPost(p.InReplyToPostId!.Value));
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
                            AddOpExhaustiveReplies(child);
                            opReplies.Add(rels.GetPost(child));
                        }
                    }
                    AddOpExhaustiveReplies(focalPostId);
                });
                thread.AddRange(opReplies.OrderBy(x => x.Date));
            }


            var wantMore = Math.Max(1, limit - thread.Count) + 1;

            PostId? parsedContinuation = continuation != null ? PostIdTimeFirst.Deserialize(continuation) : null;
            var otherReplies = WithRelationshipsLock(rels => rels.DirectReplies.GetValuesSorted(focalPostId, parsedContinuation).Where(x => x.Author != focalPostId.Author).Take(wantMore).Select(x => rels.GetPost(x)).ToArray());

            string? nextContinuation = null;
            if (otherReplies.Length == wantMore)
            {
                otherReplies = otherReplies.AsSpan(0, otherReplies.Length - 1).ToArray();
                nextContinuation = otherReplies[^1].PostId.Serialize(); // continuation is exclusive, so UI-last instead of core-last
            }

            thread.AddRange(otherReplies.OrderByDescending(x => x.LikeCount).ThenByDescending(x => x.Date));
            await EnrichAsync(thread.ToArray(), ctx, focalPostAuthor: focalPostId.Author);
            return new(thread.ToArray(), nextContinuation);
        }

        public BlueskyPost GetSinglePost(string did, string rkey)
        {
            return WithRelationshipsLockForDid(did, (plc, rels) => rels.GetPost(plc, Tid.Parse(rkey)));
        }

        public BlueskyProfile GetSingleProfile(string did)
        {
            return WithRelationshipsLockForDid(did, (plc, rels) => rels.GetProfile(plc));
        }

        private Plc SerializeSingleDid(string did)
        {
            return WithRelationshipsLockForDid(did, (plc, rels) => plc);
        }

        //private Dictionary<(string Did, string RKey), (BlueskyFeedGeneratorData Info, DateTime DateCached)> FeedDomainCache = new();

        public async Task<(BlueskyPost[] Posts, BlueskyFeedGenerator Info, string? NextContinuation)> GetFeedAsync(string did, string rkey, string? continuation, RequestContext ctx, bool forGrid = false)
        {
            var feedGenInfo = await GetFeedGeneratorAsync(did, rkey);
            if (!feedGenInfo.Data!.ImplementationDid!.StartsWith("did:web:", StringComparison.Ordinal)) throw new NotSupportedException();
            var domain = feedGenInfo.Data.ImplementationDid.Substring(8);

            var skeletonUrl = $"https://{domain}/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://{did}/app.bsky.feed.generator/{rkey}&limit=30";
            if (continuation != null)
                skeletonUrl += "&cursor=" + Uri.EscapeDataString(continuation);

            AtFeedSkeletonResponse postsJson;
            ATUri[] postsJsonParsed;
            try
            {
                postsJson = JsonConvert.DeserializeObject<AtFeedSkeletonResponse>(await DefaultHttpClient.GetStringAsync(skeletonUrl))!;
                postsJsonParsed = postsJson.feed?.Select(x => new ATUri(x.post)).ToArray() ?? [];
            }
            catch (Exception ex)
            {
                throw CreateExceptionMessageForExternalServerError($"The feed provider", ex);
            }

            var posts = WithRelationshipsLockWithPreamble(
                rels => 
                {
                    var postIds = new List<PostId>();
                    foreach (var item in postsJsonParsed)
                    {
                        if (item.Collection != Post.RecordType) throw new UnexpectedFirehoseDataException("Incorrect collection for feed skeleton entry");
                        var author = rels.TrySerializeDidMaybeReadOnly(item.Did!.Handler);
                        if (author == default) return default;
                        postIds.Add(new PostId(author, Tid.Parse(item.Rkey)));
                    }
                    return PreambleResult.Create(postIds);
                }, 
                (p, rels) =>
                {
                    return p.Select(x => rels.GetPost(x)).ToArray();
                });
            if (continuation == null && posts.Length == 0)
                throw new Exception("The feed provider didn't return any results." + (ctx.IsLoggedIn ? " Note that feeds that require a logged-in user are not currently supported." : null));

            if (forGrid)
                ctx = new RequestContext(ctx.Session, null, TimeSpan.FromSeconds(3), ctx.SignalrConnectionId); // the grid doesn't support automatic refresh
            return (await EnrichAsync(posts, ctx), feedGenInfo, !string.IsNullOrEmpty(postsJson.cursor) ? postsJson.cursor : null);
        }


        private static Exception CreateExceptionMessageForExternalServerError(string subjectDisplayText, Exception ex)
        {
            if (ex is PermissionException) return ex;
            return new UnexpectedFirehoseDataException(GetExceptionMessageForExternalServerError(subjectDisplayText, ex), ex);
        }
        private static string? GetExceptionMessageForExternalServerError(string subjectDisplayText, Exception ex)
        {
            if (ex is ATNetworkErrorException at)
            {
                var code = at.AtError.Detail?.Error;
                if (code == "RecordNotFound")
                    return "This record was not found.";

                var message = at.AtError.Detail?.Message ?? at.AtError.Detail?.Error;
                if (string.IsNullOrEmpty(message)) return subjectDisplayText + " returned error " + at.AtError.StatusCode;

                if (message == "Repo not found" || message.StartsWith("Could not find repo:", StringComparison.Ordinal))
                    return "This user no longer exists at the specified PDS.";

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

        private async Task<BlueskyFeedGeneratorData> GetFeedGeneratorDataAsync(string did, string rkey)
        {
            var (plc, result) = WithRelationshipsLockForDid(did, (plc, rels) =>
            {
                return (plc, rels.TryGetFeedGeneratorData(new(plc, rkey)));
            });

            var now = DateTime.UtcNow;

            if (result == null || (now - result.RetrievalDate).TotalHours > 6)
            { 
                var recordOutput = await GetRecordAsync(did, Generator.RecordType, rkey);
                var generator = (Generator)recordOutput!.Value!;
                WithRelationshipsWriteLock(rels =>
                {
                    rels.IndexFeedGenerator(plc, rkey, (Generator)recordOutput.Value);
                    result = rels.TryGetFeedGeneratorData(new(plc, rkey));
                });
            }

            return result!;
        }

        public async Task<PostsAndContinuation> GetFirehosePostsAsync(DateTime maxDate, bool includeReplies, string? continuation, RequestContext ctx)
        {
            var limit = 30;
            var maxPostIdExclusive = continuation != null ? PostIdTimeFirst.Deserialize(continuation) : new PostIdTimeFirst(Tid.FromDateTime(maxDate, 0), default);
            var posts = WithRelationshipsLock(rels =>
            {
                var enumerables = rels.PostData.slices.Select(slice =>
                {
                    return rels.GetRecentPosts(slice, maxPostIdExclusive); //.AssertOrderedAllowDuplicates(x => (PostIdTimeFirst)x.PostId, new DelegateComparer<PostIdTimeFirst>((a, b) => b.CompareTo(a)));
                })
                .Append(rels.PostData.QueuedItems.Where(x => x.Key.CompareTo(maxPostIdExclusive) < 0 && !rels.PostDeletions.ContainsKey(x.Key)).OrderByDescending(x => x.Key).Take(limit).Select(x => rels.GetPost((PostId)x.Key, BlueskyRelationships.DeserializePostData(x.Values.AsUnsortedSpan(), x.Key))))
                .ToArray();

                var merged = SimpleJoin.ConcatPresortedEnumerablesKeepOrdered(enumerables, x => (PostIdTimeFirst)x.PostId, new DelegateComparer<PostIdTimeFirst>((a, b) => b.CompareTo(a)))
                    .Where(x => !x.Author.IsBlockedByAdministrativeRule);

                //  .AssertOrderedAllowDuplicates(x => (PostIdTimeFirst)x.PostId, new DelegateComparer<PostIdTimeFirst>((a, b) => b.CompareTo(a)));
                if (!includeReplies)
                    merged = merged.Where(x => x.IsRootPost && (x.Data?.IsReplyToUnspecifiedPost != true));
                return merged
                    .Take(limit)
                    .ToArray();
            });
            await EnrichAsync(posts, ctx);
            return (posts, posts.LastOrDefault()?.PostId.Serialize());
        }

        public async Task<ProfilesAndContinuation> GetPostLikersAsync(string did, string rkey, string? continuation, int limit, RequestContext ctx)
        {
            EnsureLimit(ref limit, 50);
            var profiles = WithRelationshipsLockForDid(did, (plc, rels) => rels.GetPostLikers(plc, rkey, DeserializeRelationshipContinuation(continuation), limit + 1));
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
            var profiles = WithRelationshipsLockForDid(did, (plc, rels) => rels.GetPostReposts(plc, rkey, DeserializeRelationshipContinuation(continuation), limit + 1));
            var nextContinuation = SerializeRelationshipContinuation(profiles, limit);
            SortByDescendingRelationshipRKey(ref profiles);
            //DeterministicShuffle(profiles, did + rkey);
            await EnrichAsync(profiles, ctx);
            return (profiles, nextContinuation);
        }

        public async Task<PostsAndContinuation> GetPostQuotesAsync(string did, string rkey, string? continuation, int limit, RequestContext ctx)
        {
            EnsureLimit(ref limit, 30);
            var posts = WithRelationshipsLockForDid(did, (plc, rels) => rels.GetPostQuotes(plc, rkey, continuation != null ? PostId.Deserialize(continuation) : default, limit + 1));
            var nextContinuation = SerializeRelationshipContinuationPlcFirst(posts, limit);
            SortByDescendingRelationshipRKey(ref posts);
            //DeterministicShuffle(posts, did + rkey);
            await EnrichAsync(posts, ctx, sideWithQuotee: true);
            return (posts, nextContinuation);
        }


        public async Task<ProfilesAndContinuation> GetFollowersAsync(string did, string? continuation, int limit, RequestContext ctx)
        {
            EnsureLimit(ref limit, 50);
            var profiles = WithRelationshipsLockForDid(did, (plc, rels) => rels.GetFollowers(plc, DeserializeRelationshipContinuation(continuation), limit + 1));
            var nextContinuation = SerializeRelationshipContinuation(profiles, limit);
            SortByDescendingRelationshipRKey(ref profiles);
            //DeterministicShuffle(profiles, did);
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
            var profile = WithRelationshipsLockForDid(did, (plc, rels) => rels.GetFullProfile(plc, ctx, followersYouFollowToLoad));
            await EnrichAsync([profile.Profile, ..profile.FollowedByPeopleYouFollow?.Take(followersYouFollowToLoad) ?? []], ctx);
            return profile;
        }
        public async Task<BlueskyProfile> GetProfileAsync(string did, RequestContext ctx)
        {
            var profile = GetSingleProfile(did);
            await EnrichAsync([profile], ctx);
            return profile;
        }
        public async Task<BlueskyProfile[]> GetProfilesAsync(string[] dids, RequestContext ctx, Action<BlueskyProfile>? onProfileDataAvailable = null)
        {
            var profiles = WithRelationshipsUpgradableLock(rels => dids.Select(x => rels.GetProfile(rels.SerializeDid(x))).ToArray());
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
            });
            await EnrichAsync([.. posts.Select(x => x.InReplyToFullPost).Where(x => x != null)!, .. posts.Select(x => x.RootFullPost).Where(x => x != null)!], ctx);
        }

        public async Task<BlueskyFeedGenerator> GetFeedGeneratorAsync(string did, string rkey)
        {
            var data = await GetFeedGeneratorDataAsync(did, rkey);
            return WithRelationshipsLockForDid(did, (plc, rels) => rels.GetFeedGenerator(plc, data));
        }

        public async Task<(BlueskyNotification[] NewNotifications, BlueskyNotification[] OldNotifications, Notification NewestNotification)> GetNotificationsAsync(RequestContext ctx)
        {
            if (!ctx.IsLoggedIn) return ([], [], default);
            var session = ctx.Session;
            var user = session.LoggedInUser!.Value;

            var notifications = WithRelationshipsLock(rels => rels.GetNotificationsForUser(user));
            var nonHiddenNotifications = notifications.NewNotifications.Concat(notifications.OldNotifications).Where(x => !x.Hidden).ToArray();
            await EnrichAsync(nonHiddenNotifications.Select(x => x.Post).Where(x => x != null).ToArray()!, ctx);
            await EnrichAsync(nonHiddenNotifications.Select(x => x.Profile).Where(x => x != null).ToArray()!, ctx);
            return (notifications.NewNotifications, notifications.OldNotifications, notifications.NewestNotification);
        }

        public async Task<(CoalescedNotification[] NewNotifications, CoalescedNotification[] OldNotifications, Notification NewestNotification)> GetCoalescedNotificationsAsync(RequestContext ctx)
        {
            var rawNotifications = await GetNotificationsAsync(ctx);

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

                var c = coalescedList.TakeWhile(x => (x.LatestDate - raw.EventDate).TotalHours < 24).FirstOrDefault(x => x.CoalesceKey == key);
                if (c == null)
                {
                    c = new CoalescedNotification
                    {
                        PostId = key.PostId,
                        Kind = key.Kind,
                        FeedRKeyHash = key.FeedRKeyHash,

                        LatestDate = raw.EventDate,
                        Post = raw.Post,
                        Feed = raw.Feed,
                        IsNew = areNew,
                    };
                    coalescedList.Add(c);
                }

                if (raw.Profile != null)
                {
                    c.Profiles ??= [];
                    c.Profiles.Add(raw.Profile);
                }
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
        public string? GetImageBannerUrl(string did, byte[] cid, string? pds, string? fileNameForDownload = null)
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

        public string? GetImageUrl(ThumbnailSize size, string did, byte[]? cid, string? pds, string? fileNameForDownload = null)
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
                    cdn == null && 
                    size != ThumbnailSize.feed_video_blob &&
                    !DidDocOverrides.GetValue().CustomDidDocs.ContainsKey(did)
                    ) cdn = "https://cdn.bsky.app";
            }

            var cidString = isNativeAtProto ? Cid.Read(cid).ToString() : Ipfs.Base32.ToBase32(cid);

            if (size is ThumbnailSize.video_thumbnail or ThumbnailSize.feed_video_playlist or ThumbnailSize.feed_video_blob)
            {

                if (isNativeAtProto && size == ThumbnailSize.feed_video_playlist)
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

        public long GetNotificationCount(AppViewLiteSession ctx)
        {
            if (!ctx.IsLoggedIn) return 0;
            return WithRelationshipsLock(rels => rels.GetNotificationCount(ctx.LoggedInUser!.Value));
        }



        public async Task<PostsAndContinuation> GetFollowingFeedAsync(string? continuation, int limit, RequestContext ctx)
        {
            EnsureLimit(ref limit, 50);
            Tid? maxTid = continuation != null ? Tid.Parse(continuation) : null;
            var alreadyReturned = new HashSet<PostId>();
            var posts = WithRelationshipsLock(rels =>
            {
                var posts = rels.EnumerateFollowingFeed(ctx.LoggedInUser, DateTime.Now.AddDays(-7), maxTid);
                var normalized = rels.EnumerateFeedWithNormalization(posts, alreadyReturned);
                return normalized.Take(limit).ToArray();
            });
            await EnrichAsync(posts, ctx);
            return new PostsAndContinuation(posts, posts.Length != 0 ? posts[^1].PostId.PostRKey.ToString() : null);
        }


        public async Task<PostsAndContinuation> GetBalancedFollowingFeedAsync(string? continuation, int limit, RequestContext ctx)
        {
            EnsureLimit(ref limit, 50);


    

            var now = DateTime.UtcNow;
            var candidates =
                WithRelationshipsLock(rels => rels.EnumerateFollowingFeed(ctx.LoggedInUser, DateTime.Now.AddDays(-1), null).ToArray())
                .Select(x => new ScoredBlueskyPost(x, GetDecayedScore(x.LikeCount, now - x.Date, 0.5), GetBalancedFeedGlobalScore(x.LikeCount, now - x.Date)))
                .ToArray();

            var alreadyReturnedPost = new HashSet<PostId>();
            var finalPosts = new List<BlueskyPost>();
            var postToMostRecentRepostDate = GetMostRecentRepostDates(candidates);

            var bestOriginalPostsByUser = candidates.Where(x => x.Post.RepostedBy == null).GroupBy(x => x.Post.AuthorId).Select(x => x.OrderByDescending(x => x.PerUserScore).ToQueue()).ToList();
            var bestRepostsByUser = candidates.Where(x => x.Post.RepostedBy != null).GroupBy(x => x.Post.AuthorId).Select(x => x.OrderByDescending(x => x.PerUserScore).ToQueue()).ToList();

            var mergedFollowedPosts = new Queue<ScoredBlueskyPost>();
            var mergedNonFollowedPosts = new Queue<ScoredBlueskyPost>();

            while (bestOriginalPostsByUser.Count != 0 && bestRepostsByUser.Count != 0)
            {
                var followedPosts = new List<ScoredBlueskyPost>();
                foreach (var user in bestOriginalPostsByUser)
                {
                    while (user.TryDequeue(out var post))
                    {
                        if (alreadyReturnedPost.Add(post.PostId))
                        {
                            followedPosts.Add(post);
                            break;
                        }
                    }
                }
                bestOriginalPostsByUser.RemoveAll(x => x.Count == 0);

                var nonFollowedReposts = new List<ScoredBlueskyPost>();
                foreach (var user in bestRepostsByUser)
                {
                    while (user.TryDequeue(out var post))
                    {
                        if (alreadyReturnedPost.Add(post.PostId))
                        {
                            if (post.Post.Author.IsFollowedBySelf != null)
                                followedPosts.Add(post);
                            else
                                nonFollowedReposts.Add(post);

                            break;
                        }
                    }
                }
                bestRepostsByUser.RemoveAll(x => x.Count == 0);

                
                mergedFollowedPosts.EnqueueRange(followedPosts.OrderByDescending(x => x.GlobalScore));
                mergedNonFollowedPosts.EnqueueRange(nonFollowedReposts.OrderByDescending(x => x.GlobalScore));


                var done = false;
                while (mergedFollowedPosts.Count != 0 && !done && finalPosts.Count < 200)
                {
                    for (int i = 0; i < 3; i++)
                    {
                        if (mergedFollowedPosts.TryDequeue(out var followed))
                        {
                            finalPosts.Add(followed.Post);
                        }
                        else
                        {
                            done = true;
                            break;
                        }
                    }
                    if (!done)
                    {
                        if (mergedNonFollowedPosts.TryDequeue(out var nonFollowed))
                            finalPosts.Add(nonFollowed.Post);
                    }
                }

            }

            var posts = finalPosts.ToArray();
            var alreadyReturnedAfterNormalization = new HashSet<PostId>();
            posts = WithRelationshipsLock(rels => rels.EnumerateFeedWithNormalization(posts, alreadyReturnedAfterNormalization).ToArray());
            await EnrichAsync(posts, ctx);
            return new PostsAndContinuation(posts, null);
        }

        private static Dictionary<PostId, DateTime> GetMostRecentRepostDates(ScoredBlueskyPost[] candidates)
        {
            var postToMostRecentRepostDate = new Dictionary<PostId, DateTime>();
            foreach (var candidate in candidates)
            {
                ref var date = ref CollectionsMarshal.GetValueRefOrAddDefault(postToMostRecentRepostDate, candidate.PostId, out var exists);
                if (!exists)
                    date = candidate.Post.Date;
                if (candidate.Post.RepostDate is { } repostDate && repostDate > date)
                    date = repostDate;
            }

            return postToMostRecentRepostDate;
        }

        record struct ScoredBlueskyPost(BlueskyPost Post, float PerUserScore, float GlobalScore)
        {
            public PostId PostId => Post.PostId;
            public override string ToString()
            {
                return $"{PerUserScore:0.000} | {GlobalScore:0.000} | +{Post.LikeCount} | {Post}";
            }
        }

        private static float GetBalancedFeedGlobalScore(long likeCount, TimeSpan age)
        {
            return GetDecayedScore(Math.Pow(likeCount, 0.1), age, 1.8);
        }
        private static float GetDecayedScore(double likeCount, TimeSpan age, double gravity)
        {
            // https://medium.com/hacking-and-gonzo/how-hacker-news-ranking-algorithm-works-1d9b0cf2c08d
            // HackerNews uses gravity=1.8
            if (age < TimeSpan.Zero) age = TimeSpan.Zero;
            var ageHours = age.TotalHours;
            var score = (likeCount + 1) / Math.Pow(ageHours + 2, gravity);
            return (float)score;
        }

        public async Task<RepositoryImportEntry?> ImportCarIncrementalAsync(string did, RepositoryImportKind kind, bool startIfNotRunning = true, TimeSpan ignoreIfRecentlyRan = default, CancellationToken ct = default)
        {

            Plc plc = default;
            RepositoryImportEntry? previousImport = null;
            WithRelationshipsLockForDid(did, (plc_, rels) =>
            {
                plc = plc_;
                previousImport = rels.GetRepositoryImports(plc).Where(x => x.Kind == kind).MaxBy(x => (x.LastRevOrTid, x.StartDate));
            });
            if (!startIfNotRunning)
            {
                Task<RepositoryImportEntry> running;
                lock (carImports)
                {
                    carImports.TryGetValue((plc, kind), out running);
                }
                if (running != null) return await running;
                return previousImport;
            }

            if (previousImport != null && (ignoreIfRecentlyRan != default && (DateTime.UtcNow - previousImport.StartDate) < ignoreIfRecentlyRan))
                return previousImport;

            Task<RepositoryImportEntry>? r;
            lock (carImports)
            {
                var key = (plc, kind);
                if (!carImports.TryGetValue(key, out r))
                {
                    if (!startIfNotRunning) return null;
                    r = ImportCarIncrementalCoreAsync(did, kind, plc, previousImport != null && previousImport.LastRevOrTid != 0 ? new Tid(previousImport.LastRevOrTid) : default, ct);
                    carImports[key] = r;
                }
            }
            return await r;
        }

        private async Task<RepositoryImportEntry> ImportCarIncrementalCoreAsync(string did, RepositoryImportKind kind, Plc plc, Tid since, CancellationToken ct)
        {
            await Task.Yield();
            var startDate = DateTime.UtcNow;
            var sw = Stopwatch.StartNew();

            var indexer = new Indexer(this);
            Tid lastTid;
            Exception? exception = null;

            if (kind == RepositoryImportKind.Full)
            {
                try
                {
                    lastTid = await indexer.ImportCarAsync(did, since, ct);
                }
                catch (Exception ex)
                {
                    exception = ex;
                    lastTid = default;
                }
            }
            else 
            {
                var recordType = kind switch {
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
                (lastTid, exception) = await indexer.IndexUserCollectionAsync(did, recordType, since, ct);
            }

            var summary = new RepositoryImportEntry
            {
                DurationMillis = (long)sw.Elapsed.TotalMilliseconds,
                Kind = kind,
                Plc = plc,
                StartDate = startDate,
                LastRevOrTid = lastTid.TidValue,
                Error = exception != null ? GetErrorDetails(exception) : null,
                
            };
            WithRelationshipsWriteLock(rels =>
            {
                rels.CarImports.AddRange(new RepositoryImportKey(plc, startDate), BlueskyRelationships.SerializeProto(summary));
            });
            return summary;
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

        public string GetCarImportStatus(string did, RepositoryImportKind kind)
        {
            var task = ImportCarIncrementalAsync(did, kind, startIfNotRunning: false);
            if (task.IsCompletedSuccessfully)
            {
                var r = task.Result;
                if (r == null) return "Not running.";
                var durationSeconds = (r.DurationMillis / 1000.0).ToString("0.0");
                if (r.Error != null) return $"Error: {r.Error}, Duration: {durationSeconds} seconds.";
                return $"Completed. Start date: {r.StartDate}, Duration: {durationSeconds} seconds.";
            }
            else if (task.IsFaulted)
                return $"Failed: {task.Exception.Message}";
            else
                return "Running...";
        }

        public async Task<Tid> CreateRecordAsync(ATObject record, RequestContext ctx)
        {
            return await PerformPdsActionAsync(async session => Tid.Parse((await session.CreateRecordAsync(new ATDid(session.Session.Did.Handler), record.Type, record)).HandleResult()!.Uri.Rkey), ctx);
        }

        private async Task<T> PerformPdsActionAsync<T>(Func<ATProtocol, Task<T>> func, RequestContext ctx)
        {
            var session = ctx.Session;
            using var sessionProtocol = await GetSessionProtocolAsync(ctx);
            if (sessionProtocol.AuthSession!.Session.ExpiresIn.AddMinutes(-5) > DateTime.UtcNow)
            {
                try
                {
                    return await func(sessionProtocol);
                }
                catch (ATNetworkErrorException ex) when (ex.AtError.Detail?.Error == "ExpiredToken")
                {
                    // continue
                }
            }

            var authSession = await sessionProtocol.RefreshAuthSessionAsync();

            WithRelationshipsLock(rels =>
            {
                var proto = rels.TryGetAppViewLiteProfile(session.LoggedInUser!.Value)!;
                proto.PdsSessionCbor = SerializeAuthSession(authSession!);
                ctx.Session.PdsSession = authSession!.Session;
                rels.StoreAppViewLiteProfile(ctx.LoggedInUser, proto);
            });
            
            using var sessionProtocol2 = await GetSessionProtocolAsync(ctx);
            return await func(sessionProtocol2);
        }

        public static byte[] SerializeAuthSession(AuthSession authSession)
        {
            return CBORObject.FromJSONString(authSession.ToString()).EncodeToBytes();
        }
        public static AuthSession DeserializeAuthSession(byte[] bytes)
        {
            return AuthSession.FromString(CBORObject.DecodeFromBytes(bytes).ToJSONString());
        }

        public async Task<ATProtocol> GetSessionProtocolAsync(RequestContext ctx)
        {
            if (!ctx.IsLoggedIn) throw new ArgumentException();
            if (ctx.Session.IsReadOnlySimulation) throw new InvalidOperationException("Read only simulation.");
            var pdsSession = ctx.Session.PdsSession!;
            var sessionProtocol = await CreateProtocolForDidAsync(pdsSession.Did.Handler);
            (await sessionProtocol.AuthenticateWithPasswordSessionResultAsync(new AuthSession(pdsSession))).HandleResult();
            return sessionProtocol;

        }

        public async Task<Session> LoginToPdsAsync(string did, string password)
        {
            var sessionProtocol = await CreateProtocolForDidAsync(did);
            var session = (await sessionProtocol.AuthenticateWithPasswordResultAsync(did, password)).HandleResult();
            return session;
        }

        public async Task<ATProtocol?> TryCreateProtocolForDidAsync(string did)
        {
            if (!BlueskyRelationships.IsNativeAtProtoDid(did)) return null;
            return await CreateProtocolForDidAsync(did)!;
        }
        public async Task<ATProtocol> CreateProtocolForDidAsync(string did)
        {
            var diddoc = await GetDidDocAsync(did);
            AdministrativeBlocklist.ThrowIfBlockedOutboundConnection(did, diddoc);
            
            var pds = diddoc.Pds;
            var builder = new ATProtocolBuilder();
            builder.WithInstanceUrl(new Uri(pds));
            var dict = new Dictionary<ATDid, Uri>
            {
                { new ATDid(did), new Uri(pds) }
            };
            builder.WithATDidCache(dict);
            return builder.Build();
        }

        public async Task<Tid> CreateFollowAsync(string did, RequestContext ctx)
        {
            return await CreateRecordAsync(new Follow { Subject = new ATDid(did) }, ctx);
        }
        public async Task DeleteFollowAsync(Tid rkey, RequestContext ctx)
        {
            await DeleteRecordAsync(Follow.RecordType, rkey, ctx);
        }
        public async Task<Tid> CreatePostLikeAsync(string did, Tid rkey, RequestContext ctx)
        {
            var cid = await GetCidAsync(did, Post.RecordType, rkey);
            return await CreateRecordAsync(new Like { Subject = new StrongRef(new ATUri("at://" + did + "/" + Post.RecordType + "/" + rkey), cid) }, ctx);
        }
        public async Task<Tid> CreateRepostAsync(string did, Tid rkey, RequestContext ctx)
        {
            var cid = await GetCidAsync(did, Post.RecordType, rkey);
            return await CreateRecordAsync(new Repost { Subject = new StrongRef(new ATUri("at://" + did + "/" + Post.RecordType + "/" + rkey), cid) }, ctx);
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
            await PerformPdsActionAsync(session => session.DeleteRecordAsync(session.Session.Did, collection, rkey.ToString()), ctx);
        }

        public async Task<Tid> CreatePostAsync(string text, PostIdString? inReplyTo, PostIdString? quotedPost, RequestContext ctx)
        {
            ReplyRefDef? replyRefDef = null;
            if (inReplyTo != null)
            {
                var inReplyToRef = await GetPostStrongRefAsync(inReplyTo);
                replyRefDef = new ReplyRefDef
                {
                    Parent = inReplyToRef.StrongRef,
                    Root = inReplyToRef.Record.Reply?.Root ?? inReplyToRef.StrongRef,
                };
            }
            ATObject? embed = null;
            if (quotedPost != null)
            {
                embed = new EmbedRecord
                {
                    Record = (await GetPostStrongRefAsync(quotedPost)).StrongRef,
                };
            }
            return await CreateRecordAsync(new Post { Text = text, Reply = replyRefDef, Embed = embed }, ctx);
        }

        private async Task<(StrongRef StrongRef, Post Record)> GetPostStrongRefAsync(PostIdString post)
        {
            var info = await GetRecordAsync(post.Did, Post.RecordType, post.RKey);
            return (new StrongRef(info.Uri, info.Cid!), (Post)info.Value);
            
        }

        internal Task<string> GetCidAsync(string did, string collection, Tid rkey)
        {
            return GetCidAsync(did, collection, rkey.ToString()!);
        }
        internal async Task<string> GetCidAsync(string did, string collection, string rkey)
        {
            return (await GetRecordAsync(did, collection, rkey)).Cid!;
        }

        public async Task<BlueskyPost> GetPostAsync(string did, string rkey, RequestContext ctx)
        {
            var post = GetSinglePost(did, rkey);
            await EnrichAsync([post], ctx);
            return post;
        }




        public async Task<(BlueskyList[] Lists, string? NextContinuation)> GetMemberOfListsAsync(string did, string? continuation, int limit, RequestContext ctx)
        {
            EnsureLimit(ref limit, 10);

            ListMembership? parsedContinuation = continuation != null ? ListMembership.Deserialize(continuation) : null;

            var lists = WithRelationshipsLockForDid(did, (plc, rels) =>
            {
                return rels.ListMemberships.GetValuesSorted(plc, parsedContinuation)
                    .Where(x => !rels.ListItemDeletions.ContainsKey(new(x.ListAuthor, x.ListItemRKey)))
                    .Select(x => rels.GetList(new(x.ListAuthor, x.ListRKey)))
                    .Where(x => x.Data?.Deleted != true)
                    .Take(limit + 1)
                    .ToArray();
            });

            await EnrichAsync(lists, ctx);
            return GetPageAndNextPaginationFromLimitPlus1(lists, limit, x => x.ListId.Serialize());
        }

        private async Task<BlueskyFeedGenerator[]> EnrichAsync(BlueskyFeedGenerator[] feeds, RequestContext ctx, CancellationToken ct = default)
        {
            await EnrichAsync(feeds.Select(x => x.Author).ToArray(), ctx, ct: ct);
            return feeds;
        }

        public async Task<BlueskyLabel[]> EnrichAsync(BlueskyLabel[] labels, RequestContext ctx)
        {
            var toLookup = labels.Where(x => x.Data == null).GroupBy(x => (x.LabelerPlc, x.LabelerDid)).ToArray();
            if (toLookup.Length == 0) return labels;
            var all = Task.WhenAll(toLookup.Select(async x =>
            {
                await FetchAndStoreLabelerServiceMetadata.GetTaskAsync(x.Key.LabelerDid);
                foreach (var label in x)
                {
                    var data = WithRelationshipsLock(rels => rels.TryGetLabelData(label.LabelId));
                    label.Data = data;
                }
            }));
            if (ctx.ShortDeadline != null)
                all = Task.WhenAny(all, ctx.ShortDeadline);

            await all;
            return labels;
        }

        private async Task<BlueskyList[]> EnrichAsync(BlueskyList[] lists, RequestContext ctx, CancellationToken ct = default)
        {

            var toLookup = lists.Where(x => x.Data == null).Select(x => x.ListIdStr).Distinct().ToArray();
            if (toLookup.Length == 0) return lists;

            toLookup = WithRelationshipsUpgradableLock(rels => toLookup.Where(x => !rels.FailedProfileLookups.ContainsKey(rels.SerializeDid(x.Did))).ToArray());
            if (toLookup.Length == 0) return lists;

            var idToList = lists.ToLookup(x => x.ListId);

            void OnRecordReceived(BlueskyRelationships rels, Models.Relationship listId)
            {
                foreach (var list in idToList[listId])
                {
                    if (list.Data == null)
                        list.Data = rels.TryGetListData(list.ListId);
                }
            }

            var task = LookupManyRecordsWithinShortDeadlineAsync<List>(toLookup, pendingProfileRetrievals, List.RecordType, ct,
                (key, listRecord) =>
                {
                    WithRelationshipsWriteLock(rels =>
                    {
                        var id = new Models.Relationship(rels.SerializeDid(key.Did), Tid.Parse(key.RKey));
                        rels.Lists.AddRange(id, BlueskyRelationships.SerializeProto(BlueskyRelationships.ListToProto(listRecord)));
                        OnRecordReceived(rels, id);
                    });
                },
                key =>
                {
                    WithRelationshipsWriteLock(rels =>
                    {
                        var id = new Models.Relationship(rels.SerializeDid(key.Did), Tid.Parse(key.RKey));
                        rels.FailedListLookups.Add(id, DateTime.UtcNow);
                        OnRecordReceived(rels, id);
                    });
                },
                key =>
                {
                    WithRelationshipsLock(rels =>
                    {
                        var id = new Models.Relationship(rels.SerializeDid(key.Did), Tid.Parse(key.RKey));
                        OnRecordReceived(rels, id);
                    });
                }
                , ctx);

            
            await task;
            await EnrichAsync(lists.Select(x => x.Author).ToArray(), ctx, ct: ct);
            return lists;
        }

        public async Task<(BlueskyList List, BlueskyProfile[] Page, string? NextContinuation)> GetListMembersAsync(string did, string rkey, string? continuation, int limit, RequestContext ctx)
        {
            EnsureLimit(ref limit);
            var listId = WithRelationshipsLockForDid(did, (plc, rels) => new Models.Relationship(plc, Tid.Parse(rkey)));
            var list = WithRelationshipsLock(rels => rels.GetList(listId));
            
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

            var response = await ListRecordsAsync(did, Listitem.RecordType, limit: limit + 1, cursor: continuation);
            var members = WithRelationshipsUpgradableLock(rels =>
            {
                return response!.Records!.Select(x => rels.GetProfile(rels.SerializeDid(((FishyFlip.Lexicon.App.Bsky.Graph.Listitem)x.Value!).Subject!.Handler))).ToArray();
            });
            await EnrichAsync(members, ctx);
            return (list, members, response.Records.Count > limit ? response!.Cursor : null);

#endif
        }

        public async Task<ListRecordsOutput> ListRecordsAsync(string did, string collection, int limit, string? cursor, CancellationToken ct = default)
        {
            using var proto = await TryCreateProtocolForDidAsync(did);
            if (proto == null) return new ListRecordsOutput(null, []);
            try
            {
                return (await proto.ListRecordsAsync(GetAtId(did), collection, limit, cursor, cancellationToken: ct)).HandleResult()!;
            }
            catch (Exception ex)
            {
                throw CreateExceptionMessageForExternalServerError($"The PDS of this user", ex);
            }
            
        }

        public async Task<GetRecordOutput> GetRecordAsync(string did, string collection, string rkey, CancellationToken ct = default)
        {
            using var proto = await CreateProtocolForDidAsync(did);
            try
            {
                return (await proto.GetRecordAsync(GetAtId(did), collection, rkey, cancellationToken: ct)).HandleResult()!;
            }
            catch (Exception ex)
            {
                throw CreateExceptionMessageForExternalServerError($"The PDS of this user", ex);
            }
            
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
                        var f = rels.TryGetFeedGenerator(r);
                        if (f != null)
                            list.Add(f);
                    }
                }
                return list;
                
            });
            
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
                .Select(x => rels.TryGetFeedGenerator(x)!)
                .Where(x => 
                {
                    var words = StringUtils.GetAllWords(x.Data?.DisplayName).Concat(StringUtils.GetAllWords(x.Data?.Description)).Distinct().ToArray();
                    return queryWords.All(x => words.Contains(x));
                })
                .Where(x => x != null && x.Data?.Deleted != true)
                .Take(limit + 1)
                .ToArray();
            });
            await EnrichAsync(feeds, ctx);
            return GetPageAndNextPaginationFromLimitPlus1(feeds, limit, x => x.FeedId.Serialize());
        }

        

        public async Task<ProfilesAndContinuation> SearchProfilesAsync(string query, bool allowPrefixForLastWord, string? continuation, int limit, RequestContext ctx, Action<BlueskyProfile>? onProfileDataAvailable = null)
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
                    .Select(x => rels.GetProfile(x))
                    .Where(x => rels.ProfileMatchesSearchTerms(x, parsedContinuation.AlsoSearchDescriptions, queryWords, wordPrefix))
                    .Where(x => alreadyReturned.Add(x.Plc))
                    .Select(x => 
                    {
                        followerCount[x.Plc] = rels.Follows.GetActorCount(x.Plc);
                        return x;
                    })
                    .Take(limit + 1 - profiles.Count)
                    .ToArray();
                });

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
                            .Select(x => rels.GetProfile(x))
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
                    });
                    result.Items = result.Items.Concat(extra).ToArray();
                }
            }

            await EnrichAsync(result.Items, ctx, onProfileDataAvailable: onProfileDataAvailable);
            

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

            var response = await ListRecordsAsync(did, List.RecordType, limit: limit + 1, cursor: continuation);
            var lists = WithRelationshipsLockForDid(did, (plc, rels) =>
            {
                return response!.Records!.Select(x =>
                {
                    var listId = new Models.Relationship(plc, Tid.Parse(x.Uri.Rkey));
                    return rels.GetList(listId, BlueskyRelationships.ListToProto((List)x.Value));
                }).ToArray();
            });
            await EnrichAsync(lists, ctx);
            return GetPageAndNextPaginationFromLimitPlus1(lists, limit, x => x.RKey);

        }


        public async Task<(BlueskyFeedGenerator[] Feeds, string? NextContinuation)> GetProfileFeedsAsync(string did, string? continuation, int limit, RequestContext ctx)
        {
            EnsureLimit(ref limit);

            var response = await ListRecordsAsync(did, Generator.RecordType, limit: limit + 1, cursor: continuation);
            var feeds = WithRelationshipsUpgradableLock(rels =>
            {
                var plc = rels.SerializeDid(did);
                return response!.Records!.Select(x =>
                {
                    var feedId = new RelationshipHashedRKey(plc, x.Uri.Rkey);
                    if (!rels.FeedGenerators.ContainsKey(feedId))
                    {
                        rels.WithWriteUpgrade(() => rels.IndexFeedGenerator(plc, x.Uri.Rkey, (Generator)x.Value));
                    }
                    return rels.TryGetFeedGenerator(feedId)!;
                }).ToArray();
            });
            await EnrichAsync(feeds, ctx);
            return GetPageAndNextPaginationFromLimitPlus1(feeds, limit, x => x.RKey);

        }

        public static TimeSpan HandleToDidMaxStale = TimeSpan.FromHours(Math.Max(1, AppViewLiteConfiguration.GetInt32(AppViewLiteParameter.APPVIEWLITE_HANDLE_TO_DID_MAX_STALE_HOURS) ?? (10 * 24)));
        public static TimeSpan DidDocMaxStale = TimeSpan.FromHours(Math.Max(1, AppViewLiteConfiguration.GetInt32(AppViewLiteParameter.APPVIEWLITE_DID_DOC_MAX_STALE_HOURS) ?? (2 * 24)));

        public async Task<string> ResolveHandleAsync(string handle, bool forceRefresh = false, CancellationToken ct = default)
        {
            handle = StringUtils.NormalizeHandle(handle);
            if (handle.StartsWith('@')) handle = handle.Substring(1);

            AdministrativeBlocklist.ThrowIfBlockedDisplay(handle);

            if (handle.StartsWith("did:", StringComparison.Ordinal))
            {
                EnsureValidDid(handle);
                return handle;
            }

            foreach (var pluggableProtocol in AppViewLite.PluggableProtocols.PluggableProtocol.RegisteredPluggableProtocols)
            {
                if (pluggableProtocol.TryHandleToDid(handle) is { } pluggableDid)
                {
                    AdministrativeBlocklist.ThrowIfBlockedDisplay(pluggableDid);
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
            });



            if (forceRefresh || plc == default || (DateTime.UtcNow - handleToDidVerificationDate) > HandleToDidMaxStale)
            {
                await HandleToDidCoreAsync(handle, ct);
                (handleToDidVerificationDate, plc, did) = WithRelationshipsLock(rels =>
                {
                    var lastVerification = rels.HandleToDidVerifications.TryGetLatestValue(handleUuid, out var r) ? r : default;
                    var did = lastVerification.Plc != default ? rels.GetDid(lastVerification.Plc) : null;
                    return (lastVerification.VerificationDate, lastVerification.Plc, did);
                });
            }
            if (plc == default) throw new Exception();
            var didDoc = WithRelationshipsLock(rels =>
            {
                return rels.TryGetLatestDidDoc(plc);
            });



            if (forceRefresh || IsDidDocStale(did!, didDoc))
            {
                // if this is did:plc, the did-doc will be retrieved from plc.directory (as trustworthy as RetrievePlcDirectoryAsync())
                // otherwise did:web, but they're in a different namespace
                didDoc = DidDocOverrides.GetValue().TryGetOverride(did!) ?? await FetchAndStoreDidDocNoOverrideAsync(did!, plc);
            }
            if (!didDoc!.HasHandle(handle))
            {
                if (!forceRefresh)
                {
                    return await ResolveHandleAsync(handle, forceRefresh: true, ct);
                }


                if ("did:web:" + handle != did)
                    throw new UnexpectedFirehoseDataException($"Bidirectional handle verification failed: {handle} => {did} => {didDoc.Handle}");
            }

            foreach (var extraHandle in didDoc.AllHandlesAndDomains)
            {
                AdministrativeBlocklist.ThrowIfBlockedDisplay(extraHandle);
            }

            return did!;
        }

        private async Task<DidDocProto> FetchAndStoreDidDocNoOverrideAsync(string did, Plc plc)
        {
            var didDoc = await GetDidDocCoreNoOverrideAsync(did);
            didDoc.Date = DateTime.UtcNow;
            WithRelationshipsWriteLock(rels =>
            {
                rels.CompressDidDoc(didDoc);
                rels.DidDocs.AddRange(plc, didDoc.SerializeToBytes());
                didDoc = rels.TryGetLatestDidDoc(plc);
            });
            return didDoc;
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


        private async Task<string> HandleToDidCoreAsync(string handle, CancellationToken ct)
        {
            AdministrativeBlocklist.ThrowIfBlockedOutboundConnection(handle);

            try
            {
                // Is it valid to have multiple TXTs listing different DIDs? bsky.app seems to support that.
                //Console.Error.WriteLine("ResolveHandleCoreAsync: " + handle);
                
                if (!handle.EndsWith(".bsky.social", StringComparison.Ordinal)) // avoid wasting time, bsky.social uses .well-known
                {
                    string? record;
                    if (DnsUseHttps)
                    {
                        using var request = new HttpRequestMessage(HttpMethod.Get, "https://" + DnsForTxtResolution + "/dns-query?name=_atproto." + Uri.EscapeDataString(handle) + "&type=TXT");
                        request.Headers.Accept.Clear();
                        request.Headers.Accept.ParseAdd("application/dns-json");
                        using var response = await DefaultHttpClient.SendAsync(request, ct);
                        var result = await response.Content.ReadFromJsonAsync<DnsOverHttpsResponse>(ct);
                        record = result!.Answer?.Where(x => x.type == 16).Select(x => Regex.Match(x.data, @"did=[^\\""]+").Value?.Trim()).FirstOrDefault(x => !string.IsNullOrEmpty(x));
                    }
                    else
                    {
                        var lookup = new LookupClient(System.Net.IPAddress.Parse(DnsForTxtResolution), 53);
                        var result = await lookup.QueryAsync("_atproto." + handle, QueryType.TXT, cancellationToken: ct);
                        record = result.Answers.TxtRecords()
                            .Select(x => x.Text.Select(x => x.Trim()).FirstOrDefault(x => !string.IsNullOrEmpty(x)))
                            .FirstOrDefault(x => x != null && x.StartsWith("did=", StringComparison.Ordinal));
                    }
                    if (record != null)
                    {
                        var did = record.Substring(4);
                        EnsureValidDid(did);
                        WithRelationshipsWriteLock(rels =>
                        {
                            rels.IndexHandle(handle, did);
                            rels.AddHandleToDidVerification(handle, rels.SerializeDid(did));
                        });
                        return did;
                    }
                }

                var s = (await DefaultHttpClient.GetStringAsync("https://" + handle + "/.well-known/atproto-did", ct)).Trim();
                EnsureValidDid(s);
                WithRelationshipsWriteLock(rels =>
                {
                    rels.IndexHandle(handle, s);
                    rels.AddHandleToDidVerification(handle, rels.SerializeDid(s));
                });
                return s;
            }
            catch
            {
                WithRelationshipsWriteLock(rels =>
                {
                    rels.AddHandleToDidVerification(handle, default);
                });
                throw;
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
                var colon = did.IndexOf(':', 4);
                if (colon != -1)
                {
                    var pluggable = BlueskyRelationships.TryGetPluggableProtocolForDid(did);
                    if (pluggable != null)
                    {
                        pluggable.EnsureValidDid(did);
                        return;
                    }
                    throw new UnexpectedFirehoseDataException("Invalid did or no pluggable protocol registered for " + did.Substring(0, colon));
                }
                throw new UnexpectedFirehoseDataException("Invalid did.");
            }
        }


        public static bool IsValidDomain(ReadOnlySpan<char> domain)
        {
            if (domain.IsEmpty || !domain.Contains('.')) return false; // fast path
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

        internal async Task<DidDocProto> GetDidDocCoreNoOverrideAsync(string did)
        {
            Console.Error.WriteLine("GetDidDocAsync: " + did);
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


            var didDocJson = await DefaultHttpClient.GetFromJsonAsync<DidWebRoot>(didDocUrl)!;
            var didDoc = Indexer.DidDocToProto(didDocJson);
            return didDoc;
        }

        public readonly static string PlcDirectoryPrefix = AppViewLiteConfiguration.GetString(AppViewLiteParameter.APPVIEWLITE_PLC_DIRECTORY) ?? "https://plc.directory";

        public async Task<string> GetVerifiedHandleAsync(string did)
        {
            var didDoc = await GetDidDocCoreNoOverrideAsync(did);

            return didDoc.Handle!;
        }


        public async Task<BlobResult> GetBlobAsync(string did, string cid, string? pds, ThumbnailSize preferredSize, CancellationToken ct)
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
                    pds = (await GetDidDocAsync(did)).Pds;
                }
                return await GetBlobFromUrl(new Uri($"{pds}/xrpc/com.atproto.sync.getBlob?did={did}&cid={cid}"), ignoreFileName: true, ct: ct);
            }
        }

        private async Task<DidDocProto> GetDidDocAsync(string did)
        {
            var didDocOverride = DidDocOverrides.GetValue().TryGetOverride(did);
            if (didDocOverride != null) return didDocOverride;

            DidDocProto? doc;
            (var plc, doc) = WithRelationshipsLockForDid(did, (plc, rels) =>
            {
                return (plc, rels.TryGetLatestDidDoc(plc));
            });
            if (IsDidDocStale(did, doc))
            {
                doc = await FetchAndStoreDidDoc.GetValueAsync((plc, did));
            }

            return doc!;
        }
        public async Task<ATUri> ResolveUriAsync(string uri)
        {
            var aturi = new ATUri(uri);
            if (aturi.Did != null) return aturi;

            var did = await ResolveHandleAsync(aturi.Handle!.Handle);
            return new ATUri("at://" + did + aturi.Pathname + aturi.Hash);
        }

        public void RegisterPluggableProtocol(Type type)
        {
            var protocol = AppViewLite.PluggableProtocols.PluggableProtocol.Register(type);
            protocol.Apis = this;
        }

        public void MaybeAddCustomEmojis(CustomEmoji[]? emojis)
        {
            if (emojis == null || emojis.Length == 0) return;

            var missingEmojis = WithRelationshipsLock(rels => emojis.Where(x => !rels.CustomEmojis.ContainsKey(x.Hash)).ToArray());

            if (missingEmojis.Length == 0) return;
            WithRelationshipsWriteLock(rels =>
            {
                foreach (var emoji in missingEmojis)
                {
                    rels.CustomEmojis.AddRange(emoji.Hash, BlueskyRelationships.SerializeProto(emoji));
                }
            });
        }

        public CustomEmoji? TryGetCustomEmoji(DuckDbUuid hash)
        {
            return WithRelationshipsLock(rels =>
            {
                if (rels.CustomEmojis.TryGetPreserveOrderSpanAny(hash, out var bytes))
                    return BlueskyRelationships.DeserializeProto<CustomEmoji>(bytes.AsSmallSpan());
                return null;
            });
        }

        private readonly Dictionary<(Plc, RepositoryImportKind), Task<RepositoryImportEntry>> carImports = new();
        public readonly static HttpClient DefaultHttpClient;

        static BlueskyEnrichedApis()
        {
            DefaultHttpClient = new HttpClient(new BlocklistableHttpClientHandler(new SocketsHttpHandler 
            { 
                AllowAutoRedirect = true,
                AutomaticDecompression = System.Net.DecompressionMethods.All,
            }, true), true);
            DefaultHttpClient.DefaultRequestHeaders.UserAgent.ParseAdd("Mozilla/5.0");
            DefaultHttpClient.MaxResponseContentBufferSize = 10 * 1024 * 1024;
            DefaultHttpClient.Timeout = TimeSpan.FromSeconds(10);
        }

        public AdministrativeBlocklist AdministrativeBlocklist => AdministrativeBlocklist.Instance.GetValue();

        public async Task<string?> TryGetBidirectionalAtProtoBridgeForFediverseProfileAsync(BlueskyProfile profile)
        {
            if (!profile.Did.StartsWith(AppViewLite.PluggableProtocols.ActivityPub.ActivityPubProtocol.DidPrefix, StringComparison.Ordinal))
                return null;

            var userId = AppViewLite.PluggableProtocols.ActivityPub.ActivityPubProtocol.ParseDid(profile.Did);

            var possibleHandle = userId.UserName + "." + userId.Instance + ".ap.brid.gy";
            if (!WithRelationshipsLock(rels => rels.HandleToPossibleDids.ContainsKey(BlueskyRelationships.HashWord(possibleHandle))))
                return null;

            try
            {
                var resolved = await ResolveHandleAsync(possibleHandle);
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
    }
}


 
