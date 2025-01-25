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
using FishyFlip.Lexicon.Com.Atproto.Identity;
using AppViewLite.Storage;
using System.Collections.Concurrent;
using System.Text.RegularExpressions;
using AppViewLite.Models;
using AppViewLite.Numerics;
using System.Runtime.InteropServices;
using FishyFlip.Tools;
using FishyFlip.Lexicon.App.Bsky.Graph;
using AppViewLite.Numerics;
using System.Diagnostics;
using System.Globalization;
using AppViewLite;
using PeterO.Cbor;
using FishyFlip.Lexicon.App.Bsky.Embed;

namespace AppViewLite
{
    public class BlueskyEnrichedApis
    {
        public static BlueskyEnrichedApis Instance;
        internal ATProtocol proto;
        internal ATProtocol protoAppView;
        private BlueskyRelationships relationshipsUnlocked;

        public BlueskyRelationships DangerousUnlockedRelationships => relationshipsUnlocked;

        public BlueskyEnrichedApis(BlueskyRelationships relationships)
        {
            this.proto = new ATProtocolBuilder().WithInstanceUrl(new Uri("https://bsky.network")).Build();
            this.protoAppView = new ATProtocolBuilder().Build();
            this.relationshipsUnlocked = relationships;
        }

        public async Task<string> ResolveHandleAsync(string handle)
        {
            if (handle.StartsWith('@')) handle = handle.Substring(1);
            if (handle.StartsWith("did:", StringComparison.Ordinal)) return handle;
            var resolved = (await protoAppView.ResolveHandleAsync(new ATHandle(handle))).HandleResult();
            return resolved!.Did!.ToString();
        }
        public T WithRelationshipsLock<T>(Func<BlueskyRelationships, T> func)
        {
            BlueskyRelationships.VerifyNotEnumerable<T>();
            lock (relationshipsUnlocked)
            {
                var result = func(relationshipsUnlocked);
                relationshipsUnlocked.MaybeGlobalFlush();
                return result;
            }
        }
        public void WithRelationshipsLock(Action<BlueskyRelationships> func)
        {
            lock (relationshipsUnlocked)
            {
                func(relationshipsUnlocked);
                relationshipsUnlocked.MaybeGlobalFlush();
            }
        }

        

        private ConcurrentDictionary<RelationshipStr, (Task Task, DateTime DateStarted)> pendingProfileRetrievals = new();
        private ConcurrentDictionary<RelationshipStr, (Task Task, DateTime DateStarted)> pendingPostRetrievals = new();

        public async Task<BlueskyProfile[]> EnrichAsync(BlueskyProfile[] profiles, RequestContext ctx, CancellationToken ct = default)
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
                }
            });
            
            var toLookup = profiles.Where(x => x.BasicData == null).Select(x => new RelationshipStr(x.Did, "self")).Distinct().ToArray();
            if (toLookup.Length == 0) return profiles;

            toLookup = WithRelationshipsLock(rels => toLookup.Where(x => !rels.FailedProfileLookups.ContainsKey(rels.SerializeDid(x.Did))).ToArray());
            if (toLookup.Length == 0) return profiles;

            var plcToProfile = profiles.ToLookup(x => x.Plc);

            void OnRecordReceived(BlueskyRelationships rels, Plc plc)
            {
                foreach (var profile in plcToProfile[plc])
                {
                    if (profile.BasicData == null)
                        profile.BasicData = rels.GetProfileBasicInfo(profile.Plc);
                }
            }

            var task = LookupManyRecordsWithinShortDeadlineAsync<Profile>(toLookup, pendingProfileRetrievals, Profile.RecordType, ct,
                (key, profileRecord) =>
                {
                    WithRelationshipsLock(rels =>
                    {
                        var plc = rels.SerializeDid(key.Did);
                        rels.StoreProfileBasicInfo(plc, profileRecord);
                        OnRecordReceived(rels, plc);
                    });
                },
                key =>
                {
                    WithRelationshipsLock(rels =>
                    {
                        var plc = rels.SerializeDid(key.Did);
                        rels.FailedProfileLookups.Add(plc, DateTime.UtcNow);
                        OnRecordReceived(rels, plc);
                    });
                }, ctx);

            await task;

            return profiles;
        }

        public async Task<ProfilesAndContinuation> GetFollowingAsync(string did, string? continuation, int limit, RequestContext ctx)
        {
            EnsureLimit(ref limit, 50);
            var response = (await proto.Repo.ListRecordsAsync(GetAtId(did), Follow.RecordType, limit: limit + 1, cursor: continuation)).HandleResult();
            var following = WithRelationshipsLock(rels =>
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

        public async Task<BlueskyPost[]> EnrichAsync(BlueskyPost[] posts, RequestContext ctx, bool loadQuotes = true, CancellationToken ct = default)
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

                }
            }


            var task = LookupManyRecordsWithinShortDeadlineAsync<Post>(toLookup, pendingPostRetrievals, Post.RecordType, ct,
                (key, postRecord) =>
                {
                    WithRelationshipsLock(rels =>
                    {
                        rels.SuppressNotificationGeneration++;
                        try
                        {
                            var postId = rels.GetPostId(key.Did, key.RKey);
                            rels.StorePostInfo(postId, postRecord);
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
                    WithRelationshipsLock(rels =>
                    {
                        var postId = rels.GetPostId(key.Did, key.RKey);
                        rels.FailedPostLookups.Add(rels.GetPostId(key.Did, key.RKey), DateTime.UtcNow);
                        OnRecordReceived(rels, postId);
                    });
                }, ctx);



            await EnrichAsync(posts.SelectMany(x => new[] { x.Author, x.InReplyToUser, x.RepostedBy }).Where(x => x != null).ToArray(), ctx, ct);

            if (loadQuotes)
            {
                var r = posts.Select(x => x.QuotedPost).Where(x => x != null).ToArray();
                if (r.Length != 0)
                {
                    await EnrichAsync(r, ctx, loadQuotes: false, ct: ct);
                }
            }
            return posts;
        }

        private Task LookupManyRecordsWithinShortDeadlineAsync<TValue>(IReadOnlyList<RelationshipStr> keys, ConcurrentDictionary<RelationshipStr, (Task Task, DateTime DateStarted)> pendingRetrievals, string collection, CancellationToken ct, Action<RelationshipStr, TValue> onItemSuccess, Action<RelationshipStr> onItemFailure, RequestContext ctx) where TValue : ATObject
        {

            if (keys.Count != 0 && (ctx.LongDeadline == null || !ctx.LongDeadline.IsCompleted))
            {
                CleanupOldRetrievals(pendingRetrievals);

                var keysToLookup = new List<RelationshipStr>();
                var allTasksToAwait = new List<Task>();
                foreach (var key in keys)
                {
                    if (pendingRetrievals.TryGetValue(key, out var task))
                    {
                        task.Task.GetAwaiter().OnCompleted(ctx.TriggerStateChange);
                        allTasksToAwait.Add(task.Task);
                    }
                    else if (keysToLookup.Count + 1 < 100)
                        keysToLookup.Add(key);
                }

                allTasksToAwait.AddRange(keysToLookup.Select(key =>
                {
                    async Task RunAsync()
                    {
                        await Task.Yield();
                        try
                        {
                            Console.Error.WriteLine("  Firing request for " + key);
                            var (response, error) = await proto.Repo.GetRecordAsync(GetAtId(key.Did), collection, key.RKey, cancellationToken: ct);

                            if (response is not null)
                            {
                                var obj = (TValue)response.Value!;
                                onItemSuccess(key, obj);
                                Console.Error.WriteLine("     > Completed: " + key);
                                ctx.TriggerStateChange();
                            }
                            else
                            {
                                onItemFailure(key);
                                Console.Error.WriteLine("     > Failure: " + key);
                                ctx.TriggerStateChange();
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
            var author = options.Author != null ? WithRelationshipsLock(rels => rels.SerializeDid(options.Author)) : default;
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

                        if (options.Language != LanguageEnum.Unknown)
                            posts = posts.Where(x => x.Data!.Language == options.Language);

                        posts = posts
                            .Where(x => x.Data!.Text != null && IsMatch(x.Data.Text));
                        return posts;
                    })
                    .Select(x => 
                    {
                        x.InReplyToUser = x.IsReply ? rels.GetProfile(x.InReplyToPostId.Value.Author) : null;
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

        public async Task<PostsAndContinuation> GetUserPostsAsync(string did, bool includePosts, bool includeReplies, bool includeReposts, bool includeLikes, bool mediaOnly, string? continuation, RequestContext ctx)
        {
            Record[] postRecords = [];
            if (includePosts)
            {
                var results = (await proto.Repo.ListRecordsAsync(GetAtId(did), Post.RecordType)).HandleResult();
                postRecords = results!.Records!.ToArray();
            }
            var posts = WithRelationshipsLock(rels =>
            {

                foreach (var record in postRecords)
                {
                    var postId = rels.GetPostId(record.Uri!);
                    if (!rels.PostData.ContainsKey(postId))
                    {
                        rels.StorePostInfo(postId, (Post)record.Value!);
                    }
                }


                var p = postRecords.Select(x => rels.GetPost(x.Uri!));
                if (!includeReplies) p = p.Where(x => x.IsRootPost);
                return p.ToArray();
            });

            List<Record> repostRecords = [];
            if (includeReposts)
            {
                var result = (await proto.Repo.ListRecordsAsync(GetAtId(did), Repost.RecordType)).HandleResult();
                repostRecords = result!.Records!.ToList();
            }
            var reposts = WithRelationshipsLock(rels =>
            {
                var reposter = rels.GetProfile(rels.SerializeDid(did));
                return repostRecords.Select(repostRecord =>
                {
                    var reposted = (Repost)repostRecord.Value!;
                    var p = rels.GetPost(reposted.Subject!.Uri!);
                    p.RepostedBy = reposter;
                    p.RepostDate = reposted.CreatedAt!.Value;
                    return p;
                }).ToArray();
            });

            List<Record> likeRecords = [];
            if (includeLikes)
            {
                var results = (await proto.Repo.ListRecordsAsync(GetAtId(did), Like.RecordType)).HandleResult();
                likeRecords = results!.Records!.ToList();
            }
            var likes = WithRelationshipsLock(rels =>
            {
                return likeRecords.Select(likeRecord =>
                {
                    var liked = (Like)likeRecord.Value!;
                    var p = rels.GetPost(liked.Subject!.Uri!);
                    p.RepostDate = liked.CreatedAt!.Value;
                    return p;
                }).ToArray();
            });

            var allPosts = reposts.Concat(posts).Concat(likes).OrderByDescending(x => x.RepostDate ?? x.Date).ToArray();

            await EnrichAsync(allPosts, ctx);
            if (!includeReplies) allPosts = allPosts.Where(x =>  x.IsRootPost || x.RepostDate != null).ToArray();

            if (mediaOnly)
                allPosts = allPosts.Where(x => x.Data?.Media != null).ToArray();
            allPosts = allPosts.DistinctBy(x => (x.Author.Did, x.RKey)).ToArray();

            return (allPosts, null);
        }


        public async Task<PostsAndContinuation> GetPostThreadAsync(string did, string rkey, int limit, string? continuation, RequestContext ctx)
        {
            EnsureLimit(ref limit, 100);
            var thread = new List<BlueskyPost>();

            var focalPost = WithRelationshipsLock(rels => rels.GetPost(did, rkey));
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
            await EnrichAsync(thread.ToArray(), ctx);
            return new(thread.ToArray(), nextContinuation);
        }

        private Dictionary<(string Did, string RKey), (BlueskyFeedGeneratorData Info, DateTime DateCached)> FeedDomainCache = new();
        public async Task<(BlueskyPost[] Posts, BlueskyFeedGenerator Info, string? NextContinuation)> GetFeedAsync(string did, string rkey, string? continuation, RequestContext ctx)
        {
            var feedGenInfo = await GetFeedGeneratorAsync(did, rkey);
            if (!feedGenInfo.Data.ImplementationDid.StartsWith("did:web:", StringComparison.Ordinal)) throw new NotSupportedException();
            var domain = feedGenInfo.Data.ImplementationDid.Substring(8);

            var skeletonUrl = $"https://{domain}/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://{did}/app.bsky.feed.generator/{rkey}&limit=30";
            if (continuation != null)
                skeletonUrl += "&cursor=" + Uri.EscapeDataString(continuation);

            var postsJson = JsonConvert.DeserializeObject<AtFeedSkeletonResponse>(await DefaultHttpClient.GetStringAsync(skeletonUrl))!;
            var posts = WithRelationshipsLock(rels =>
            {
                return postsJson.feed.Select(x => rels.GetPost(new ATUri(x.post))).ToArray();
            });
            return (await EnrichAsync(posts, ctx), feedGenInfo, !string.IsNullOrEmpty(postsJson.cursor) ? postsJson.cursor : null);
        }


        private async Task<BlueskyFeedGeneratorData> GetFeedGeneratorDataAsync(string did, string rkey)
        {
            lock (FeedDomainCache)
            {
                if (FeedDomainCache.TryGetValue((did, rkey), out var z) && (DateTime.UtcNow - z.DateCached).TotalHours < 3)
                {
                    return z.Info;
                }
            }
            var recordOutput = (await proto.GetRecordAsync(GetAtId(did), Generator.RecordType, rkey)).HandleResult();
            var generator = (Generator)recordOutput!.Value!;
            var result = new BlueskyFeedGeneratorData
            {
                DisplayName = generator.DisplayName,
                ImplementationDid = generator.Did!.Handler,
                AvatarCid = generator.Avatar?.Ref?.Link?.ToArray(),
                Description = generator.Description,
            };
            lock (FeedDomainCache)
            {
                FeedDomainCache[(did, rkey)] = (result, DateTime.UtcNow);
            }
            return result;
        }

        public async Task<PostsAndContinuation> GetRecentPostsAsync(DateTime maxDate, bool includeReplies, string? continuation, RequestContext ctx)
        {
            var limit = 30;
            var maxPostIdExclusive = continuation != null ? PostIdTimeFirst.Deserialize(continuation) : new PostIdTimeFirst(Tid.FromDateTime(maxDate, 0), default);
            var posts = WithRelationshipsLock(rels =>
            {
                var enumerables = rels.PostData.slices.Select(slice =>
                {
                    return rels.GetRecentPosts(slice, maxPostIdExclusive); //.AssertOrderedAllowDuplicates(x => (PostIdTimeFirst)x.PostId, new DelegateComparer<PostIdTimeFirst>((a, b) => b.CompareTo(a)));
                })
                .Append(rels.PostData.QueuedItems.Where(x => x.Key.CompareTo(maxPostIdExclusive) < 0 && !rels.PostDeletions.ContainsKey(x.Key)).OrderByDescending(x => x.Key).Take(limit).Select(x => rels.GetPost((PostId)x.Key, BlueskyRelationships.DeserializePostData(x.Values.ToArray(), x.Key))))
                .ToArray();

                var merged = SimpleJoin.ConcatPresortedEnumerablesKeepOrdered(enumerables, x => (PostIdTimeFirst)x.PostId, new DelegateComparer<PostIdTimeFirst>((a, b) => b.CompareTo(a)));
                //  .AssertOrderedAllowDuplicates(x => (PostIdTimeFirst)x.PostId, new DelegateComparer<PostIdTimeFirst>((a, b) => b.CompareTo(a)));
                if (!includeReplies)
                    merged = merged.Where(x => x.IsRootPost);
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
            var profiles = WithRelationshipsLock(rels => rels.GetPostLikers(did, rkey, DeserializeRelationshipContinuation(continuation), limit + 1));
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
            var profiles = WithRelationshipsLock(rels => rels.GetPostReposts(did, rkey, DeserializeRelationshipContinuation(continuation), limit + 1));
            var nextContinuation = SerializeRelationshipContinuation(profiles, limit);
            SortByDescendingRelationshipRKey(ref profiles);
            //DeterministicShuffle(profiles, did + rkey);
            await EnrichAsync(profiles, ctx);
            return (profiles, nextContinuation);
        }

        public async Task<PostsAndContinuation> GetPostQuotesAsync(string did, string rkey, string? continuation, int limit, RequestContext ctx)
        {
            EnsureLimit(ref limit, 30);
            var posts = WithRelationshipsLock(rels => rels.GetPostQuotes(did, rkey, continuation != null ? PostId.Deserialize(continuation) : default, limit + 1));
            var nextContinuation = SerializeRelationshipContinuationPlcFirst(posts, limit);
            SortByDescendingRelationshipRKey(ref posts);
            //DeterministicShuffle(posts, did + rkey);
            await EnrichAsync(posts, ctx);
            return (posts, nextContinuation);
        }


        public async Task<ProfilesAndContinuation> GetFollowersAsync(string did, string? continuation, int limit, RequestContext ctx)
        {
            EnsureLimit(ref limit, 50);
            var profiles = WithRelationshipsLock(rels => rels.GetFollowers(did, DeserializeRelationshipContinuation(continuation), limit + 1));
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

        private static void EnsureLimit(ref int limit, int defaultLimit = 100)
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
            var profile = WithRelationshipsLock(rels => rels.GetFullProfile(did, ctx, followersYouFollowToLoad));
            await EnrichAsync([profile.Profile, ..profile.FollowedByPeopleYouFollow?.Take(followersYouFollowToLoad) ?? []], ctx);
            return profile;
        }
        public async Task<BlueskyProfile> GetProfileAsync(string did, RequestContext ctx)
        {
            var profile = WithRelationshipsLock(rels => rels.GetProfile(rels.SerializeDid(did)));
            await EnrichAsync([profile], ctx);
            return profile;
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
            return new BlueskyFeedGenerator
            {
                Did = did,
                RKey = rkey,
                Data = await GetFeedGeneratorDataAsync(did, rkey),
            };
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
                var key = (PostId: raw.Post?.PostId ?? default, raw.Kind);

                var c = coalescedList.TakeWhile(x => (x.LatestDate - raw.EventDate).TotalHours < 24).FirstOrDefault(x => (x.PostId, x.Kind) == key);
                if (c == null)
                {
                    c = new CoalescedNotification
                    {
                        PostId = key.PostId,
                        Kind = key.Kind,
                        LatestDate = raw.EventDate,
                        Post = raw.Post,
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

        internal static string? GetAvatarUrl(string did, string? avatarCid)
        {
            return avatarCid != null ? $"https://cdn.bsky.app/img/avatar_thumbnail/plain/{did}/{avatarCid}@jpeg" : null;
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


        


        public async Task<RepositoryImportEntry?> ImportCarIncrementalAsync(string did, RepositoryImportKind kind, bool startIfNotRunning = true, TimeSpan ignoreIfRecentlyRan = default, CancellationToken ct = default)
        {

            Plc plc = default;
            RepositoryImportEntry? previousImport = null;
            WithRelationshipsLock(rels =>
            {
                plc = rels.SerializeDid(did);
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

            var indexer = new Indexer(relationshipsUnlocked);
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
            WithRelationshipsLock(rels =>
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
            var sessionProtocol = await GetSessionProtocolAsync(ctx);
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
                var proto = rels.TryGetAppViewLiteProfile(session.LoggedInUser.Value);
                proto.PdsSessionCbor = SerializeAuthSession(authSession);
                ctx.Session.PdsSession = authSession.Session;
                rels.StoreAppViewLiteProfile(ctx.LoggedInUser, proto);
            });
            
            sessionProtocol = await GetSessionProtocolAsync(ctx);
            return await func(sessionProtocol);
        }

        public static byte[] SerializeAuthSession(AuthSession? authSession)
        {
            return CBORObject.FromJSONString(authSession.ToString()).EncodeToBytes();
        }
        public static AuthSession DeserializeAuthSession(byte[] bytes)
        {
            return AuthSession.FromString(CBORObject.DecodeFromBytes(bytes).ToJSONString());
        }

        public async Task<ATProtocol> GetSessionProtocolAsync(RequestContext ctx)
        {
            if (ctx.Session.IsReadOnlySimulation) throw new InvalidOperationException("Read only simulation.");
            var pdsSession = ctx.Session.PdsSession;
            var sessionProtocol = new ATProtocolBuilder().Build();
            await sessionProtocol.AuthenticateWithPasswordSessionAsync(new AuthSession(pdsSession));
            return sessionProtocol;

        }

        public async Task<Session> LoginToPdsAsync(string did, string password)
        {
            var sessionProtocol = new ATProtocolBuilder().Build();
            var session = (await sessionProtocol.AuthenticateWithPasswordResultAsync(did, password)).HandleResult();
            return session;
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
            var info = (await proto.Repo.GetRecordAsync(new ATDid(post.Did), Post.RecordType, post.RKey)).HandleResult();
            return (new StrongRef(info.Uri, info.Cid), (Post)info.Value);
            
        }

        internal Task<string> GetCidAsync(string did, string collection, Tid rkey)
        {
            return GetCidAsync(did, collection, rkey.ToString()!);
        }
        internal async Task<string> GetCidAsync(string did, string collection, string rkey)
        {
            return (await proto.Repo.GetRecordAsync(new ATDid(did), collection, rkey)).HandleResult()!.Cid!;
        }

        public async Task<BlueskyPost> GetPostAsync(string did, string rkey, RequestContext ctx)
        {
            var post = WithRelationshipsLock(rels => rels.GetPost(did, rkey));
            await EnrichAsync([post], ctx);
            return post;
        }


        private readonly Dictionary<(Plc, RepositoryImportKind), Task<RepositoryImportEntry>> carImports = new();
        private readonly static HttpClient DefaultHttpClient = new();
    }
}



