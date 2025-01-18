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

namespace AppViewLite
{
    public class BlueskyEnrichedApis
    {
        public static BlueskyEnrichedApis Instance;
        internal ATProtocol proto;
        internal ATProtocol protoAppView;
        private BlueskyRelationships relationshipsUnlocked;


        public BlueskyEnrichedApis(BlueskyRelationships relationships)
        {
            this.proto = new ATProtocolBuilder().WithInstanceUrl(new Uri("https://bsky.network")).Build();
            this.protoAppView = new ATProtocolBuilder().Build();
            this.relationshipsUnlocked = relationships;
        }

        public async Task<string> ResolveHandleAsync(string handle)
        {
            if (handle.StartsWith("did:", StringComparison.Ordinal)) return handle;
            var resolved = (await protoAppView.ResolveHandleAsync(new ATHandle(handle))).HandleResult();
            return resolved!.Did!.ToString();
        }
        public T WithRelationshipsLock<T>(Func<BlueskyRelationships, T> func)
        {
            BlueskyRelationships.VerifyNotEnumerable<T>();
            lock (relationshipsUnlocked)
            {
                return func(relationshipsUnlocked);
            }
        }
        public void WithRelationshipsLock(Action<BlueskyRelationships> func)
        {
            lock (relationshipsUnlocked)
            {
                func(relationshipsUnlocked);
            }
        }

        private ConcurrentDictionary<RelationshipStr, (Task Task, DateTime DateStarted)> pendingProfileRetrievals = new();
        private ConcurrentDictionary<RelationshipStr, (Task Task, DateTime DateStarted)> pendingPostRetrievals = new();

        public async Task<BlueskyProfile[]> EnrichAsync(BlueskyProfile[] profiles, RequestContext ctx, CancellationToken ct = default)
        {
            var toLookup = profiles.Where(x => x.BasicData == null).Select(x => new RelationshipStr(x.Did, "self")).Distinct().ToArray();
            if (toLookup.Length == 0) return profiles;

            toLookup = WithRelationshipsLock(rels => toLookup.Where(x => !rels.FailedProfileLookups.ContainsKey(rels.SerializeDid(x.Did))).ToArray());
            if (toLookup.Length == 0) return profiles;

            await LookupManyRecordsAsync<Profile>(toLookup, pendingProfileRetrievals, Profile.RecordType, ct,
                (key, profileRecord) => WithRelationshipsLock(rels => rels.StoreProfileBasicInfo(rels.SerializeDid(key.Did), profileRecord)),
                key => WithRelationshipsLock(rels => rels.FailedProfileLookups.Add(rels.SerializeDid(key.Did), DateTime.UtcNow)), ctx);


            WithRelationshipsLock(rels =>
            {
                foreach (var profile in profiles)
                {
                    if (profile.BasicData == null)
                        profile.BasicData = rels.GetProfileBasicInfo(rels.SerializeDid(profile.Did));
                }
            });


            return profiles;
        }

        public async Task<ProfilesAndContinuation> GetFollowingAsync(string did, string? continuation, int limit, RequestContext ctx)
        {
            EnsureLimit(ref limit);
            var response = (await proto.Repo.ListRecordsAsync(GetAtId(did), Follow.RecordType, limit: limit, cursor: continuation)).HandleResult();
            var following = WithRelationshipsLock(rels =>
            {
                return response!.Records!.Select(x => rels.GetProfile(rels.SerializeDid(((FishyFlip.Lexicon.App.Bsky.Graph.Follow)x.Value!).Subject!.Handler))).ToArray();
            });
            await EnrichAsync(following, ctx);
            return (following, response!.Cursor);
        }
        public async Task<BlueskyPost[]> EnrichAsync(BlueskyPost[] posts, RequestContext ctx, bool loadQuotes = true, CancellationToken ct = default)
        {
            var toLookup = posts.Where(x => x.Data == null).Select(x => new RelationshipStr(x.Author.Did, x.RKey)).ToArray();
            toLookup = WithRelationshipsLock(rels => toLookup.Where(x => !rels.FailedPostLookups.ContainsKey(rels.GetPostId(x.Did, x.RKey))).ToArray());
            await LookupManyRecordsAsync<Post>(toLookup, pendingPostRetrievals, Post.RecordType, ct,
                (key, postRecord) => WithRelationshipsLock(rels => rels.StorePostInfo(rels.GetPostId(key.Did, key.RKey), postRecord)), 
                key => WithRelationshipsLock(rels => rels.FailedPostLookups.Add(rels.GetPostId(key.Did, key.RKey), DateTime.UtcNow)), ctx);


            WithRelationshipsLock(rels =>
            {
                foreach (var post in posts)
                {
                    if (post.Data == null)
                    {
                        (post.Data, post.InReplyToUser) = rels.TryGetPostDataAndInReplyTo(rels.GetPostId(post.Author.Did, post.RKey));
                    }

                    if (loadQuotes && post.Data?.QuotedPlc != null)
                    {
                        post.QuotedPost = rels.GetPost(new PostId(new Plc(post.Data.QuotedPlc.Value), new Tid(post.Data.QuotedRKey!.Value)));
                    }

                }

            });

            WithRelationshipsLock(rels =>
            {
                foreach (var post in posts)
                {
                    if (post.Data?.InReplyToPlc != null && post.InReplyToUser == null)
                    {
                        post.InReplyToUser = rels.GetProfile(new Plc(post.Data.InReplyToPlc.Value));
                    }

                    if (ctx.IsLoggedIn)
                    {
                        var loggedInUser = ctx.LoggedInUser;
                        post.IsLiked = rels.Likes.HasActor(post.PostId, loggedInUser, out _);
                        post.IsReposted = rels.Reposts.HasActor(post.PostId, loggedInUser, out _);
                    }
                }
            });

            await EnrichAsync(posts.SelectMany(x => new[] { x.Author, x.InReplyToUser, x.RepostedBy }).Where(x => x != null).ToArray(), ctx);

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

        private Task LookupManyRecordsAsync<TValue>(IReadOnlyList<RelationshipStr> keys, ConcurrentDictionary<RelationshipStr, (Task Task, DateTime DateStarted)> pendingRetrievals, string collection, CancellationToken ct, Action<RelationshipStr, TValue> onItemSuccess, Action<RelationshipStr> onItemFailure, RequestContext ctx) where TValue : ATObject
        {
            //return Task.CompletedTask;
            var date = DateTime.UtcNow;

            var failedKeys = new List<RelationshipStr>();
            if (keys.Count != 0 && (ctx.DeadlineReached == null || !ctx.DeadlineReached.IsCompleted))
            {

                if (pendingRetrievals.Count >= 1000)
                {
                    foreach (var item in pendingRetrievals)
                    { 
                        if ((date - item.Value.DateStarted).TotalMinutes > 1)
                            pendingRetrievals.TryRemove(item.Key, out _);
                    }
                }
                

                var keysToLookup = new List<RelationshipStr>();
                var allTasksToAwait = new List<Task>();
                foreach (var key in keys)
                {
                    if (pendingRetrievals.TryGetValue(key, out var task))
                        allTasksToAwait.Add(task.Task);
                    else if (keysToLookup.Count + 1 < 50)
                        keysToLookup.Add(key);
                }

                Console.Error.WriteLine($"Looking up {keysToLookup.Count} keys for {collection} ({allTasksToAwait.Count} already running)");
                using var semaphore = new SemaphoreSlim(5);

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
                            }
                            else
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
                    pendingRetrievals[key] = (task, date);
                    return task;
                }));


                var fullTask = Task.WhenAll(allTasksToAwait);

                return ctx.DeadlineReached != null ? Task.WhenAny(fullTask, ctx.DeadlineReached) : fullTask;
            }
            return Task.CompletedTask;

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
            options = await InitializeSearchOptionsAsync(options);

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
                    var latest = await SearchLatestPostsAsync(options with { MinLikes = Math.Max(minLikes, options.MinLikes) }, limit: limit * 2, ctx: default, enrichOutput: false, alreadyProcessedPosts: searchSession.AlreadyProcessed);
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
            EnsureLimit(ref limit);
            options = await InitializeSearchOptionsAsync(options);
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
                    .SearchPosts(coreSearchTerms.ToArray(), options.Since != null ? (ApproximateDateTime32)options.Since : default, until != null ? ((ApproximateDateTime32)until).AddTicks(1) : null, author)
                    .DistinctAssumingOrderedInput(skipCheck: true)
                    .SelectMany(approxDate =>
                    {
                        var startPostId = new PostIdTimeFirst(Tid.FromDateTime(approxDate, 0), default);
                        var endPostId = new PostIdTimeFirst(Tid.FromDateTime(approxDate.AddTicks(1), 0), default);
                        var postsCore = rels.PostData.GetInRangeUnsorted(startPostId, endPostId)
                            .Where(x =>
                            {
                                if (alreadyProcessedPosts != null)
                                {
                                    if (!alreadyProcessedPosts.TryAdd(x.Key, new CachedSearchResult(null, -1))) // Will be overwritten later with actual post, if it matches
                                        return false;
                                }
                                var date = x.Key.PostRKey.Date;
                                if (date < options.Since) return false;
                                if (until != null && date >= until) return false;
                                if (continuationParsed != null)
                                {
                                    if (x.Key.CompareTo(continuationParsed.Value) >= 0) return false;
                                }
                                return true;
                            });
                        if (options.MinLikes > 0)
                        {
                            postsCore = postsCore.Where(x => rels.Likes.HasAtLeastActorCount(x.Key, options.MinLikes));
                        }
                        if (options.MinReposts > 0)
                        {
                            postsCore = postsCore.Where(x => rels.Reposts.HasAtLeastActorCount(x.Key, options.MinReposts));
                        }

                        var posts = postsCore
                            .Where(x => !rels.PostDeletions.ContainsKey(x.Key))
                            .Where(x => author != default ? x.Key.Author == author : true)
                            .Select(x => rels.GetPost(x.Key, BlueskyRelationships.DeserializePostData(x.Values.AsSmallSpan(), x.Key)))
                            .Where(x => x.Data?.Text != null && IsMatch(x.Data.Text));
                        return posts;
                    })
                    .Take(limit)
                    .ToArray();
            });
            if (enrichOutput)
                await EnrichAsync(posts, ctx);
            return (posts, posts.LastOrDefault()?.PostId.Serialize());
        }

        private async Task<PostSearchOptions> InitializeSearchOptionsAsync(PostSearchOptions options)
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


        public async Task<BlueskyPost[]> GetPostThreadAsync(string did, string rkey, RequestContext ctx)
        {
            var thread = new List<BlueskyPost>();

            var focalPost = WithRelationshipsLock(rels => rels.GetPost(did, rkey));
            thread.Add(focalPost);

            await EnrichAsync([focalPost], ctx);

            var loadedBefore = 0;
            while (thread[0].IsReply)
            {
                var p = thread[0];
                var prepend = WithRelationshipsLock(rels => rels.GetPost(p.InReplyToPostId!.Value));
                if (prepend.Data == null)
                {
                    if (loadedBefore++ < 3)
                        await EnrichAsync([prepend], ctx);
                    else
                        break;
                }
                thread.Insert(0, prepend);
                
            }
            var focalPostId = new PostId(new Plc(focalPost.Author.PlcId), Tid.Parse(focalPost.RKey));
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
            

            var otherReplies = WithRelationshipsLock(rels => rels.DirectReplies.GetDistinctValuesSorted(focalPostId).Where(x => x.Author != focalPostId.Author).Select(x => rels.GetPost(x)).ToArray());
            thread.AddRange(otherReplies.OrderByDescending(x => x.LikeCount).ThenByDescending(x => x.Date));
            await EnrichAsync([..opReplies, ..otherReplies], ctx);
            return thread.ToArray();
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
            EnsureLimit(ref limit);
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
            EnsureLimit(ref limit);
            var profiles = WithRelationshipsLock(rels => rels.GetPostReposts(did, rkey, DeserializeRelationshipContinuation(continuation), limit + 1));
            var nextContinuation = SerializeRelationshipContinuation(profiles, limit);
            SortByDescendingRelationshipRKey(ref profiles);
            //DeterministicShuffle(profiles, did + rkey);
            await EnrichAsync(profiles, ctx);
            return (profiles, nextContinuation);
        }

        public async Task<PostsAndContinuation> GetPostQuotesAsync(string did, string rkey, string? continuation, int limit, RequestContext ctx)
        {
            EnsureLimit(ref limit);
            var posts = WithRelationshipsLock(rels => rels.GetPostQuotes(did, rkey, continuation != null ? PostId.Deserialize(continuation) : default, limit + 1));
            var nextContinuation = SerializeRelationshipContinuationPlcFirst(posts, limit);
            SortByDescendingRelationshipRKey(ref posts);
            //DeterministicShuffle(posts, did + rkey);
            await EnrichAsync(posts, ctx);
            return (posts, nextContinuation);
        }


        public async Task<ProfilesAndContinuation> GetFollowersAsync(string did, string? continuation, int limit, RequestContext ctx)
        {
            EnsureLimit(ref limit);
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

        public async Task<BlueskyFullProfile> GetFullProfileAsync(string did, RequestContext ctx)
        {
            var profile = WithRelationshipsLock(rels => rels.GetFullProfile(did));
            await EnrichAsync([profile.Profile], ctx);
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

        internal static string? GetAvatarUrl(string did, string? avatarCid)
        {
            return avatarCid != null ? $"https://cdn.bsky.app/img/avatar_thumbnail/plain/{did}/{avatarCid}@jpeg" : null;
        }

        private readonly static HttpClient DefaultHttpClient = new();
    }
}



