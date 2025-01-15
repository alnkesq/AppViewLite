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

        public async Task<BlueskyProfile[]> EnrichAsync(BlueskyProfile[] profiles, EnrichDeadlineToken deadline, CancellationToken ct = default)
        {
            var toLookup = profiles.Where(x => x.BasicData == null).Select(x => new RelationshipStr(x.Did, "self")).Distinct().ToArray();
            if (toLookup.Length == 0) return profiles;

            toLookup = WithRelationshipsLock(rels => toLookup.Where(x => !rels.FailedProfileLookups.ContainsKey(rels.SerializeDid(x.Did))).ToArray());
            if (toLookup.Length == 0) return profiles;

            await LookupManyRecordsAsync<Profile>(toLookup, pendingProfileRetrievals, Profile.RecordType, ct,
                (key, profileRecord) => WithRelationshipsLock(rels => rels.StoreProfileBasicInfo(rels.SerializeDid(key.Did), profileRecord)),
                key => WithRelationshipsLock(rels => rels.FailedProfileLookups.Add(rels.SerializeDid(key.Did), DateTime.UtcNow)), deadline);


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

        public async Task<(BlueskyProfile[] Profiles, string? NextContinuation)> GetFollowingAsync(string did, string? continuation, int limit, EnrichDeadlineToken deadline)
        {
            EnsureLimit(ref limit);
            var response = (await proto.Repo.ListRecordsAsync(GetAtId(did), Follow.RecordType, limit: limit, cursor: continuation)).HandleResult();
            var following = WithRelationshipsLock(rels =>
            {
                return response!.Records!.Select(x => rels.GetProfile(rels.SerializeDid(((FishyFlip.Lexicon.App.Bsky.Graph.Follow)x.Value!).Subject!.Handler))).ToArray();
            });
            await EnrichAsync(following, deadline);
            return (following, response!.Cursor);
        }
        public async Task<BlueskyPost[]> EnrichAsync(BlueskyPost[] posts, EnrichDeadlineToken deadline, bool loadQuotes = true, CancellationToken ct = default)
        {
            var toLookup = posts.Where(x => x.Data == null).Select(x => new RelationshipStr(x.Author.Did, x.RKey)).ToArray();
            toLookup = WithRelationshipsLock(rels => toLookup.Where(x => !rels.FailedPostLookups.ContainsKey(rels.GetPostId(x.Did, x.RKey))).ToArray());
            await LookupManyRecordsAsync<Post>(toLookup, pendingPostRetrievals, Post.RecordType, ct,
                (key, postRecord) => WithRelationshipsLock(rels => rels.StorePostInfo(rels.GetPostId(key.Did, key.RKey), postRecord)), 
                key => WithRelationshipsLock(rels => rels.FailedPostLookups.Add(rels.GetPostId(key.Did, key.RKey), DateTime.UtcNow)), deadline);


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
                }
            });

            await EnrichAsync(posts.SelectMany(x => new[] { x.Author, x.InReplyToUser, x.RepostedBy }).Where(x => x != null).ToArray(), deadline);

            if (loadQuotes)
            {
                var r = posts.Select(x => x.QuotedPost).Where(x => x != null).ToArray();
                if (r.Length != 0)
                {
                    await EnrichAsync(r, deadline, loadQuotes: false, ct: ct);
                }
            }
            return posts;
        }

        private Task LookupManyRecordsAsync<TValue>(IReadOnlyList<RelationshipStr> keys, ConcurrentDictionary<RelationshipStr, (Task Task, DateTime DateStarted)> pendingRetrievals, string collection, CancellationToken ct, Action<RelationshipStr, TValue> onItemSuccess, Action<RelationshipStr> onItemFailure, EnrichDeadlineToken deadline) where TValue : ATObject
        {
            //return Task.CompletedTask;
            var date = DateTime.UtcNow;

            var failedKeys = new List<RelationshipStr>();
            if (keys.Count != 0 && (deadline == default || !deadline.DeadlineReached.IsCompleted))
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

                return deadline != default ? Task.WhenAny(fullTask, deadline.DeadlineReached) : fullTask;
            }
            return Task.CompletedTask;

        }

        private static ATIdentifier GetAtId(string did)
        {
            return ATIdentifier.Create(did)!;
        }

        public async Task<(BlueskyPost[] Posts, string? NextContinuation)> SearchAsync(string query, DateTime? since = null, DateTime? until = null, string? authorDid = null, string? continuation = null, EnrichDeadlineToken deadline = default)
        {
            authorDid = !string.IsNullOrEmpty(authorDid) ? await this.ResolveHandleAsync(authorDid) : null;
            var author = authorDid != null ? WithRelationshipsLock(rels => rels.SerializeDid(authorDid)) : default;
            var queryWords = StringUtils.GetDistinctWords(query);
            if (queryWords.Length == 0) return ([], null);
            var queryPhrases = StringUtils.GetExactPhrases(query);
            var tags = Regex.Matches(query, @"#\w+\b").Select(x => x.Value.Substring(1).ToLowerInvariant()).ToArray();
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
            var coreSearchTerms = queryWords.Select(x => x.ToString()).Where(x => !tags.Contains(x)).Concat(tags.Select(x => "#" + x)).ToArray();
            var posts = WithRelationshipsLock(rels =>
            {
                
                return rels
                    .SearchPosts(coreSearchTerms, since != null ? (ApproximateDateTime32)since : default, until != null ? ((ApproximateDateTime32)until).AddTicks(1) : null, author)
                    .DistinctAssumingOrderedInput(skipCheck: true)
                    .SelectMany(approxDate =>
                    {
                        var startPostId = new PostIdTimeFirst(Tid.FromDateTime(approxDate, 0), default);
                        var endPostId = new PostIdTimeFirst(Tid.FromDateTime(approxDate.AddTicks(1), 0), default);
                        var posts = rels.PostData.GetInRangeUnsorted(startPostId, endPostId)
                            .Where(x =>
                            {
                                var date = x.Key.PostRKey.Date;
                                if (date < since) return false;
                                if (until != null && date >= until) return false;
                                if (continuationParsed != null)
                                {
                                    if (x.Key.CompareTo(continuationParsed.Value) >= 0) return false;
                                }
                                return true;
                            })
                            .Where(x => !rels.PostDeletions.ContainsKey(x.Key))
                            .Where(x => author != default ? x.Key.Author == author : true)
                            .Select(x => rels.GetPost(x.Key, rels.DeserializePostData(x.Values.AsSpan(), x.Key)))
                            .Where(x => x.Data?.Text != null && IsMatch(x.Data.Text));
                        return posts;
                    })
                    .Take(100)
                    .ToArray();
            });
            await EnrichAsync(posts, deadline);
            return (posts, posts.LastOrDefault()?.PostId.Serialize());
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

        public async Task<(BlueskyPost[] Posts, string? NextContinuation)> GetUserPostsAsync(string did, bool includePosts, bool includeReplies, bool includeReposts, bool includeLikes, bool mediaOnly, string? continuation, EnrichDeadlineToken deadline)
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

            await EnrichAsync(allPosts, deadline);
            if (!includeReplies) allPosts = allPosts.Where(x =>  x.IsRootPost || x.RepostDate != null).ToArray();

            if (mediaOnly)
                allPosts = allPosts.Where(x => x.Data?.Media != null).ToArray();
            allPosts = allPosts.DistinctBy(x => (x.Author.Did, x.RKey)).ToArray();

            return (allPosts, null);
        }


        public async Task<BlueskyPost[]> GetPostThreadAsync(string did, string rkey, EnrichDeadlineToken deadline)
        {
            var thread = new List<BlueskyPost>();

            var focalPost = WithRelationshipsLock(rels => rels.GetPost(did, rkey));
            thread.Add(focalPost);

            await EnrichAsync([focalPost], deadline);

            var loadedBefore = 0;
            while (thread[0].IsReply)
            {
                var p = thread[0];
                var prepend = WithRelationshipsLock(rels => rels.GetPost(p.InReplyToPostId!.Value));
                if (prepend.Data == null)
                {
                    if (loadedBefore++ < 3)
                        await EnrichAsync([prepend], deadline);
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
            await EnrichAsync([..opReplies, ..otherReplies], deadline);
            return thread.ToArray();
        }

        private Dictionary<(string Did, string RKey), (BlueskyFeedGeneratorData Info, DateTime DateCached)> FeedDomainCache = new();
        public async Task<(BlueskyPost[] Posts, BlueskyFeedGenerator Info, string? NextContinuation)> GetFeedAsync(string did, string rkey, string? continuation, EnrichDeadlineToken deadline)
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
            return (await EnrichAsync(posts, deadline), feedGenInfo, !string.IsNullOrEmpty(postsJson.cursor) ? postsJson.cursor : null);
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

        public async Task<(BlueskyPost[] Posts, string? NextContinuation)> GetRecentPostsAsync(DateTime maxDate, bool includeReplies, string? continuation, EnrichDeadlineToken deadline)
        {
            var limit = 30;
            var maxPostIdExclusive = continuation != null ? PostIdTimeFirst.Deserialize(continuation) : new PostIdTimeFirst(Tid.FromDateTime(maxDate, 0), default);
            var posts = WithRelationshipsLock(rels =>
            {
                var enumerables = rels.PostData.slices.Select(slice =>
                {
                    return rels.GetRecentPosts(slice, maxPostIdExclusive); //.AssertOrderedAllowDuplicates(x => (PostIdTimeFirst)x.PostId, new DelegateComparer<PostIdTimeFirst>((a, b) => b.CompareTo(a)));
                })
                .Append(rels.PostData.QueuedItems.Where(x => x.Key.CompareTo(maxPostIdExclusive) < 0 && !rels.PostDeletions.ContainsKey(x.Key)).OrderByDescending(x => x.Key).Take(limit).Select(x => rels.GetPost((PostId)x.Key, rels.DeserializePostData(x.Values.ToArray(), x.Key))))
                .ToArray();

                var merged = SimpleJoin.ConcatPresortedEnumerablesKeepOrdered(enumerables, x => (PostIdTimeFirst)x.PostId, new DelegateComparer<PostIdTimeFirst>((a, b) => b.CompareTo(a)));
                //  .AssertOrderedAllowDuplicates(x => (PostIdTimeFirst)x.PostId, new DelegateComparer<PostIdTimeFirst>((a, b) => b.CompareTo(a)));
                if (!includeReplies)
                    merged = merged.Where(x => x.IsRootPost);
                return merged
                    .Take(limit)
                    .ToArray();
            });
            await EnrichAsync(posts, deadline);
            return (posts, posts.LastOrDefault()?.PostId.Serialize());
        }

        public async Task<(BlueskyProfile[] Profiles, string? NextContinuation)> GetPostLikersAsync(string did, string rkey, string? continuation, int limit, EnrichDeadlineToken deadline)
        {
            EnsureLimit(ref limit);
            var profiles = WithRelationshipsLock(rels => rels.GetPostLikers(did, rkey, DeserializeRelationshipContinuation(continuation), limit + 1));
            var nextContinuation = SerializeRelationshipContinuation(profiles, limit);
            DeterministicShuffle(profiles, did + rkey);
            await EnrichAsync(profiles, deadline);
            return (profiles, nextContinuation);
        }

        public async Task<(BlueskyProfile[] Profiles, string? NextContinuation)> GetPostRepostersAsync(string did, string rkey, string? continuation, int limit, EnrichDeadlineToken deadline)
        {
            EnsureLimit(ref limit);
            var profiles = WithRelationshipsLock(rels => rels.GetPostReposts(did, rkey, DeserializeRelationshipContinuation(continuation), limit + 1));
            var nextContinuation = SerializeRelationshipContinuation(profiles, limit);
            DeterministicShuffle(profiles, did + rkey);
            await EnrichAsync(profiles, deadline);
            return (profiles, nextContinuation);
        }

        public async Task<(BlueskyProfile[] Profiles, string? NextContinuation)> GetFollowersAsync(string did, string? continuation, int limit, EnrichDeadlineToken deadline)
        {
            EnsureLimit(ref limit);
            var profiles = WithRelationshipsLock(rels => rels.GetFollowers(did, DeserializeRelationshipContinuation(continuation), limit + 1));
            var nextContinuation = SerializeRelationshipContinuation(profiles, limit);
            DeterministicShuffle(profiles, did);
            await EnrichAsync(profiles, deadline);
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

        private static string? SerializeRelationshipContinuation(BlueskyProfile[] actors, int limit)
        {
            if (actors.Length == 0) return null;
            if (actors.Length <= limit) return null; // we request limit + 1
            var last = actors[^1];
            var relationship = new Models.Relationship(new Plc(last.PlcId), last.RelationshipRKey!.Value);
            return relationship.Serialize();
        }

        private static Models.Relationship DeserializeRelationshipContinuation(string? continuation)
        {
            return continuation != null ? Models.Relationship.Deserialize(continuation) : default;
        }

        public async Task<BlueskyFullProfile> GetFullProfileAsync(string did, EnrichDeadlineToken deadline)
        {
            var profile = WithRelationshipsLock(rels => rels.GetFullProfile(did));
            await EnrichAsync([profile.Profile], deadline);
            return profile;
        }
        public async Task<BlueskyProfile> GetProfileAsync(string did, EnrichDeadlineToken deadline)
        {
            var profile = WithRelationshipsLock(rels => rels.GetProfile(rels.SerializeDid(did)));
            await EnrichAsync([profile], deadline);
            return profile;
        }
        public async Task PopulateFullInReplyToAsync(BlueskyPost[] posts, EnrichDeadlineToken deadline)
        {
            WithRelationshipsLock(rels =>
            {
                foreach (var post in posts)
                {
                    if (post.IsReply)
                    {
                        post.InReplyToFullPost = rels.GetPost(post.InReplyToPostId!.Value);
                        //post.RootFullPost = rels.GetPost(post.Data.InReplyToRKey); // TODO: https://github.com/alnkesq/AppViewLite/issues/15
                    }
                }
            });
            await EnrichAsync([.. posts.Select(x => x.InReplyToFullPost).Where(x => x != null)!, .. posts.Select(x => x.RootFullPost).Where(x => x != null)!], deadline);
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



