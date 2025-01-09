using FishyFlip.Models;
using FishyFlip;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using FishyFlip.Lexicon;
using FishyFlip.Lexicon.App.Bsky.Feed;
using FishyFlip.Lexicon.App.Bsky.Graph;
using FishyFlip.Lexicon.App.Bsky.Actor;
using FishyFlip.Lexicon.Com.Atproto.Repo;
using System.Net.Http;
using Newtonsoft.Json;
using Ipfs;
using FishyFlip.Lexicon.Com.Atproto.Identity;
using AppViewLite.Storage;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.RegularExpressions;
using AppViewLite.Models;
using AppViewLite.Numerics;

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
            var resolved = await protoAppView.ResolveHandleAsync(new ATHandle(handle));
            return resolved.AsT0.Did.ToString();
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

            await LookupManyRecordsAsync<Profile>(toLookup, pendingProfileRetrievals, "app.bsky.actor.profile", ct,
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

        public async Task<BlueskyProfile[]> GetFollowing(string did, EnrichDeadlineToken deadline)
        {
            var records = (await proto.Repo.ListRecordsAsync(GetAtId(did), "app.bsky.graph.follow")).AsT0.Records;
            var following = WithRelationshipsLock(rels =>
            {
                return records.Select(x => rels.GetProfile(rels.SerializeDid(((Follow)x.Value).Subject.Handler))).ToArray();
            });
            await EnrichAsync(following, deadline);
            return following;
        }
        public async Task<BlueskyPost[]> EnrichAsync(BlueskyPost[] posts, EnrichDeadlineToken deadline, bool loadQuotes = true, CancellationToken ct = default)
        {
            var toLookup = posts.Where(x => x.Data == null).Select(x => new RelationshipStr(x.Author.Did, x.RKey)).ToArray();
            toLookup = WithRelationshipsLock(rels => toLookup.Where(x => !rels.FailedPostLookups.ContainsKey(rels.GetPostId(x.Did, x.RKey))).ToArray());
            await LookupManyRecordsAsync<Post>(toLookup, pendingPostRetrievals, "app.bsky.feed.post", ct,
                (key, postRecord) => WithRelationshipsLock(rels => rels.StorePostInfo(rels.GetPostId(key.Did, key.RKey), postRecord)), 
                key => WithRelationshipsLock(rels => rels.FailedPostLookups.Add(rels.GetPostId(key.Did, key.RKey), DateTime.UtcNow)), deadline);


            WithRelationshipsLock(rels =>
            {
                foreach (var post in posts)
                {
                    if (post.Data == null)
                    {
                        post.Data = rels.TryGetPostData(rels.GetPostId(post.Author.Did, post.RKey));
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
                    if (post.Data?.InReplyToPlc != null)
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
                            var response = await proto.Repo.GetRecordAsync(GetAtId(key.Did), collection, key.RKey, cancellationToken: ct);

                            if (response.IsT0)
                            {
                                var obj = (TValue)response.AsT0.Value!;
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

        public async Task<BlueskyPost[]> SearchAsync(string query, DateTime? since = null, DateTime? until = null, string? authorDid = null, EnrichDeadlineToken deadline = default)
        {
            authorDid = !string.IsNullOrEmpty(authorDid) ? await this.ResolveHandleAsync(authorDid) : null;
            var author = authorDid != null ? WithRelationshipsLock(rels => rels.SerializeDid(authorDid)) : default;
            var queryWords = StringUtils.GetWords(query);
            if (queryWords.Length == 0) return [];
            var tags = Regex.Matches(query, @"#\w+\b").Select(x => x.Value.Substring(1).ToLowerInvariant()).ToArray();
            Regex[] hashtagRegexes = tags.Select(x => new Regex("#" + Regex.Escape(x) + "\\b", RegexOptions.IgnoreCase)).ToArray();

            bool IsMatch(string postText)
            {
                var postWords = StringUtils.GetWords(postText);
                if (queryWords.Any(x => !postWords.Contains(x))) return false;
                if (hashtagRegexes.Any(r => !r.IsMatch(postText))) return false;
                return true;
            }
            var coreSearchTerms = queryWords.Select(x => x.ToString()).Where(x => !tags.Contains(x)).Concat(tags.Select(x => "#" + x)).ToArray();
            var posts = WithRelationshipsLock(rels =>
            {
                
                return rels
                    .SearchPosts(coreSearchTerms, since != null ? (ApproximateDateTime32)since : default, until != null ? ((ApproximateDateTime32)until).AddTicks(1) : null, author)
                    .DistinctAssumingOrderedInput(skipCheck: true)
                    .Take(100)
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
                                return true;
                            })
                            .Where(x => !rels.PostDeletions.ContainsKey(x.Key))
                            .Where(x => author != default ? x.Key.Author == author : true)
                            .Select(x => rels.GetPost(x.Key, rels.DeserializePostData(x.Values.AsSpan())))
                            .Where(x => x.Data?.Text != null && IsMatch(x.Data.Text));
                        return posts;
                    })
                    .Take(100)
                    .ToArray();
            });
            await EnrichAsync(posts, deadline);
            return posts;
        }

        public async Task<BlueskyPost[]> GetUserPostsAsync(string did, bool includePosts, bool includeReplies, bool includeReposts, bool includeLikes, bool mediaOnly, EnrichDeadlineToken deadline)
        {

            var postRecords = includePosts ? (await proto.Repo.ListRecordsAsync(GetAtId(did), "app.bsky.feed.post")).AsT0!.Records!.ToArray() : [];
            var posts = WithRelationshipsLock(rels =>
            {

                foreach (var record in postRecords)
                {
                    var postId = rels.GetPostId(record.Uri!);
                    if (!rels.PostData.ContainsKey(postId))
                    {
                        rels.StorePostInfo(postId, (Post)record.Value);
                    }
                }


                var p = postRecords.Select(x => rels.GetPost(x.Uri));
                if (!includeReplies) p = p.Where(x => x.Data?.InReplyToPlc == null);
                return p.ToArray();
            });



            var repostRecords = includeReposts ? (await proto.Repo.ListRecordsAsync(GetAtId(did), "app.bsky.feed.repost")).AsT0!.Records! : [];
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


            var likeRecords = includeLikes ? (await proto.Repo.ListRecordsAsync(GetAtId(did), "app.bsky.feed.like")).AsT0!.Records! : [];
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
            if (!includeReplies) allPosts = allPosts.Where(x =>  x.Data?.InReplyToPlc == null || x.RepostDate != null).ToArray();

            if (mediaOnly)
                allPosts = allPosts.Where(x => x.Data?.Media != null).ToArray();
            allPosts = allPosts.DistinctBy(x => (x.Author.Did, x.RKey)).ToArray();

            return allPosts;
        }


        public async Task<BlueskyPost[]> GetPostThreadAsync(string did, string rkey, EnrichDeadlineToken deadline)
        {
            var thread = new List<BlueskyPost>();

            var focalPost = WithRelationshipsLock(rels => rels.GetPost(did, rkey));
            thread.Add(focalPost);

            await EnrichAsync([focalPost], deadline);

            var loadedBefore = 0;
            while (thread[0].Data?.InReplyToPlc != null)
            {
                var p = thread[0];
                var prepend = WithRelationshipsLock(rels => rels.GetPost(new PostId(new Plc(p.Data.InReplyToPlc.Value), new Tid(p.Data.InReplyToRKey.Value))));
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
            

            var replies = WithRelationshipsLock(rels => rels.DirectReplies.GetDistinctValuesSorted(focalPostId).Where(x => x.Author != focalPostId.Author).Select(x => rels.GetPost(x)).ToArray());
            thread.AddRange(replies.OrderByDescending(x => x.LikeCount).ThenByDescending(x => x.Date));
            await EnrichAsync(replies, deadline);
            return thread.ToArray();
        }

        private Dictionary<(string Did, string RKey), (string ImplementationDid, string DisplayName, DateTime DateCached)> FeedDomainCache = new();
        public async Task<(BlueskyPost[], string DisplayName)> GetFeedAsync(string did, string rkey, EnrichDeadlineToken deadline)
        {
            var eedGenInfo = await GetFeedGeneratorInfoAsync(did, rkey);
            if (!eedGenInfo.ImplementationDid.StartsWith("did:web:", StringComparison.Ordinal)) throw new NotSupportedException();
            var domain = eedGenInfo.ImplementationDid.Substring(8);

            var skeletonUrl = $"https://{domain}/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://{did}/app.bsky.feed.generator/{rkey}&limit=30";

            var postsJson = JsonConvert.DeserializeObject<AtFeedSkeletonResponse>(await DefaultHttpClient.GetStringAsync(skeletonUrl))!;
            var posts = WithRelationshipsLock(rels =>
            {
                return postsJson.feed.Select(x => rels.GetPost(new ATUri(x.post))).ToArray();
            });
            return (await EnrichAsync(posts, deadline), eedGenInfo.DisplayName);
        }


        private async Task<(string ImplementationDid, string DisplayName)> GetFeedGeneratorInfoAsync(string did, string rkey)
        {
            lock (FeedDomainCache)
            {
                if (FeedDomainCache.TryGetValue((did, rkey), out var z) && (DateTime.UtcNow - z.DateCached).TotalHours < 3)
                {
                    return (z.ImplementationDid, z.DisplayName);
                }
            }
            var generator = (Generator)(await proto.GetRecordAsync(GetAtId(did), "app.bsky.feed.generator", rkey)).AsT0.Value;
            var feedGenDid = generator.Did.Handler;
            var displayName = generator.DisplayName;
            lock (FeedDomainCache)
            {
                FeedDomainCache[(did, rkey)] = (feedGenDid, displayName, DateTime.UtcNow);
            }
            return (feedGenDid, displayName);
        }

        private readonly static HttpClient DefaultHttpClient = new();
    }
}



