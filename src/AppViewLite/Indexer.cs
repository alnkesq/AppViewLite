using AppBsky.Actor;
using AppBsky.Feed;
using AppBsky.Graph;
using AppViewLite.Models;
using AppViewLite.Numerics;
using AppViewLite.PluggableProtocols;
using CarpaNet;
using CarpaNet.Jetstream;
using CarpaNet.Repo;
using ComAtproto.Label;
using ComAtproto.Sync;
using DuckDbSharp;
using DuckDbSharp.Types;
using Google.Protobuf;
using Microsoft.Extensions.Logging;
using AppViewLite;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Relationship = AppViewLite.Models.Relationship;

namespace AppViewLite
{
    public class Indexer : BlueskyRelationshipsClientBase
    {
        public FirehoseUrlWithFallbacks FirehoseUrl = new([new Uri("https://bsky.network/")]);
        public BlueskyEnrichedApis Apis;
        public Indexer(BlueskyEnrichedApis apis)
            : base(apis.primarySecondaryPair)
        {
            this.Apis = apis;
        }
        public void OnRecordDeleted(string commitAuthor, CollectionAndRKey path, bool ignoreIfDisposing = false, RequestContext? ctx = null)
        {
            if (Apis.AdministrativeBlocklist.ShouldBlockIngestion(commitAuthor)) return;

            var collection = path.Collection;
            var deletionDate = DateTime.UtcNow;
            ctx ??= RequestContext.CreateForFirehose("Delete:" + collection, allowStale: true /* only temporarily, will be disabled in a moment*/);

            var rkeyAsTid = Tid.TryParse(path.RKey, out var parsedTid) ? parsedTid : default;


            var preresolved = WithRelationshipsLock(rels =>
            {
                if (ignoreIfDisposing && rels.IsDisposed) return default;

                var commitPlc = rels.TrySerializeDidMaybeReadOnly(commitAuthor, ctx);
                if (commitPlc == default) return default;


                PostIdTimeFirst postLikeOrRepostTarget = default;
                Plc followTarget = default;
                if (rkeyAsTid != default)
                {
                    if (collection == Like.RecordType)
                    {
                        postLikeOrRepostTarget = rels.Likes.TryGetTarget(new Relationship(commitPlc, rkeyAsTid));
                    }
                    if (collection == Repost.RecordType)
                    {
                        postLikeOrRepostTarget = rels.Reposts.TryGetTarget(new Relationship(commitPlc, rkeyAsTid));
                    }
                    if (collection == Follow.RecordType)
                    {
                        followTarget = rels.Follows.TryGetTarget(new Relationship(commitPlc, rkeyAsTid));
                    }
                }
                return (commitPlc, postLikeOrRepostTarget, followTarget);
            }, ctx);

            ctx.AllowStale = false;

            WithRelationshipsWriteLock((Action<BlueskyRelationships>)(relationships =>
            {
                if (ignoreIfDisposing && relationships.IsDisposed) return;
                relationships.EnsureNotDisposed();
                var commitPlc = preresolved.commitPlc != default ? preresolved.commitPlc : relationships.SerializeDid(commitAuthor, ctx);

                if (collection == Generator.RecordType)
                {
                    relationships.FeedGeneratorDeletions.Add(new RelationshipHashedRKey(commitPlc, path.RKey), deletionDate);
                }
                else
                {

                    if (rkeyAsTid == default) return;

                    var rel = new Relationship(commitPlc, rkeyAsTid);
                    if (collection == Like.RecordType)
                    {
                        var target = relationships.Likes.Delete(rel, deletionDate, preresolved.postLikeOrRepostTarget);
                        if (target != null) relationships.NotifyPostStatsChange(target.Value, commitPlc);
                    }
                    else if (collection == Follow.RecordType)
                    {
                        var unfollowedUser = relationships.Follows.Delete(rel, deletionDate, preresolved.followTarget);
                        if (unfollowedUser != null)
                            relationships.AddNotification(unfollowedUser.Value, NotificationKind.UnfollowedYou, commitPlc, ctx, deletionDate);
                    }
                    else if (collection == Block.RecordType)
                    {
                        relationships.Blocks.Delete(rel, deletionDate);
                    }
                    else if (collection == Repost.RecordType)
                    {
                        var target = relationships.Reposts.Delete(rel, deletionDate, preresolved.postLikeOrRepostTarget);
                        if (target != null) relationships.NotifyPostStatsChange(target.Value, commitPlc);
                    }
                    else if (collection == Post.RecordType)
                    {
                        relationships.PostDeletions.Add(new PostId(rel.Actor, rel.RelationshipRKey), deletionDate);
                    }
                    else if (collection == Listitem.RecordType)
                    {
                        relationships.ListItemDeletions.Add(new Relationship(rel.Actor, rel.RelationshipRKey), deletionDate);
                    }
                    else if (collection == List.RecordType)
                    {
                        relationships.ListDeletions.Add(new Relationship(rel.Actor, rel.RelationshipRKey), deletionDate);
                    }
                    else if (collection == Threadgate.RecordType)
                    {
                        relationships.Threadgates.AddRange(new PostId(commitPlc, rel.RelationshipRKey), relationships.SerializeThreadgateToBytes(new Threadgate { CreatedAt = default, Post = default }, ctx, out _));
                    }
                    else if (collection == Postgate.RecordType)
                    {
                        relationships.Postgates.AddRange(new PostId(commitPlc, rel.RelationshipRKey), relationships.SerializePostgateToBytes(new Postgate { CreatedAt = default, Post = default }, ctx, out _));
                    }
                    else if (collection == Listblock.RecordType)
                    {
                        relationships.ListBlockDeletions.Add(new Relationship(rel.Actor, rel.RelationshipRKey), deletionDate);
                    }
                    //else LogInfo("Deletion of unknown object type: " + collection);
                }
            }), ctx);
        }





        private static bool HasNumericRKey(CollectionAndRKey path)
        {
            // Some spam bots?
            // Avoid noisy exceptions.
            var rkey = path.RKey;
            return long.TryParse(rkey, out _) || rkey.StartsWith("follow_", StringComparison.Ordinal);
        }




        public void OnRecordCreated(string commitAuthor, CollectionAndRKey path, JsonElement recordJson, bool ignoreIfDisposing = false, RequestContext? ctx = null, bool isRepositoryImport = false)
        {
            if (Apis.AdministrativeBlocklist.ShouldBlockIngestion(commitAuthor)) return;
            var now = DateTime.UtcNow;

            ContinueOutsideLock? continueOutsideLock = null;
            var recordType = recordJson.AtProtoTypeId;
            ctx ??= RequestContext.CreateForFirehose("Create:" + recordType, allowStale: true);


            object? record =
                recordType switch
                {
                    Like.RecordType => Like.FromJson(recordJson),
                    Repost.RecordType => Repost.FromJson(recordJson),
                    Follow.RecordType => Follow.FromJson(recordJson),
                    Block.RecordType => Block.FromJson(recordJson),
                    Post.RecordType => Post.FromJson(recordJson),
                    Profile.RecordType => Profile.FromJson(recordJson),
                    Threadgate.RecordType => Threadgate.FromJson(recordJson),
                    Postgate.RecordType => Postgate.FromJson(recordJson),
                    Listblock.RecordType => Listblock.FromJson(recordJson),
                    Listitem.RecordType => Listitem.FromJson(recordJson),
                    List.RecordType => List.FromJson(recordJson),
                    Generator.RecordType => Generator.FromJson(recordJson),
                    Link.RecordType => Link.FromJson(recordJson),
                    _ => null,
                };

            var preresolved = WithRelationshipsLock(rels =>
            {
                if (ignoreIfDisposing && rels.IsDisposed) return default;

                var commitPlc = rels.TrySerializeDidMaybeReadOnly(commitAuthor, ctx);
                if (commitPlc == default) return default;

                Plc subjectPlc = default;
                Plc viaPlc = default;
                bool relationshipIsAbsent = false;

                if (record is Like like && like.Subject!.Uri.Collection == Post.RecordType)
                {
                    subjectPlc = rels.TrySerializeDidMaybeReadOnly(like.Subject!.Uri.DidAsString!, ctx);
                    viaPlc = like.Via?.Uri.DidAsString is { } viaDid ? rels.TrySerializeDidMaybeReadOnly(viaDid, ctx) : default;
                    if (subjectPlc != default && !rels.Likes.HasActor(new PostIdTimeFirst(Tid.Parse(like.Subject.Uri.Rkey), subjectPlc), commitPlc, out _))
                        relationshipIsAbsent = true;
                }
                else if (record is Repost repost && repost.Subject!.Uri.Collection == Post.RecordType)
                {
                    subjectPlc = rels.TrySerializeDidMaybeReadOnly(repost.Subject!.Uri.DidAsString!, ctx);
                    viaPlc = repost.Via?.Uri.DidAsString is { } viaDid ? rels.TrySerializeDidMaybeReadOnly(viaDid, ctx) : default;
                    if (subjectPlc != default && !rels.Reposts.HasActor(new PostIdTimeFirst(Tid.Parse(repost.Subject.Uri.Rkey), subjectPlc), commitPlc, out _))
                        relationshipIsAbsent = true;
                }
                else if (record is Follow follow)
                {
                    subjectPlc = rels.TrySerializeDidMaybeReadOnly(follow.Subject!.DidAsString, ctx);
                    if (subjectPlc != default && !rels.Follows.HasActor(subjectPlc, commitPlc, out _))
                        relationshipIsAbsent = true;
                }


                return (commitPlc, subjectPlc, viaPlc, relationshipIsAbsentAsOf: relationshipIsAbsent ? rels.Version : 0);
            }, ctx);


            ctx.AllowStale = false;
            WithRelationshipsWriteLock(relationships =>
            {
                if (ignoreIfDisposing && relationships.IsDisposed) return;

                relationships.EnsureNotDisposed();

                var commitPlc = relationships.SerializeDidWithHint(commitAuthor, ctx, preresolved.commitPlc);

                if (commitAuthor.StartsWith("did:web:", StringComparison.Ordinal))
                {
                    relationships.IndexHandle(null, commitAuthor, ctx);
                }

                if (record is Like l)
                {
                    var likeRkey = GetMessageTid(path, Like.RecordType);

                    if (!Apis.LikeSpamThrottler.TryAddEvent(commitPlc, likeRkey))
                    {
                        if (Apis.PrintedLikeSpamMessages.Add(commitPlc))
                            LogInfo("Refusing post like spam events by " + commitAuthor);
                        return;
                    }

                    if (l.Subject.Uri.Collection == Post.RecordType)
                    {
                        // quick check to avoid noisy exceptions

                        var postId = relationships.GetPostId(l.Subject, ctx, hint: preresolved.subjectPlc);

                        // So that Likes.GetApproximateActorCount can quickly skip most slices (MaximumKey)
                        BlueskyRelationships.EnsureNotExcessivelyFutureDate(postId.PostRKey);


                        if (relationships.Likes.Add(postId, new Relationship(commitPlc, likeRkey), preresolved.relationshipIsAbsentAsOf))
                        {
                            relationships.AddNotification(postId, NotificationKind.LikedYourPost, commitPlc, ctx, likeRkey.Date);

                            var approxActorCount = relationships.GetApproximateLikeCount(postId, couldBePluggablePost: false);
                            approxActorCount++; // this dict is only written here, while holding the write lock.
                            var primaryLikeCountCache = relationships.ApproximateLikeCountCache;

                            if (approxActorCount >= int.MaxValue)
                                primaryLikeCountCache.Remove(postId);
                            else
                                primaryLikeCountCache[postId] = (int)approxActorCount;


                            var replicaLikeCountCache = relationships.ReplicaOnlyApproximateLikeCountCache;
                            if (replicaLikeCountCache.Dictionary.ContainsKey(postId))
                            {
                                if (approxActorCount >= int.MaxValue)
                                    replicaLikeCountCache.Remove(postId);
                                else
                                    replicaLikeCountCache[postId] = (int)approxActorCount;
                            }

                            //Console.Error.WriteLine("LikeCount: " + approxActorCount);
                            relationships.MaybeIndexPopularPost(postId, "likes", approxActorCount, BlueskyRelationships.SearchIndexPopularityMinLikes);
                            relationships.NotifyPostStatsChange(postId, commitPlc);


                            if (l.Via != null)
                            {
                                if (l.Via.Uri.Collection != Repost.RecordType) throw new UnexpectedFirehoseDataException("Like.via should be a repost URL.");
                                var viaRepostPlc = relationships.SerializeDidWithHint(l.Via.Uri.DidAsString, ctx, preresolved.viaPlc);
                                var viaRepostRkey = l.Via.Uri.Rkey;
                                if (viaRepostPlc != postId.Author)
                                    relationships.AddNotification(viaRepostPlc, NotificationKind.LikedYourRepost, commitPlc, Tid.Parse(viaRepostRkey), ctx, likeRkey.Date);
                            }

                            relationships.IncrementRecentPopularPostLikeCount(postId, null);

                            if (relationships.IsRegisteredForNotifications(commitPlc))
                                relationships.SeenPosts.Add(commitPlc, new PostEngagement(postId, PostEngagementKind.LikedOrBookmarked));
                        }
                    }
                    else if (l.Subject.Uri.Collection == Generator.RecordType)
                    {
                        // TODO: handle deletions for feed likes
                        var feedId = new RelationshipHashedRKey(relationships.SerializeDidWithHint(l.Subject.Uri.DidAsString, ctx, preresolved.subjectPlc), l.Subject.Uri.Rkey);

                        if (relationships.FeedGeneratorLikes.Add(feedId, new Relationship(commitPlc, likeRkey)))
                        {
                            var approxActorCount = relationships.FeedGeneratorLikes.GetApproximateActorCount(feedId);
                            relationships.MaybeIndexPopularFeed(feedId, "likes", approxActorCount, BlueskyRelationships.SearchIndexFeedPopularityMinLikes);
                            relationships.AddNotification(feedId.Plc, NotificationKind.LikedYourFeed, commitPlc, new Tid((long)feedId.RKeyHash) /*evil cast*/, ctx, likeRkey.Date);
                            if (!relationships.FeedGenerators.ContainsKey(feedId))
                            {
                                ScheduleRecordIndexing(l.Subject.Uri, ctx);
                            }
                        }
                    }
                }
                else if (record is Follow f)
                {
                    if (HasNumericRKey(path)) return;

                    var rkey = GetMessageTid(path, Follow.RecordType);


                    if (!Apis.FollowSpamThrottler.TryAddEvent(commitPlc, rkey))
                    {
                        // TODO: what about follows from starter packs?
                        if (Apis.PrintedFollowSpamMessages.Add(commitPlc))
                            LogInfo("Refusing follow spam events by " + commitAuthor);
                        return;
                    }

                    var followed = relationships.SerializeDidWithHint(f.Subject!.DidAsString, ctx, preresolved.subjectPlc);

                    if (relationships.Follows.Add(followed, new Relationship(commitPlc, rkey), preresolved.relationshipIsAbsentAsOf))
                    {
                        if (relationships.IsRegisteredForNotifications(followed))
                            relationships.AddNotification(followed, relationships.Follows.HasActor(commitPlc, followed, out _) ? NotificationKind.FollowedYouBack : NotificationKind.FollowedYou, commitPlc, ctx, rkey.Date);
                    }


                    if (relationships.IsRegisteredForNotifications(commitPlc)) // must stay outside of if(Follow.Add) since IsRegisteredForNotifications can change over time
                    {
                        relationships.RegisteredUserToFollowees.AddIfMissing(commitPlc, new ListEntry(followed, rkey));
                    }
                }
                else if (record is Repost r)
                {
                    var postId = relationships.GetPostId(r.Subject!, ctx, hint: preresolved.subjectPlc);
                    BlueskyRelationships.EnsureNotExcessivelyFutureDate(postId.PostRKey);

                    var repostRKey = GetMessageTid(path, Repost.RecordType);
                    if (relationships.Reposts.Add(postId, new Relationship(commitPlc, repostRKey), preresolved.relationshipIsAbsentAsOf))
                    {
                        relationships.AddNotification(postId, NotificationKind.RepostedYourPost, commitPlc, ctx, repostRKey.Date);

                        relationships.MaybeIndexPopularPost(postId, "reposts", relationships.Reposts.GetApproximateActorCount(postId), BlueskyRelationships.SearchIndexPopularityMinReposts);
                        relationships.UserToRecentReposts.Add(commitPlc, new RecentRepost(repostRKey, postId));
                        relationships.NotifyPostStatsChange(postId, commitPlc);

                        if (relationships.IsRegisteredForNotifications(commitPlc))
                            relationships.SeenPosts.Add(commitPlc, new PostEngagement(postId, PostEngagementKind.LikedOrBookmarked));

                        if (r.Via != null)
                        {
                            if (r.Via.Uri.Collection != Repost.RecordType) throw new UnexpectedFirehoseDataException("Repost.via should be a repost URL.");
                            var viaRepostPlc = relationships.SerializeDidWithHint(r.Via.Uri.DidAsString, ctx, preresolved.viaPlc);
                            var viaRepostRkey = r.Via.Uri.Rkey;

                            if (viaRepostPlc != postId.Author)
                                relationships.AddNotification(viaRepostPlc, NotificationKind.RepostedYourRepost, commitPlc, Tid.Parse(viaRepostRkey), ctx, repostRKey.Date);
                        }
                    }



                    relationships.AddRepostToRecentRepostCache(commitPlc, new RecentRepost(repostRKey, postId));
                }
                else if (record is Block b)
                {
                    var blockedUser = relationships.SerializeDid(b.Subject!.DidAsString, ctx);
                    var blockRkey = GetMessageTid(path, Block.RecordType);
                    relationships.Blocks.Add(blockedUser, new Relationship(commitPlc, blockRkey));
                    relationships.AddNotification(blockedUser, NotificationKind.BlockedYou, commitPlc, ctx, blockRkey.Date);
                }
                else if (record is Post p)
                {
                    var rkey = GetMessageTid(path, Post.RecordType);


                    var isRootPost = p.Reply == null;
                    var isReplyToOwnThread = p.Reply?.Root.Uri.DidAsString == commitAuthor;

                    if (isRootPost || !isReplyToOwnThread)
                    {
                        if (!Apis.PostSpamThrottler.TryAddEvent(commitPlc, rkey))
                        {
                            if (Apis.PrintedPostSpamMessages.Add(commitPlc))
                                LogInfo("Refusing post spam events by " + commitAuthor);
                            return;
                        }
                    }
                    else
                    {
                        // Non-root reply to own thread. Don't count towards PostSpamThrottler, because bsky UI allows multi-post threads with a single submit button.
                    }

                    var postId = new PostId(commitPlc, rkey);
                    BlueskyRelationships.EnsureNotExcessivelyFutureDate(postId.PostRKey);

                    // Is this check too expensive?
                    //var didDoc = relationships.TryGetLatestDidDoc(commitPlc);
                    //if (didDoc != null && Apis.AdministrativeBlocklist.ShouldBlockIngestion(null, didDoc))
                    //return;

                    var proto = relationships.StorePostInfoExceptData(p, postId, ctx);
                    if (proto != null)
                    {


                        byte[]? postBytes = null;
                        continueOutsideLock = new ContinueOutsideLock(() => postBytes = BlueskyRelationships.SerializePostData(proto, commitAuthor), relationships =>
                        {
                            relationships.PostData.AddRange(postId, postBytes); // double insertions are fine, the second one wins.
                        });
                    }
                }
                else if (record is Profile pf && GetMessageRKey(path, Profile.RecordType) == "self")
                {
                    relationships.StoreProfileBasicInfo(commitPlc, pf, ctx);
                }
                else if (record is List list)
                {
                    relationships.Lists.AddRange(new Relationship(commitPlc, GetMessageTid(path, List.RecordType)), BlueskyRelationships.SerializeProto(BlueskyRelationships.ListToProto(list)));
                }
                else if (record is Link link)
                {
                    relationships.ProfileLinks.AddRange(new Relationship(commitPlc, GetMessageTid(path, Link.RecordType)), BlueskyRelationships.SerializeProto(BlueskyRelationships.ProfileLinkToProto(link)));
                }
                else if (record is Listitem listItem)
                {
                    if (commitAuthor != listItem.List!.DidAsString) throw new UnexpectedFirehoseDataException("Listitem for non-owned list.");
                    if (listItem.List.Collection != List.RecordType) throw new UnexpectedFirehoseDataException("Listitem in non-listitem collection.");
                    var listRkey = Tid.Parse(listItem.List.Rkey);
                    var listItemRkey = GetMessageTid(path, Listitem.RecordType);
                    var member = relationships.SerializeDid(listItem.Subject!.DidAsString, ctx);

                    var listId = new Relationship(commitPlc, listRkey);
                    var entry = new ListEntry(member, listItemRkey);

                    relationships.ListItems.Add(listId, entry);
                    relationships.ListMemberships.Add(entry.Member, new ListMembership(commitPlc, listRkey, listItemRkey));
                    relationships.AddNotification(entry.Member, NotificationKind.AddedYouToAList, commitPlc, listRkey, ctx, entry.ListItemRKey.Date);

                    if (!isRepositoryImport && !relationships.HaveCollectionForUser(commitPlc, RepositoryImportKind.ListEntries))
                    {
                        Indexer.RunOnFirehoseProcessingThreadpoolFireAndForget(async () =>
                        {
                            var result = await Apis.EnsureHaveCollectionAsync(commitPlc, RepositoryImportKind.ListEntries, ctx, slowImport: true);
                            if (result != null && result.Error != null)
                            {
                                Log($"Could not fetch full ListEntries collection for " + commitAuthor + ": " + result.Error);
                            }
                            return result;
                        });
                    }
                }
                else if (record is Threadgate threadGate)
                {
                    var rkey = GetMessageTid(path, Threadgate.RecordType);
                    relationships.StoreThreadgate(commitAuthor, commitPlc, rkey, threadGate, ctx);
                }
                else if (record is Postgate postGate)
                {
                    var rkey = GetMessageTid(path, Postgate.RecordType);
                    relationships.StorePostgate(commitAuthor, commitPlc, rkey, postGate, ctx);
                }
                else if (record is Listblock listBlock)
                {
                    var blockId = new Relationship(commitPlc, GetMessageTid(path, Listblock.RecordType));
                    if (listBlock.Subject!.Collection != List.RecordType) throw new UnexpectedFirehoseDataException("Listblock in non-listblock collection.");
                    var listId = new Relationship(relationships.SerializeDid(listBlock.Subject.DidAsString, ctx), Tid.Parse(listBlock.Subject.Rkey));

                    relationships.ListBlocks.Add(blockId, listId);
                    relationships.ListSubscribers.Add(listId, blockId);
                }
                else if (record is Generator generator)
                {
                    var rkey = GetMessageRKey(path, Generator.RecordType);
                    relationships.IndexFeedGenerator(commitPlc, rkey, generator, now);
                }
                //else LogInfo("Creation of unknown object type: " + path);
            }, ctx);

            if (continueOutsideLock != null)
            {

                continueOutsideLock.Value.OutsideLock();
                WithRelationshipsWriteLock(relationships =>
                {
                    continueOutsideLock.Value.Complete(relationships);
                }, ctx);

            }
        }


        private readonly ConcurrentSet<string> currentlyRunningRecordRetrievals = [];
        private void ScheduleRecordIndexing(ATUri uri, RequestContext ctx)
        {
            if (!currentlyRunningRecordRetrievals.TryAdd(uri.ToString())) return;
            Task.Run(async () =>
            {
                LogInfo("Fetching record " + uri);

                var record = (await Apis.GetRecordAsync(uri.DidAsString, uri.Collection!, uri.Rkey!, ctx)).Value;

                OnRecordCreated(uri.DidAsString, uri.GetParsedPath(), record, ignoreIfDisposing: true);

                currentlyRunningRecordRetrievals.Remove(uri.ToString());

            });
        }

        public void OnJetStreamEvent(JetstreamEvent e)
        {
            if (!OnFirehoseEventBeginProcessing()) return;


            if (!string.IsNullOrEmpty(e.Did))
                VerifyValidForCurrentRelay?.Invoke(e.Did);

            if (e.Account is { } acct)
            {
                OnAccountStateChanged(acct.Did!, acct.Active, acct.Status);
                return;

            }

            if (e.Commit is { } commit)
            {

                var operation = commit.Operation;
                if (operation is "create" or "update")
                {
                    OnRecordCreated(e.Did, new CollectionAndRKey(commit.Collection!, commit.Rkey!), commit.Record!.Value, ignoreIfDisposing: true);
                }
                else if (operation == "delete")
                {
                    OnRecordDeleted(e.Did, new CollectionAndRKey(commit.Collection!, commit.Rkey!), ignoreIfDisposing: true);
                }
            }

        }

        private bool OnFirehoseEventBeginProcessing()
        {
            if (Apis.ShutdownRequested.IsCancellationRequested) return false;

            var received = Interlocked.Read(ref RecordsReceived);
            var processed = Interlocked.Read(ref RecordsProcessed);
            var lagBehind = received - processed;

            if ((RecordsReceived % 30) == 0)
            {
                if (lagBehind >= LagBehindWarnThreshold)
                {
                    lock (FirehoseLagBehindWarnLock)
                    {
                        if (LastLagBehindWarningPrint == null || LastLagBehindWarningPrint.ElapsedMilliseconds > LagBehindWarnIntervalMs)
                        {
                            LogInfo($"Struggling to process the firehose quickly enough, lagging behind: {lagBehind} ({processed}/{received}, {(100.0 * processed / received):0.0}%)");
                            LastLagBehindWarningPrint ??= Stopwatch.StartNew();
                            LastLagBehindWarningPrint.Restart();
                        }
                    }

                }
            }
            return true;
        }

        public static string GetMessageRKey(CollectionAndRKey path, string prefix)
        {
            BlueskyRelationships.Assert(prefix[^1] != '/');
            if (!path.Collection.SequenceEqual(prefix)) throw new UnexpectedFirehoseDataException($"Expecting path prefix {prefix}, but found {path}");
            var postShortId = path.RKey;
            return postShortId;
        }

        public static Tid GetMessageTid(CollectionAndRKey path, string prefix) => Tid.Parse(GetMessageRKey(path, prefix));




        public static RetryPolicy CreateMainFirehoseRetryPolicy()
        {
            return RetryPolicy.CreateConstant(TimeSpan.FromSeconds(5));
        }


        const int DefaultFirehoseConnectTimeoutMs = 10_000;

        private void LogFirehoseStartMessage(FirehoseCursor currentFirehoseCursor, bool isImportant)
        {
            string message;
            if (currentFirehoseCursor.CommittedCursor == null)
                message = $"Starting firehose {FirehoseUrl} at current time.";
            else
                message = $"Starting firehose {FirehoseUrl} at cursor '{currentFirehoseCursor.CommittedCursor}' (~{currentFirehoseCursor.FirehoseTimeLastProcessed}, {StringUtils.ToHumanTimeSpan(DateTime.UtcNow - currentFirehoseCursor.FirehoseTimeLastProcessed, showSeconds: true)} ago)";
            if (isImportant) Log(message);
            else LogInfo(message);

        }





        public Task ListenToFirehoseLabels(string nameForDebugging, CancellationToken ct = default)
        {

            async IAsyncEnumerable<ISubscribeLabelsMessage> CoreAsync(string? cursor, [EnumeratorCancellation] CancellationToken connectionCt)
            {
                using var client = new ATProtoClient(new ATProtoClientOptions
                {
                    BaseUrl = FirehoseUrl.GetNext(),
                    LoggerFactory = new LogWrapper.LogFactory(),
                    JsonOptions = ATProtoClientFactory.CreateJsonOptions(),
                    CborContext = CarpaNet.Cbor.ATProtoCborContext.Default,
                    HttpClient = BlueskyEnrichedApis.DefaultHttpClient,
                });
                await foreach (var e in client.ComAtprotoLabelSubscribeLabelsAsync(new SubscribeLabelsParameters
                {
                    Cursor = cursor != null ? long.Parse(cursor) : null
                }, connectionCt))
                {
                    yield return e;
                }
            }

            return ListenToFirehoseAsync(CoreAsync, OnLabelFirehoseEvent, RetryPolicy.CreateForUnreliableServer(), useWatchdog: false, errorsAreUnimportant: FirehoseUrl.CanonicalIdentifier != "https://mod.bsky.app/", ct: ct);
        }

        public Task ListenToFirehoseRepos(RetryPolicy? retryPolicy, bool useWatchdog = true, CancellationToken ct = default)
        {

            async IAsyncEnumerable<ISubscribeReposMessage> CoreAsync(string? cursor, [EnumeratorCancellation] CancellationToken connectionCt)
            {
                using var client = new ATProtoClient(new ATProtoClientOptions
                {
                    BaseUrl = FirehoseUrl.GetNext(),
                    LoggerFactory = new LogWrapper.LogFactory(),
                    JsonOptions = ATProtoClientFactory.CreateJsonOptions(),
                    CborContext = CarpaNet.Cbor.ATProtoCborContext.Default,
                    HttpClient = BlueskyEnrichedApis.DefaultHttpClient,
                });
                await foreach (var e in client.ComAtprotoSyncSubscribeReposAsync(new SubscribeReposParameters
                {
                    Cursor = cursor != null ? long.Parse(cursor) : null
                }, connectionCt))
                {
                    yield return e;
                }
            }

            return ListenToFirehoseAsync(CoreAsync, OnRepoFirehoseEvent, retryPolicy, getDidForDebugging: x => (x as SubscribeReposCommit)?.Repo.Value, useWatchdog: useWatchdog, ct: ct);
        }

        public Task ListenToFirehoseJetstream(RetryPolicy? retryPolicy, CancellationToken ct = default)
        {
            async IAsyncEnumerable<JetstreamEvent> CoreAsync(string? cursor, [EnumeratorCancellation] CancellationToken connectionCt)
            {
                using var client = new JetstreamClient(FirehoseUrl.GetNext()) { BufferSize = 2 * 1024 * 1024 };
                await foreach (var e in client.SubscribeAsync(new JetstreamSubscribeOptions
                {
                    Cursor = cursor != null ? long.Parse(cursor) : null
                }, connectionCt))
                {
                    yield return e;
                }
            }

            return ListenToFirehoseAsync(CoreAsync, e =>
            {
                OnJetStreamEvent(e);
                return new FirehoseEventCursor(DateTime.UnixEpoch.AddMicroseconds(e.TimeUs), e.TimeUs);
            }, retryPolicy, getDidForDebugging: x => x.Did, ct: ct);
        }



        private async Task ListenToFirehoseAsync<T>(Func<string?, CancellationToken, IAsyncEnumerable<T>> subscribeKind, Func<T, FirehoseEventCursor> onEvent, RetryPolicy? retryPolicy, Func<T, string?>? getDidForDebugging = null, bool useWatchdog = true, bool errorsAreUnimportant = false, CancellationToken ct = default)
        {
            await Task.Yield();
            await PluggableProtocol.RetryInfiniteLoopAsync(FirehoseUrl.CanonicalIdentifier, async ct =>
            {
                try
                {

                    this.currentFirehoseCursor = relationshipsUnlocked.GetOrCreateFirehoseCursorThreadSafe(FirehoseUrl.CanonicalIdentifier); currentFirehoseCursor.State = FirehoseState.Starting;
                    currentFirehoseCursor.State = FirehoseState.Starting;
                    currentFirehoseCursor.RestartCount++;

                    LogFirehoseStartMessage(currentFirehoseCursor, isImportant: true);

                    using var connectionCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                    connectionCts.CancelAfter(TimeSpan.FromMilliseconds(AppViewLiteConfiguration.GetInt32(AppViewLiteParameter.APPVIEWLITE_FIREHOSE_CONNECT_TIMEOUT_MS) ?? DefaultFirehoseConnectTimeoutMs));

                    var outOfOrderTracker = new OutOfOrderCompletionTracker();

                    var timeoutMillis = useWatchdog ? (AppViewLiteConfiguration.GetInt32(AppViewLiteParameter.APPVIEWLITE_FIREHOSE_WATCHDOG_SECONDS) ?? 30) * 1000 : Timeout.Infinite;
                    
                    await foreach(var e in subscribeKind(currentFirehoseCursor.CommittedCursor, connectionCts.Token))
                    {


                        currentFirehoseCursor.State = FirehoseState.Running;


                        connectionCts.CancelAfter(timeoutMillis);

                        currentFirehoseCursor!.SystemTimeLastRawReceived = DateTime.UtcNow;
                        Interlocked.Increment(ref currentFirehoseCursor!.ReceivedEvents);

                        var uniqueId = outOfOrderTracker.OnEventGenerated();
                        FirehoseThreadpoolTaskFactory!.StartNew(() =>
                        {

                            FirehoseEventCursor cursor = default;
                            TryProcessRecord(() =>
                            {
                                cursor = onEvent(e);
                            }, getDidForDebugging?.Invoke(e) ?? this.FirehoseUrl.CanonicalIdentifier);

                            
                            outOfOrderTracker.OnEventProcessed(uniqueId, cursor.Cursor != 0 ? cursor.Cursor : null);
                            BumpLargestSeenFirehoseCursor(outOfOrderTracker.LastDefinitelyProcessedSeq, cursor.Date);


                        }, ct).FireAndForget();

                    }
                }
                catch (Exception ex)
                {
                    currentFirehoseCursor!.State = FirehoseState.Error;
                    currentFirehoseCursor.LastException = ex;
                    currentFirehoseCursor.LastExceptionDate = DateTime.UtcNow;
                    throw;
                }
            }, ct, retryPolicy: retryPolicy, errorsAreUnimportant: errorsAreUnimportant);

        }

        private Watchdog? CreateFirehoseWatchdog(TaskCompletionSource tcs)
        {
            if (Debugger.IsAttached) return null;
            var timeout = AppViewLiteConfiguration.GetInt32(AppViewLiteParameter.APPVIEWLITE_FIREHOSE_WATCHDOG_SECONDS) ?? 120;
            if (timeout == 0) return null;
            return new Watchdog(TimeSpan.FromSeconds(timeout), () =>
            {
                Log("Firehouse watchdog");
                File.AppendAllText(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile) + "/firehose-watchdog.txt", DateTime.UtcNow.ToString("O") + " " + FirehoseUrl + "\n");
                tcs.TrySetException(new Exception("Firehose watchdog"));
            });
        }


        private void BumpLargestSeenFirehoseCursor(long? definitelyProcessedCursor, DateTime? recentEventDate)
        {
            if (definitelyProcessedCursor != null)
            {
                var definitelyProcessedCursorAsString = definitelyProcessedCursor.ToString();

                while (true)
                {
                    var oldCursor = currentFirehoseCursor!.CommittedCursor;
                    if (long.Parse(oldCursor ?? "0", CultureInfo.InvariantCulture) >= definitelyProcessedCursor) break;

                    Interlocked.CompareExchange(ref currentFirehoseCursor!.CommittedCursor, definitelyProcessedCursorAsString, oldCursor);
                }
            }
            if (recentEventDate != null)
                currentFirehoseCursor!.FirehoseTimeLastProcessed = recentEventDate.Value;
            currentFirehoseCursor!.SystemTimeLastProcessed = DateTime.UtcNow;
        }

        internal FirehoseCursor? currentFirehoseCursor;


        private FirehoseEventCursor OnRepoFirehoseEvent(ISubscribeReposMessage e)
        {
            if (!OnFirehoseEventBeginProcessing()) return default;

            
            if (e is SubscribeReposAccount acct)
            {
                VerifyValidForCurrentRelay!(acct.Did.DidAsString);

                OnAccountStateChanged(acct.Did!.DidAsString, acct.Active, acct.Status);
                return new FirehoseEventCursor(acct.Time.UtcDateTime, acct.Seq);
            }

            if (e is SubscribeReposCommit commit)
            {
                var commitAuthor = commit.Repo.DidAsString;

                VerifyValidForCurrentRelay!(commitAuthor);

                var carReader = new CarpaNet.Repo.CarReader(commit.Blocks);
                var blocks = carReader.ReadAllBlocks();
                
                
                foreach (var op in commit.Ops)
                {
                    if (op.Action == "delete")
                    {
                        OnRecordDeleted(commitAuthor, CollectionAndRKey.ParseUnprefixed(op.Path!), ignoreIfDisposing: true);
                    }
                    else
                    {
                        var block = blocks[op.Cid!.Value.Value];
                        throw new NotImplementedException();
                        //var record = commit.Records!.First(x => x.Cid.Equals(op.Cid)).Value;

                        //OnRecordCreated(commitAuthor, CollectionAndRKey.ParseUnprefixed(op.Path!), record, ignoreIfDisposing: true);
                    }
                }
                return new FirehoseEventCursor(commit.Time.UtcDateTime, commit.Seq);

            }

            return default;
        }

        private void OnAccountStateChanged(string did, bool active, string? status)
        {
            VerifyValidForCurrentRelay!(did);
            var ctx = RequestContext.CreateForFirehose("FirehoseAccountState");
            Apis.SetAccountState(did, active, status, ctx);
        }

        private FirehoseEventCursor OnLabelFirehoseEvent(ISubscribeLabelsMessage e)
        {
            if (e is SubscribeLabelsLabels labels)
            {

                var ctx = RequestContext.CreateForFirehose("Label");
                foreach (var label in labels.Labels)
                {
                    VerifyValidForCurrentRelay!(label.Src.DidAsString);
                    OnLabelCreated(label.Src.DidAsString, label, ctx);
                }


                return new FirehoseEventCursor(labels.Labels.LastOrDefault()!.Cts.UtcDateTime, labels.Seq);
            }
            return default;
        }

        private void OnLabelCreated(string labeler, DefsLabel label, RequestContext ctx)
        {
            var uri = new ATUri(label.Uri);
            if (string.IsNullOrEmpty(label.Val))
                throw new ArgumentException("OnLabelCreated: label is null or empty");

            // LogInfo("Label: " + label.Val +  " to " + label.Uri + " (from " + labeler + " via " + this.FirehoseUrl + ")");
            WithRelationshipsWriteLock(rels =>
            {

                var entry = new LabelEntry(rels.SerializeDid(labeler, ctx), (ApproximateDateTime32)label.Cts.UtcDateTime, BlueskyRelationships.HashLabelName(label.Val), label.Neg ?? false);

                if (!rels.LabelNames.ContainsKey(entry.KindHash))
                {
                    rels.LabelNames.AddRange(entry.KindHash, System.Text.Encoding.UTF8.GetBytes(label.Val));
                }

                if (uri.Collection != null)
                {
                    if (uri.Collection == Post.RecordType)
                    {
                        var postId = rels.GetPostId(uri, ctx);
                        rels.PostLabels.Add(postId, entry);
                        rels.LabelToPosts.Add(new LabelId(entry.Labeler, entry.KindHash), postId);
                        rels.AddNotification(postId, NotificationKind.LabeledYourPost, entry.Labeler, ctx, entry.Date);
                    }
                }
                else
                {
                    var labeledUser = rels.SerializeDid(uri.DidAsString, ctx);
                    rels.ProfileLabels.Add(labeledUser, entry);
                    rels.LabelToProfiles.Add(new LabelId(entry.Labeler, entry.KindHash), labeledUser);
                    rels.AddNotification(labeledUser, NotificationKind.LabeledYourProfile, entry.Labeler, ctx, entry.Date);
                }


            }, ctx);
        }

        public Action<string>? VerifyValidForCurrentRelay;

        public async Task<Tid> ImportCarAsync(string did, string carPath, RequestContext ctx, Action<CarImportProgress>? progress, CancellationToken ct = default)
        {
            using var stream = File.Open(carPath, FileMode.Open, FileAccess.Read, FileShare.Read);
            return await ImportCarAsync(did, stream, ctx, null, progress, ct).ConfigureAwait(false);
        }


        public async Task<Tid> ImportCarAsync(string did, Stream stream, RequestContext ctx, DateTime? probableDateOfEarliestRecord, Action<CarImportProgress>? progress, CancellationToken ct = default)
        {
            using var _ = BlueskyEnrichedApis.LogNetworkOperation(ctx, nameof(ImportCarAsync), argument: did);
#pragma warning disable CA2000 // Must be disposed by caller
            if (!stream.CanSeek) stream = new PositionAwareStream(stream);
#pragma warning restore CA2000
            using var importer = new CarImporter(did, probableDateOfEarliestRecord ?? await GetProbableDateOfEarliestRecordAsync(did, ctx), relationshipsUnlocked.CarSpillDirectory);
            importer.Log("Reading stream");

            using var carReader = new CarReader(stream);
            foreach (var item in carReader.ReadBlocks())
            {
                ct.ThrowIfCancellationRequested();
                //Console.Error.WriteLine(item.Cid + " = " + Convert.ToHexString(item.Cid.ToArray()));

                importer.OnCarDecoded(item.Cid, item.Data);
                var estimatedProgress = importer.EstimatedRetrievalProgress;
                var estimatedTotalSize = stream.Position / estimatedProgress;
                progress?.Invoke(new CarImportProgress(stream.Position, (long)estimatedTotalSize, default, default, default));

            }
            var totalSize = stream.Position;
            importer.LogStats();
            long recordCount = 0;
            foreach (var record in importer.EnumerateRecords())
            {
                ct.ThrowIfCancellationRequested();
                recordCount++;

                var path = CollectionAndRKey.ParseUnprefixed(record.Path);
                if (Tid.TryParse(path.RKey, out var tid))
                    progress?.Invoke(new CarImportProgress(totalSize, totalSize, recordCount, importer.TotalRecords, tid));

                await Apis.DangerousUnlockedRelationships.CarRecordInsertionSemaphore.WaitAsync(ct);
                try
                {

                    TryProcessRecord(() => OnRecordCreated(record.Did, path, record.Record, isRepositoryImport: true), record.Did);
                }
                finally
                {
                    Apis.DangerousUnlockedRelationships.CarRecordInsertionSemaphore.Release();
                }

            }
            importer.Log("Done.");
            return importer.LargestSeenRev;
        }

        private async Task<DateTime> GetProbableDateOfEarliestRecordAsync(string did, RequestContext ctx)
        {
            try
            {
                using var at = await Apis.CreateProtocolForDidAsync(did, ctx);
                var earliestLike = Tid.Parse(((await Apis.ListRecordsAsync(did, Like.RecordType, 1, null, ctx, descending: false)).Records.First()).Uri.Rkey).Date;
                return earliestLike;
            }
            catch (Exception ex)
            {
                LogNonCriticalException("Could not determine date of earliest like.", ex);
            }
            return new DateTime(2024, 3, 1, 0, 0, 0, DateTimeKind.Utc);
        }

        private static void TryProcessRecord(Action action, string? authorForDebugging = null)
        {
            try
            {
                action();
            }
            catch (UnexpectedFirehoseDataException ex)
            {
                LogInfo(authorForDebugging + ": " + ex.Message);
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception ex)
            {
                LogNonCriticalException(authorForDebugging ?? "Unknown record author", ex);
            }
        }



        public async Task<Tid> ImportCarAsync(string did, Tid since, RequestContext ctx, Action<CarImportProgress>? progress = null, CancellationToken ct = default)
        {
            await Apis.CarDownloadSemaphore.WaitAsync(ct);
            try
            {
                using var stream = await GetCarStreamAsync(did, ctx, since, ct).ConfigureAwait(false);
                var tid = await ImportCarAsync(did, stream, ctx, since != default ? since.Date : null, progress, ct).ConfigureAwait(false);
                return tid != default ? tid : since;
            }
            finally
            {
                Apis.CarDownloadSemaphore.Release();
            }

        }


        private async Task<Stream> GetCarStreamAsync(string did, RequestContext ctx, Tid since = default, CancellationToken ct = default)
        {
            throw new NotImplementedException();
#if false
            using var at = await Apis.CreateProtocolForDidAsync(did, ctx);
            var pds = (await at.ResolveATDidHostAsync(new ATDid(did), ct)).HandleResult()!;
            var url = new Uri(new Uri(pds), ComAtproto.Sync.SyncEndpoints.GetRepo + "?did=" + did + (since != default ? "&since=" + since : null));
            return await at.Client.GetStreamAsync(url, ct);
#endif

        }

        public async Task<(Tid LastTid, Exception? Exception)> IndexUserCollectionAsync(string did, string recordType, Tid since, RequestContext ctx, CancellationToken ct, Action<CarImportProgress>? progress, bool slowImport)
        {
            using var _ = BlueskyEnrichedApis.LogNetworkOperation(ctx, nameof(IndexUserCollectionAsync), recordType, argument: did);
            using var at = await Apis.CreateProtocolForDidAsync(did, ctx);

            string? cursor = since != default ? since.ToString() : null;
            Tid lastTid = since;
            long recordCount = 0;
            DateTime oldestRecord = default;
            try
            {
                while (true)
                {
                    var page = await at.ComAtprotoRepoListRecordsAsync(new ComAtproto.Repo.ListRecordsParameters 
                    {
                        Repo = new ATDid(did),
                        Collection = recordType, 
                        Limit = 100, 
                        Cursor = cursor, 
                        Reverse = true
                    }, cancellationToken: ct);
                    cursor = page!.Cursor;
                    foreach (var item in page.Records)
                    {
                        recordCount++;

                        await Apis.DangerousUnlockedRelationships.CarRecordInsertionSemaphore.WaitAsync(ct);
                        try
                        {
                            OnRecordCreated(did, item.Uri.GetParsedPath(), item.Value, isRepositoryImport: true);
                        }
                        finally
                        {
                            Apis.DangerousUnlockedRelationships.CarRecordInsertionSemaphore.Release();
                        }

                        if (Tid.TryParse(item.Uri.Rkey, out var tid))
                        {
                            if (oldestRecord == default)
                                oldestRecord = tid.Date;
                            var timespan = DateTime.UtcNow - oldestRecord;
                            if (timespan.Ticks >= 0 && recordCount >= 10)
                            {
                                var positionWithinTimespan = tid.Date - oldestRecord;
                                var positionRatioWithinTimespan = (double)positionWithinTimespan.Ticks / timespan.Ticks;
                                var totalRecordsEstimation = (double)recordCount / positionRatioWithinTimespan;
                                progress?.Invoke(new CarImportProgress(0, 0, recordCount, Math.Max(recordCount + 10, (long)Math.Ceiling(totalRecordsEstimation)), tid));

                            }

                            lastTid = tid;
                        }
                    }

                    // if (slowImport) await Task.Delay();

                    if (cursor == null) break;
                }
                progress?.Invoke(new CarImportProgress(0, 0, recordCount, recordCount, lastTid));
                return (lastTid, null);
            }
            catch (Exception ex)
            {
                return (lastTid, ex);
            }

        }


        public async Task RetrievePlcDirectoryAsync()
        {
            var ctx = RequestContext.CreateForFirehose("PlcDirectory");
            var lastRetrievedDidDoc = WithRelationshipsLock(rels => rels.LastRetrievedPlcDirectoryEntry.MaximumKey, ctx) ?? new DateTime(1980, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            await RetrievePlcDirectoryAsync(EnumeratePlcDirectoryAsync(lastRetrievedDidDoc), ctx);
        }

        public async Task InitializePlcDirectoryFromBundleAsync(string parquetFileOrDirectory)
        {
            var ctx = RequestContext.CreateForFirehose("PlcDirectoryBulkImport");
            var prevDate = WithRelationshipsLock(rels => rels.LastRetrievedPlcDirectoryEntry.MaximumKey, ctx);
            using var mem = ThreadSafeTypedDuckDbConnection.CreateInMemory();
            var checkGaps = false;
            if (Directory.Exists(parquetFileOrDirectory))
            {
                parquetFileOrDirectory += "/*.parquet";
                checkGaps = true;
            }
            Log($"Incremental PLC directory bundle import since {prevDate?.ToString() ?? "beginning"}...");
            mem.ExecuteNonQuery("SET memory_limit = '100MB';");
            mem.ExecuteNonQuery("SET threads = 1;");
            var rows = mem.ExecuteStreamed<PlcDirectoryBundleParquetRow>($"from '{parquetFileOrDirectory}' where Date >= ?", prevDate ?? new DateTime(1980, 1, 1, 0, 0, 0, DateTimeKind.Utc))
                .Select(row =>
                {
                    var x = new DidDocProto
                    {
                        PlcAsUInt128 = row.PlcAsUInt128,
                        EarliestDateApprox16 = row.EarliestDateApprox16,
                        BskySocialUserName = row.BskySocialUserName,
                        CustomDomain = row.CustomDomain,
                        Pds = row.Pds,
                        MultipleHandles = row.MultipleHandles,
                        OtherUrls = row.OtherUrls,
                        AtProtoLabeler = row.AtProtoLabeler,
                        Date = row.Date!.Value,
                    };
                    BlueskyRelationships.Assert(prevDate == null || x.Date >= prevDate);
                    if (prevDate == null)
                    {
                        if (checkGaps && x.Date > new DateTime(2022, 11, 18)) throw new Exception("PLC directory should start at 2022-11-17");
                        prevDate = x.Date;
                    }
                    x.TrustedDid = BlueskyRelationships.DeserializeDidPlcFromUInt128(Unsafe.BitCast<DuckDbUuid, UInt128>(x.PlcAsUInt128));
                    var delta = x.Date - prevDate.Value;
                    if (delta < TimeSpan.Zero) throw AssertionLiteException.Throw("Negative time delta in parquet PLC bundle import");

                    if (checkGaps)
                    {
                        var maxAllowedGap =
                            x.Date < new DateTime(2023, 2, 20, 0, 0, 0, DateTimeKind.Utc) ? TimeSpan.FromDays(6) :
                            x.Date < new DateTime(2023, 3, 1, 0, 0, 0, DateTimeKind.Utc) ? TimeSpan.FromHours(24) :
                            TimeSpan.FromHours(5);

                        if (delta > maxAllowedGap)
                        {
                            throw new Exception("Excessive gap between PLC directory entries: " + prevDate + " delta: " + delta + ". Are any files missing?");
                        }
                    }
                    prevDate = x.Date;
                    return x;
                });
            await RetrievePlcDirectoryAsync(AsAsyncEnumerable(rows), ctx);
        }

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
        private static async IAsyncEnumerable<T> AsAsyncEnumerable<T>(IEnumerable<T> input)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
        {
            foreach (var value in input)
            {
                yield return value;
            }
        }
        private async Task RetrievePlcDirectoryAsync(IAsyncEnumerable<DidDocProto> sortedEntries, RequestContext ctx)
        {
            DateTime lastRetrievedDidDoc = default;
            var entries = new List<DidDocProto>();
            var pdsCache = new Dictionary<string, Pds>();
            void FlushBatch()
            {
                if (entries.Count == 0) return;

                var didHashes = new DuckDbUuid[entries.Count];
                Parallel.For(0, didHashes.Length, i =>
                {
                    var did = entries[i].TrustedDid!;
                    BlueskyEnrichedApis.EnsureValidDid(did);
                    if (!did.StartsWith("did:plc:", StringComparison.Ordinal)) AssertionLiteException.Throw("Invalid did:plc in PLC directory.");
                    didHashes[i] = StringUtils.HashUnicodeToUuid(did);
                });
                LogInfo("Flushing " + entries.Count + " PLC directory entries (" + lastRetrievedDidDoc.ToString("yyyy-MM-dd") + ")");

                WithRelationshipsWriteLock(rels =>
                {
                    rels.AvoidFlushes++; // We'll perform many writes, avoid frequent intermediate flushes.
                    var didResumeWrites = false;
                    try
                    {
                        foreach (var (index, entry) in entries.Index())
                        {
                            if (index == entries.Count - 1)
                            {
                                // Last entry of the batch, allow the flushes to happen (if necessary)
                                rels.AvoidFlushes--;
                                didResumeWrites = true;

                            }

                            var didHash = didHashes[index];
                            var mightHavePreviousDidDoc = true;
                            if (!rels.DidHashToUserId.TryGetSingleValue(didHash, out var plc))
                            {
                                plc = rels.AddDidPlcMappingCore(entry.TrustedDid!, didHash);
                                mightHavePreviousDidDoc = false;
                            }

                            var prev = mightHavePreviousDidDoc ? rels.TryGetLatestDidDoc(plc) : null;
                            if (entry.EarliestDateApprox16 == null && entry.Date != default)
                                entry.EarliestDateApprox16 = (entry.Date < ApproximateDateTime16.MinValueAsDateTime ? ApproximateDateTime16.MinValue : ((ApproximateDateTime16)entry.Date)).Value;
                            if (prev != null && prev.EarliestDateApprox16 < entry.EarliestDateApprox16)
                            {
                                entry.EarliestDateApprox16 = prev.EarliestDateApprox16;
                            }
                            rels.CompressDidDoc(entry, pdsCache);
                            rels.DidDocs.AddRange(plc, entry.SerializeToBytes());

                            rels.IndexHandle(entry.Handle, entry.TrustedDid!, ctx, plcHint: plc);
                        }
                        LogInfo("PLC directory entries flushed.");
                    }
                    finally
                    {
                        if (!didResumeWrites)
                            rels.AvoidFlushes--;
                    }
                    rels.DidDocs.Flush();
                    rels.ProfileSearchPrefix2.Flush();
                    rels.ProfileSearchPrefix8.Flush();

                    rels.LastRetrievedPlcDirectoryEntry.Add(lastRetrievedDidDoc, 0);
                    rels.PlcDirectorySyncDate = lastRetrievedDidDoc;
                }, ctx);

                entries.Clear();
            }

            var batchFlushes = 0;
            try
            {
                await foreach (var entry in sortedEntries)
                {
                    if (entry.IsSpam) continue;
                    entries.Add(entry);
                    lastRetrievedDidDoc = entry.Date;

                    if (entries.Count >= 50_000)
                    {
                        FlushBatch();
                        batchFlushes++;
                        await Task.Delay(500);
                    }
                }
            }
            finally
            {
                FlushBatch();
            }

        }

        public static async IAsyncEnumerable<DidDocProto> EnumeratePlcDirectoryAsync(DateTime lastRetrievedDidDoc)
        {
            while (true)
            {
                LogInfo("Fetching PLC directory: " + lastRetrievedDidDoc.ToString("o"));
                using var stream = await BlueskyEnrichedApis.DefaultHttpClient.GetStreamAsync(BlueskyEnrichedApis.PlcDirectoryPrefix + "/export?count=1000&after=" + lastRetrievedDidDoc.ToString("o"));
                var prevLastRetrievedDidDoc = lastRetrievedDidDoc;
                var itemsInPage = 0;
                await foreach (var entry in JsonSerializer.DeserializeAsyncEnumerable<PlcDirectoryEntry>(stream, topLevelValues: true))
                {
                    yield return DidDocToProto(entry!);
                    itemsInPage++;
                    if (entry!.createdAt > lastRetrievedDidDoc)
                    {
                        // PLC directory contains items with the same createdAt.
                        // However ?after= is inclusive, so we don't risk losing entries via pagination.
                        lastRetrievedDidDoc = entry.createdAt;
                    }
                    else if (entry.createdAt < lastRetrievedDidDoc)
                    {
                        throw new Exception("PLC directory createdAt goes backwards in time");
                    }
                }

                if (lastRetrievedDidDoc == prevLastRetrievedDidDoc)
                {
                    // We had a page full of IDs all with the same createdAt.
                    // We'll lose something, but there's no other way of retrieving the missing ones.
                    lastRetrievedDidDoc = lastRetrievedDidDoc.AddTicks(TimeSpan.TicksPerMillisecond);
                }
                if (itemsInPage == 0)
                {
                    Log("PLC directory returned no items.");
                    break;
                }


                if ((DateTime.UtcNow - lastRetrievedDidDoc).TotalSeconds < 60)
                {
                    Log("PLC directory sync completed.");
                    break;
                }

                await Task.Delay(500);
            }
        }


        public static DidDocProto DidDocToProto(PlcDirectoryEntry entry)
        {
            var operation = entry.operation;

            var size = 0;
            size += operation.service?.Length ?? 0;
            size += operation.services?.atproto_labeler?.endpoint?.Length ?? 0;
            size += operation.services?.atproto_pds?.endpoint?.Length ?? 0;
            size += operation.handle?.Length ?? 0;
            size += operation.alsoKnownAs?.Sum(x => x.Length) ?? 0;

            var proto = DidDocToProto(
                operation.service ?? operation.services?.atproto_pds?.endpoint,
                operation.services?.atproto_labeler?.endpoint,
                operation.handle != null ? ["at://" + operation.handle] : operation.alsoKnownAs!,
                entry.did,
                entry.createdAt);
            proto.OriginalPayloadApproximateSize = size;
            return proto;
        }


        public static DidDocProto DidDocToProto(DidWebRoot root)
        {
            return DidDocToProto(
                root.service.FirstOrDefault(x => x.id == "#atproto_pds")?.serviceEndpoint,
                root.service.FirstOrDefault(x => x.id == "#atproto_labeler")?.serviceEndpoint,
                root.handle != null ? ["at://" + root.handle] : root.alsoKnownAs, null, default);
        }
        public static DidDocProto DidDocToProto(string? pds, string? labeler, string[] akas, string? trustedDid, DateTime date)
        {
            var proto = new DidDocProto
            {
                Date = date,
                TrustedDid = trustedDid,
                AtProtoLabeler = labeler,
                Pds = pds
            };

            var handles = new List<string>();
            var other = new List<string>();
            if (akas != null)
            {
                foreach (var aka in akas)
                {
                    if (string.IsNullOrEmpty(aka)) continue;
                    if (aka.Length > 1024) continue;
                    if (aka.StartsWith("at://", StringComparison.Ordinal))
                    {
                        var handle = aka.Substring(5);
                        if (Regex.IsMatch(handle, @"^[\w\.\-]{1,}$"))
                        {
                            handles.Add(handle);
                        }
                        else
                            LogInfo("Invalid handle: " + handle);

                    }
                    else
                    {
                        other.Add(aka);
                    }
                }
            }

            if (handles.Count == 1)
            {
                var handle = handles[0];
                if (handle.EndsWith(".bsky.social", StringComparison.Ordinal))
                {
                    proto.BskySocialUserName = handle.Substring(0, handle.Length - ".bsky.social".Length);
                }
                else
                {
                    proto.CustomDomain = handle;
                }
            }
            else if (handles.Count > 1)
            {
                proto.MultipleHandles = handles.ToArray();
            }

            if (other.Count != 0)
                proto.OtherUrls = other.ToArray();

            return proto;
        }


        public static DedicatedThreadPoolScheduler? FirehoseThreadpool;
        public static TaskFactory? FirehoseThreadpoolTaskFactory;

        private readonly static long LagBehindWarnIntervalMs = AppViewLiteConfiguration.GetInt64(AppViewLiteParameter.APPVIEWLITE_FIREHOSE_PROCESSING_LAG_WARN_INTERVAL_MS) ?? 3000;
        public readonly static long LagBehindWarnThreshold = AppViewLiteConfiguration.GetInt64(AppViewLiteParameter.APPVIEWLITE_FIREHOSE_PROCESSING_LAG_WARN_THRESHOLD) ?? 1000;

        private static long RecordsReceived;
        private static long RecordsProcessed;
        private static Stopwatch? LastLagBehindWarningPrint;
        private readonly static Lock FirehoseLagBehindWarnLock = new();
        public static void InitializeFirehoseThreadpool(BlueskyEnrichedApis apis)
        {
            var ct = apis.DangerousUnlockedRelationships.ShutdownRequestedCts.Token;
            var scheduler = new DedicatedThreadPoolScheduler(Environment.ProcessorCount, "Firehose processing thread");
            scheduler.BeforeTaskEnqueued += () =>
            {
                BlueskyRelationships.FirehoseEventReceivedTimeSeries.Increment();
                var received = Interlocked.Increment(ref RecordsReceived);
                var processed = Interlocked.Read(in RecordsProcessed);

                var lagBehind = received - processed;
                BlueskyRelationships.FirehoseProcessingLagBehindTimeSeries.SetMaximum((int)lagBehind);
            };
            scheduler.AfterTaskProcessed += () =>
            {
                BlueskyRelationships.FirehoseEventProcessedTimeSeries.Increment();
                Interlocked.Increment(ref RecordsProcessed);
            };
            FirehoseThreadpool = scheduler;
            FirehoseThreadpoolTaskFactory = new TaskFactory(scheduler);
        }

        public static async void RunOnFirehoseProcessingThreadpoolFireAndForget<T>(Func<Task<T>> func)
        {
            RunOnFirehoseProcessingThreadpool(func).FireAndForget();
        }

        public static async Task<T> RunOnFirehoseProcessingThreadpool<T>(Func<Task<T>> func)
        {
            if (TaskScheduler.Current == Indexer.FirehoseThreadpoolTaskFactory!.Scheduler)
            {
                await Task.Yield();
                return await func();
            }
            else
            {
                return await Indexer.FirehoseThreadpoolTaskFactory.StartNew(() => func()).Unwrap();
            }
        }

        internal async Task<Tid> ImportFollowersBackfillAsync(string did, RequestContext? authenticatedCtx, Tid previousRecentmostFollow, RequestContext ctx, Action<CarImportProgress> progress, CancellationToken ct)
        {
            // Three strategies:
            // 1) did == Logged in user: look at the rkey in followers[].viewer.followedBy (fastest), then double check with follower's PDS if followee's PDS can't be trusted to be honest
            // 2) did != Logged in user: request with limit=1. The cursor will be the rkey that we're looking for. Rude, but perhaps not worse than fetching all the PDS follow collections (mostly also hosted on bsky infra)
            // 3) did != Logged in user (not enabled by default): Schedule a fetch of all the follows of the alleged followers, so we find their follow rkeys.

            Tid? recentmostFollow = null;

            string? cursor = null;
            var done = false;
            var followRkeysToCheck = new List<(string Did, Tid FollowRkey)>();
            var fetchFolloweesForProfiles = new List<string>();

            Tid? prevFollowRkey = null;
            var recordCount = 0;
            var onePerPage = authenticatedCtx == null;
            while (!done)
            {
                async Task<(GetFollowersOutput Response, bool Trusted)> protocolFunc(ATProtoClient protocol)
                {
                    ct.ThrowIfCancellationRequested();
                    var response = await protocol.AppBskyGraphGetFollowersAsync(new GetFollowersParameters 
                    {
                        Actor = new ATDid(did), 
                        Limit = onePerPage ? 1 : 100, 
                        Cursor = cursor
                    }, ct);
                    return (response, PdsIsTrustedToProvideRealBackreferences(protocol));
                }

                var (response, isTrustedPds) = authenticatedCtx != null ? await Apis.PerformPdsActionAsync(protocolFunc, authenticatedCtx) : await protocolFunc(Apis.CreateQuickBackfillProtocol());
                cursor = response.Cursor;
                if (cursor == null) break;

                if (onePerPage)
                    await Task.Delay(1500, ct); // bsky rate limit is 3000 requests per 5 minutes (we could technically do 10 reqs per second)

                foreach (var follower in response.Followers)
                {
                    ct.ThrowIfCancellationRequested();
                    var followerDid = follower.Did.DidAsString;


                    var followRkeyString =
                        follower.Viewer?.FollowedBy?.Rkey ??
                        (onePerPage && response.Followers.Count == 1 ? response.Cursor : null);

                    if (followRkeyString != null)
                    {
                        var followRkey = Tid.Parse(followRkeyString);
                        if (prevFollowRkey != null && prevFollowRkey.Value.CompareTo(followRkey) < 0)
                            throw new Exception("The server returned followers that were not sorted by follow rkey.");
                        if (previousRecentmostFollow != default && followRkey.CompareTo(previousRecentmostFollow) >= 0)
                        {
                            done = true;
                            break;
                        }
                        recordCount++;
                        progress(new CarImportProgress(0, 0, recordCount, recordCount, default));
                        recentmostFollow ??= followRkey;
                        if (isTrustedPds)
                        {
                            OnRecordCreated(follower.Did.DidAsString, new(Follow.RecordType, followRkeyString), new Follow
                            {
                                Subject = new ATDid(did),
                                CreatedAt = follower.CreatedAt!.Value
                            }.ToJson(), ctx: ctx, isRepositoryImport: true);
                        }
                        else
                        {
                            followRkeysToCheck.Add((followerDid, followRkey));
                        }
                        prevFollowRkey = followRkey;
                    }
                    else if (authenticatedCtx != null)
                    {
                        // probably can't happen
                    }
                    else
                    {
                        recordCount++;
                        progress(new CarImportProgress(0, 0, recordCount, recordCount, default));
                        fetchFolloweesForProfiles.Add(followerDid);
                    }
                }

            }

            if (followRkeysToCheck.Count != 0)
            {
                followRkeysToCheck = WithRelationshipsLockForDids(followRkeysToCheck.Select(x => x.Did).ToArray(), (_, rels) =>
                {
                    var followeePlc = rels.SerializeDid(did, ctx);
                    return followRkeysToCheck.Where(x =>
                    {
                        var followerPlc = rels.SerializeDid(x.Did, ctx);
                        return !rels.Follows.creations.Contains(followeePlc, new Relationship(followerPlc, x.FollowRkey));
                    }).ToList();
                }, ctx);
                await Parallel.ForEachAsync(followRkeysToCheck, new ParallelOptions() { MaxDegreeOfParallelism = 1, CancellationToken = ct }, async (x, ct) =>
                {
                    try
                    {
                        //Console.Error.WriteLine($"Checking follower PDS ({x.Did}) for alleged followee ({did}), rkey: {x.FollowRkey.ToString()}");

                        var follow = (await Apis.GetRecordAsync(x.Did, Follow.RecordType, x.FollowRkey.ToString()!, ctx, ct)).Value;
                        OnRecordCreated(x.Did, new(Follow.RecordType, x.FollowRkey), follow, ctx: ctx, isRepositoryImport: true);
                    }
                    catch (Exception ex)
                    {
                        LogNonCriticalException($"  Could not verify with follower PDS that alleged follower ({x.Did}) returned by untrusted followee PDS ({did}) is actually real. Rkey: " + x.FollowRkey, ex);
                    }
                });
            }

            if (fetchFolloweesForProfiles.Count != 0)
            {
                var plcs = Apis.WithRelationshipsLockForDids(fetchFolloweesForProfiles.ToArray(), (plcs, rels) => plcs, ctx);
                await Apis.EnsureHaveCollectionsAsync(plcs, RepositoryImportKind.Follows, ctx);
            }

            return recentmostFollow ?? previousRecentmostFollow;

        }

        private static bool PdsIsTrustedToProvideRealBackreferences(ATProtoClient url)
        {
            var instance = url.BaseUrl;
            return instance.HasHostSuffix("bsky.app") || instance.HasHostSuffix("host.bsky.network");
        }
    }
    internal record struct ContinueOutsideLock(Action OutsideLock, Action<BlueskyRelationships> Complete);
    public record struct FirehoseEventCursor(DateTime Date, long Cursor);
}

