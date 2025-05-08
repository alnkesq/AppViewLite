using FishyFlip.Lexicon.App.Bsky.Feed;
using FishyFlip.Lexicon.App.Bsky.Graph;
using FishyFlip.Lexicon;
using FishyFlip.Models;
using AppViewLite.Models;
using System;
using FishyFlip.Lexicon.App.Bsky.Actor;
using Relationship = AppViewLite.Models.Relationship;
using FishyFlip.Events;
using System.Linq;
using System.Threading.Tasks;
using FishyFlip;
using AppViewLite.Numerics;
using System.IO;
using FishyFlip.Tools;
using System.Threading;
using System.Diagnostics;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.RegularExpressions;
using DuckDbSharp;
using System.Runtime.CompilerServices;
using DuckDbSharp.Types;
using FishyFlip.Lexicon.Com.Atproto.Label;
using AppViewLite.PluggableProtocols;

namespace AppViewLite
{
    public class Indexer : BlueskyRelationshipsClientBase
    {
        public Uri FirehoseUrl = new("https://bsky.network/");
        public BlueskyEnrichedApis Apis;
        public Indexer(BlueskyEnrichedApis apis)
            : base(apis.primarySecondaryPair)
        {
            this.Apis = apis;
        }

        internal static Action? CaptureFirehoseCursors;

        public void OnRecordDeleted(string commitAuthor, string path, bool ignoreIfDisposing = false, RequestContext? ctx = null)
        {
            if (Apis.AdministrativeBlocklist.ShouldBlockIngestion(commitAuthor)) return;

            var slash = path!.IndexOf('/');
            var collection = path.Substring(0, slash);
            var rkey = path.Substring(slash + 1);
            var deletionDate = DateTime.UtcNow;
            ctx ??= RequestContext.CreateForFirehose("Delete:" + collection, allowStale: true /* only temporarily, will be disabled in a moment*/);

            var rkeyAsTid = Tid.TryParse(rkey, out var parsedTid) ? parsedTid : default;


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
                    relationships.FeedGeneratorDeletions.Add(new RelationshipHashedRKey(commitPlc, rkey), deletionDate);
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
                        relationships.Threadgates.AddRange(new PostId(commitPlc, rel.RelationshipRKey), relationships.SerializeThreadgateToBytes(new Threadgate(), ctx, out _));
                    }
                    else if (collection == Postgate.RecordType)
                    {
                        relationships.Postgates.AddRange(new PostId(commitPlc, rel.RelationshipRKey), relationships.SerializePostgateToBytes(new Postgate(), ctx, out _));
                    }
                    else if (collection == Listblock.RecordType)
                    {
                        relationships.ListBlockDeletions.Add(new Relationship(rel.Actor, rel.RelationshipRKey), deletionDate);
                    }
                    //else LogInfo("Deletion of unknown object type: " + collection);
                }
            }), ctx);
        }





        private static bool HasNumericRKey(string path)
        {
            // Some spam bots?
            // Avoid noisy exceptions.
            var rkey = path.Split('/')[1];
            return long.TryParse(rkey, out _) || rkey.StartsWith("follow_", StringComparison.Ordinal);
        }






        public void OnRecordCreated(string commitAuthor, string path, ATObject record, bool ignoreIfDisposing = false, RequestContext? ctx = null, bool isRepositoryImport = false)
        {
            if (Apis.AdministrativeBlocklist.ShouldBlockIngestion(commitAuthor)) return;
            var now = DateTime.UtcNow;

            ContinueOutsideLock? continueOutsideLock = null;
            ctx ??= RequestContext.CreateForFirehose("Create:" + record.Type, allowStale: true);

            var preresolved = WithRelationshipsLock(rels =>
            {
                if (ignoreIfDisposing && rels.IsDisposed) return default;

                var commitPlc = rels.TrySerializeDidMaybeReadOnly(commitAuthor, ctx);
                if (commitPlc == default) return default;

                Plc subjectPlc = default;
                bool relationshipIsAbsent = false;

                if (record is Like like && like.Subject!.Uri.Collection == Post.RecordType)
                {
                    subjectPlc = rels.TrySerializeDidMaybeReadOnly(like.Subject!.Uri.Did!.Handler, ctx);
                    if (subjectPlc != default && !rels.Likes.HasActor(new PostIdTimeFirst(Tid.Parse(like.Subject.Uri.Rkey), subjectPlc), commitPlc, out _))
                        relationshipIsAbsent = true;
                }
                else if (record is Repost repost && repost.Subject!.Uri.Collection == Post.RecordType)
                {
                    subjectPlc = rels.TrySerializeDidMaybeReadOnly(repost.Subject!.Uri.Did!.Handler, ctx);
                    if (subjectPlc != default && !rels.Reposts.HasActor(new PostIdTimeFirst(Tid.Parse(repost.Subject.Uri.Rkey), subjectPlc), commitPlc, out _))
                        relationshipIsAbsent = true;
                }
                else if (record is Follow follow)
                {
                    subjectPlc = rels.TrySerializeDidMaybeReadOnly(follow.Subject!.Handler, ctx);
                    if (subjectPlc != default && !rels.Follows.HasActor(subjectPlc, commitPlc, out _))
                        relationshipIsAbsent = true;
                }


                return (commitPlc, subjectPlc, relationshipIsAbsentAsOf: relationshipIsAbsent ? rels.Version : 0);
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
                    if (l.Subject!.Uri!.Collection == Post.RecordType)
                    {
                        // quick check to avoid noisy exceptions

                        var postId = relationships.GetPostId(l.Subject, ctx, hint: preresolved.subjectPlc);

                        // So that Likes.GetApproximateActorCount can quickly skip most slices (MaximumKey)
                        BlueskyRelationships.EnsureNotExcessivelyFutureDate(postId.PostRKey);

                        var likeRkey = GetMessageTid(path, Like.RecordType + "/");
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



                            relationships.IncrementRecentPopularPostLikeCount(postId, null);

                            if (relationships.IsRegisteredForNotifications(commitPlc))
                                relationships.SeenPosts.Add(commitPlc, new PostEngagement(postId, PostEngagementKind.LikedOrBookmarked));
                        }
                    }
                    else if (l.Subject.Uri.Collection == Generator.RecordType)
                    {
                        // TODO: handle deletions for feed likes
                        var feedId = new RelationshipHashedRKey(relationships.SerializeDidWithHint(l.Subject.Uri.Did!.Handler, ctx, preresolved.subjectPlc), l.Subject.Uri.Rkey);

                        var likeRkey = GetMessageTid(path, Like.RecordType + "/");
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
                    var followed = relationships.SerializeDidWithHint(f.Subject!.Handler, ctx, preresolved.subjectPlc);
                    var rkey = GetMessageTid(path, Follow.RecordType + "/");

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

                    var repostRKey = GetMessageTid(path, Repost.RecordType + "/");
                    if (relationships.Reposts.Add(postId, new Relationship(commitPlc, repostRKey), preresolved.relationshipIsAbsentAsOf))
                    {
                        relationships.AddNotification(postId, NotificationKind.RepostedYourPost, commitPlc, ctx, repostRKey.Date);

                        relationships.MaybeIndexPopularPost(postId, "reposts", relationships.Reposts.GetApproximateActorCount(postId), BlueskyRelationships.SearchIndexPopularityMinReposts);
                        relationships.UserToRecentReposts.Add(commitPlc, new RecentRepost(repostRKey, postId));
                        relationships.NotifyPostStatsChange(postId, commitPlc);

                        if (relationships.IsRegisteredForNotifications(commitPlc))
                            relationships.SeenPosts.Add(commitPlc, new PostEngagement(postId, PostEngagementKind.LikedOrBookmarked));
                    }

                    relationships.AddRepostToRecentRepostCache(commitPlc, new RecentRepost(repostRKey, postId));
                }
                else if (record is Block b)
                {
                    var blockedUser = relationships.SerializeDid(b.Subject!.Handler, ctx);
                    var blockRkey = GetMessageTid(path, Block.RecordType + "/");
                    relationships.Blocks.Add(blockedUser, new Relationship(commitPlc, blockRkey));
                    relationships.AddNotification(blockedUser, NotificationKind.BlockedYou, commitPlc, ctx, blockRkey.Date);
                }
                else if (record is Post p)
                {
                    var postId = new PostId(commitPlc, GetMessageTid(path, Post.RecordType + "/"));
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
                else if (record is Profile pf && GetMessageRKey(path, Profile.RecordType) == "/self")
                {
                    relationships.StoreProfileBasicInfo(commitPlc, pf, ctx);
                }
                else if (record is List list)
                {
                    relationships.Lists.AddRange(new Relationship(commitPlc, GetMessageTid(path, List.RecordType + "/")), BlueskyRelationships.SerializeProto(BlueskyRelationships.ListToProto(list)));
                }
                else if (record is Listitem listItem)
                {
                    if (commitAuthor != listItem.List!.Did!.Handler) throw new UnexpectedFirehoseDataException("Listitem for non-owned list.");
                    if (listItem.List.Collection != List.RecordType) throw new UnexpectedFirehoseDataException("Listitem in non-listitem collection.");
                    var listRkey = Tid.Parse(listItem.List.Rkey);
                    var listItemRkey = GetMessageTid(path, Listitem.RecordType + "/");
                    var member = relationships.SerializeDid(listItem.Subject!.Handler, ctx);

                    var listId = new Relationship(commitPlc, listRkey);
                    var entry = new ListEntry(member, listItemRkey);

                    relationships.ListItems.Add(listId, entry);
                    relationships.ListMemberships.Add(entry.Member, new ListMembership(commitPlc, listRkey, listItemRkey));
                    relationships.AddNotification(entry.Member, NotificationKind.AddedYouToAList, commitPlc, listRkey, ctx, entry.ListItemRKey.Date);

                    if (!isRepositoryImport && !relationships.HaveCollectionForUser(commitPlc, RepositoryImportKind.ListEntries))
                    {
                        Indexer.RunOnFirehoseProcessingThreadpool(async () =>
                        {
                            var result = await Apis.EnsureHaveCollectionAsync(commitPlc, RepositoryImportKind.ListEntries, ctx, slowImport: true);
                            if (result != null && result.Error != null)
                            {
                                Log($"Could not fetch full ListEntries collection for " + commitAuthor + ": " + result.Error);
                            }
                            return result;
                        }).FireAndForget();
                    }
                }
                else if (record is Threadgate threadGate)
                {
                    var rkey = GetMessageTid(path, Threadgate.RecordType + "/");
                    if (threadGate.Post!.Did!.Handler != commitAuthor) throw new UnexpectedFirehoseDataException("Threadgate for non-owned thread.");
                    if (threadGate.Post.Rkey != rkey.ToString()) throw new UnexpectedFirehoseDataException("Threadgate with mismatching rkey.");
                    if (threadGate.Post.Collection != Post.RecordType) throw new UnexpectedFirehoseDataException("Threadgate in non-threadgate collection.");
                    relationships.Threadgates.AddRange(new PostId(commitPlc, rkey), relationships.SerializeThreadgateToBytes(threadGate, ctx, out var threadgateProto));
                    if (threadgateProto.HiddenReplies != null)
                    {
                        foreach (var hiddenReply in threadgateProto.HiddenReplies)
                        {
                            var replyId = hiddenReply.PostId;
                            var replyRkey = replyId.PostRKey;
                            var now = threadGate.CreatedAt ?? replyRkey.Date;
                            relationships.AddNotificationDateInvariant(replyId.Author, NotificationKind.HidYourReply, commitPlc, replyRkey, ctx, now, replyRkey.Date < now ? replyRkey.Date : now);
                        }
                    }
                }
                else if (record is Postgate postgate)
                {
                    var rkey = GetMessageTid(path, Postgate.RecordType + "/");
                    if (postgate.Post!.Did!.Handler != commitAuthor) throw new UnexpectedFirehoseDataException("Postgate for non-owned post.");
                    if (postgate.Post.Rkey != rkey.ToString()) throw new UnexpectedFirehoseDataException("Postgate with mismatching rkey.");
                    if (postgate.Post.Collection != Post.RecordType) throw new UnexpectedFirehoseDataException("Threadgate in non-postgate collection.");
                    relationships.Postgates.AddRange(new PostId(commitPlc, rkey), relationships.SerializePostgateToBytes(postgate, ctx, out var proto));
                    if (proto.DetachedEmbeddings != null)
                    {
                        foreach (var detachedQuote in proto.DetachedEmbeddings)
                        {
                            var quoterId = detachedQuote.PostId;
                            var quoterRkey = quoterId.PostRKey;
                            var now = postgate.CreatedAt ?? quoterRkey.Date;
                            relationships.AddNotificationDateInvariant(quoterId.Author, NotificationKind.DetachedYourQuotePost, commitPlc, quoterRkey, ctx, now, quoterRkey.Date < now ? quoterRkey.Date : now);
                        }
                    }
                }
                else if (record is Listblock listBlock)
                {
                    var blockId = new Relationship(commitPlc, GetMessageTid(path, Listblock.RecordType + "/"));
                    if (listBlock.Subject!.Collection != List.RecordType) throw new UnexpectedFirehoseDataException("Listblock in non-listblock collection.");
                    var listId = new Relationship(relationships.SerializeDid(listBlock.Subject.Did!.Handler, ctx), Tid.Parse(listBlock.Subject.Rkey));

                    relationships.ListBlocks.Add(blockId, listId);
                    relationships.ListSubscribers.Add(listId, blockId);
                }
                else if (record is Generator generator)
                {
                    var rkey = GetMessageRKey(path, Generator.RecordType + "/");
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


        private ConcurrentSet<string> currentlyRunningRecordRetrievals = new();
        private void ScheduleRecordIndexing(ATUri uri, RequestContext ctx)
        {
            if (!currentlyRunningRecordRetrievals.TryAdd(uri.ToString())) return;
            Task.Run(async () =>
            {
                LogInfo("Fetching record " + uri);

                var record = (await Apis.GetRecordAsync(uri.Did!.Handler, uri.Collection, uri.Rkey, ctx));

                OnRecordCreated(uri.Did.Handler, uri.Pathname.Substring(1), record, ignoreIfDisposing: true);

                currentlyRunningRecordRetrievals.Remove(uri.ToString());

            });
        }

        public void OnJetStreamEvent(JetStreamATWebSocketRecordEventArgs e)
        {
            if (!OnFirehoseEventBeginProcessing()) return;

            if (e.Record.Account is { } acct)
            {
                OnAccountStateChanged(acct.Did.Handler, acct.Active, acct.Status);
                return;
            }

            VerifyValidForCurrentRelay!(e.Record.Did!.ToString());

            if (e.Record.Commit?.Operation is ATWebSocketCommitType.Create or ATWebSocketCommitType.Update)
            {
                OnRecordCreated(e.Record.Did!.ToString(), e.Record.Commit.Collection + "/" + e.Record.Commit.RKey, e.Record.Commit.Record!, ignoreIfDisposing: true);
            }
            if (e.Record.Commit?.Operation == ATWebSocketCommitType.Delete)
            {
                OnRecordDeleted(e.Record.Did!.ToString(), e.Record.Commit.Collection + "/" + e.Record.Commit.RKey, ignoreIfDisposing: true);
            }

            BumpLargestSeenFirehoseCursor(e.Record.TimeUs!.Value, DateTime.UnixEpoch.AddMicroseconds(e.Record.TimeUs.Value));
        }

        private bool OnFirehoseEventBeginProcessing()
        {
            if (Apis.ShutdownRequested.IsCancellationRequested) return false;

            var received = Interlocked.Read(ref RecordsReceived);
            var processed = Interlocked.Read(ref RecordsProcessed);
            var lagBehind = received - processed;
            if (lagBehind >= LagBehindErrorThreshold && !Debugger.IsAttached)
            {

                if (LagBehindErrorDropEvents)
                {
                    lock (FirehoseLagBehindWarnLock)
                    {
                        if (LastDropEventsWarningPrint == null || LastDropEventsWarningPrint.ElapsedMilliseconds > 5000)
                        {
                            Log("Unable to process the firehose quickly enough, dropping events. Lagging behind: " + lagBehind);
                            LastDropEventsWarningPrint ??= Stopwatch.StartNew();
                            LastDropEventsWarningPrint.Restart();
                        }


                    }

                    return false;
                }
                else
                {
                    // We must not block here, because we're inside a suspendable dedicated threadpool task (we would deadlock)
                    Task.Run(() =>
                    {
                        Apis.GlobalFlush("FlushBeforeLagBehindExit");
                        BlueskyRelationships.ThrowFatalError("Unable to process the firehose quickly enough, giving up. Lagging behind: " + lagBehind);
                    });

                    throw new Exception("Unable to process the firehose quickly enough.");
                }
            }


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

        public static string GetMessageRKey(SubscribeRepoMessage message, string prefix)
        {
            var first = message.Commit!.Ops![0].Path!;
            return GetMessageRKey(first, prefix);
        }
        public static string GetMessageRKey(string path, string prefix)
        {
            if (!path.StartsWith(prefix, StringComparison.Ordinal)) throw new UnexpectedFirehoseDataException($"Expecting path prefix {prefix}, but found {path}");
            var postShortId = path.Substring(prefix.Length);
            return postShortId;
        }

        public static Tid GetMessageTid(SubscribeRepoMessage message, string prefix) => Tid.Parse(GetMessageRKey(message, prefix));
        public static Tid GetMessageTid(string path, string prefix) => Tid.Parse(GetMessageRKey(path, prefix));

        public async Task StartListeningToJetstreamFirehose(CancellationToken ct = default)
        {
            await Task.Yield();
            CaptureFirehoseCursors += CaptureFirehoseCursor;
            await PluggableProtocol.RetryInfiniteLoopAsync(FirehoseUrl.AbsoluteUri, async ct =>
            {
                try
                {
                    var tcs = new TaskCompletionSource();
                    var options = new ATJetStreamBuilder()
                        .WithInstanceUrl(FirehoseUrl)
                        .WithLogger(new LogWrapper())
                        .WithTaskFactory(FirehoseThreadpoolTaskFactory!);

                    this.currentFirehoseCursor = relationshipsUnlocked.GetOrCreateFirehoseCursorThreadSafe(FirehoseUrl.AbsoluteUri); currentFirehoseCursor.State = FirehoseState.Starting;
                    currentFirehoseCursor.State = FirehoseState.Starting;

                    LogFirehoseStartMessage(currentFirehoseCursor);
                    if (this.currentFirehoseCursor.CommittedCursor != null)
                    {
                        options.WithCursor(long.Parse(currentFirehoseCursor.CommittedCursor));
                    }
                    using var firehose = options.Build();
                    using var watchdog = CreateFirehoseWatchdog(tcs);
                    ct.Register(() =>
                    {
                        tcs.TrySetCanceled();
                        firehose.Dispose();
                    });
                    firehose.OnConnectionUpdated += (_, e) =>
                    {
                        if (ct.IsCancellationRequested) return;

                        if (!(e.State is System.Net.WebSockets.WebSocketState.Open or System.Net.WebSockets.WebSocketState.Connecting))
                        {
                            tcs.TrySetException(new UnexpectedFirehoseDataException("Firehose is in state: " + e.State));
                        }
                    };
                    firehose.OnRawMessageReceived += (s, e) =>
                    {
                        Interlocked.Increment(ref currentFirehoseCursor.ReceivedEvents);
                        DedicatedThreadPoolScheduler.NotifyTaskAboutToBeEnqueuedCanBeSuspended();
                    };
                    firehose.OnRecordReceived += (s, e) =>
                    {
                        // Called from a Task.Run(() => ...) by the firehose socket reader
                        TryProcessRecord(() =>
                        {
                            OnJetStreamEvent(e);
                            watchdog?.Kick();
                        }, e.Record.Did?.Handler);
                    };
                    await firehose.ConnectAsync(token: ct);
                    currentFirehoseCursor.State = FirehoseState.Running;
                    await tcs.Task;
                }
                catch (Exception ex)
                {
                    currentFirehoseCursor!.State = FirehoseState.Error;
                    currentFirehoseCursor.LastException = ex;
                    throw;
                }
                finally
                {
                    if (!ShutdownRequested.IsCancellationRequested)
                        Apis.DrainAndCaptureFirehoseCursors();
                }
            }, ct);

            CaptureFirehoseCursors -= CaptureFirehoseCursor;
        }

        private void LogFirehoseStartMessage(FirehoseCursor currentFirehoseCursor)
        {
            if (currentFirehoseCursor.CommittedCursor == null)
                Log($"Starting firehose {FirehoseUrl} at current time.");
            else
                Log($"Starting firehose {FirehoseUrl} at cursor '{currentFirehoseCursor.CommittedCursor}' (~{currentFirehoseCursor.LastSeenEventDate}, {StringUtils.ToHumanTimeSpan(DateTime.UtcNow - currentFirehoseCursor.LastSeenEventDate, showSeconds: true)} ago)");
        }

        public Task StartListeningToAtProtoFirehoseRepos(RetryPolicy? retryPolicy, bool useWatchdog = true, CancellationToken ct = default)
        {
            return StartListeningToAtProtoFirehoseCore((protocol, cursor) => protocol.StartSubscribeReposAsync(cursor, token: ct), (protocol, cursor, watchdog) =>
            {
                protocol.OnMessageReceived += (s, e) =>
                {
                    Interlocked.Increment(ref cursor.ReceivedEvents);
                    DedicatedThreadPoolScheduler.NotifyTaskAboutToBeEnqueuedCanBeSuspended();
                };
                protocol.OnSubscribedRepoMessage += (s, e) => TryProcessRecord(() =>
                {
                    OnRepoFirehoseEvent(s, e);
                    watchdog?.Kick();
                }, e.Message.Commit?.Repo?.Handler);
            }, retryPolicy, useApproximateFirehoseCapture: false, useWatchdog: useWatchdog, ct: ct);
        }
        public Task StartListeningToAtProtoFirehoseLabels(string nameForDebugging, CancellationToken ct = default)
        {
            return StartListeningToAtProtoFirehoseCore((protocol, cursor) => protocol.StartSubscribeLabelsAsync(cursor, token: ct), (protocol, cursor, watchdog) =>
            {
                protocol.OnMessageReceived += (s, e) =>
                {
                    Interlocked.Increment(ref cursor.ReceivedEvents);
                    DedicatedThreadPoolScheduler.NotifyTaskAboutToBeEnqueuedCanBeSuspended();
                };
                protocol.OnSubscribedLabelMessage += (s, e) => TryProcessRecord(() =>
                {
                    OnLabelFirehoseEvent(s, e);
                    watchdog?.Kick();
                }, nameForDebugging);
            }, RetryPolicy.CreateForUnreliableServer(), useApproximateFirehoseCapture: true, useWatchdog: false, ct);
        }

        private void CaptureFirehoseCursor()
        {
            if (largestSeenFirehoseCursor == 0) return;
            currentFirehoseCursor!.CommittedCursor = largestSeenFirehoseCursor.ToString();
            currentFirehoseCursor.CursorCommitDate = DateTime.UtcNow;
            LogInfo($"Capturing cursor for {FirehoseUrl} = '{largestSeenFirehoseCursor}'");
        }

        private async Task StartListeningToAtProtoFirehoseCore(Func<ATWebSocketProtocol, long?, Task> subscribeKind, Action<ATWebSocketProtocol, FirehoseCursor, Watchdog?> setupHandler, RetryPolicy? retryPolicy, bool useApproximateFirehoseCapture, bool useWatchdog = true, CancellationToken ct = default)
        {
            await Task.Yield();
            CaptureFirehoseCursors += CaptureFirehoseCursor;

            await PluggableProtocol.RetryInfiniteLoopAsync(FirehoseUrl.AbsoluteUri, async ct =>
            {
                try
                {
                    var tcs = new TaskCompletionSource();
                    this.currentFirehoseCursor = relationshipsUnlocked.GetOrCreateFirehoseCursorThreadSafe(FirehoseUrl.AbsoluteUri);
                    currentFirehoseCursor.State = FirehoseState.Starting;

                    LogFirehoseStartMessage(currentFirehoseCursor);
                    using var firehose = new ATWebSocketProtocolBuilder()
                        .WithInstanceUrl(FirehoseUrl)
                        .WithLogger(new LogWrapper())
                        .WithTaskFactory(FirehoseThreadpoolTaskFactory!)
                        .Build();
                    using var watchdog = useWatchdog ? CreateFirehoseWatchdog(tcs) : null;
                    ct.Register(() =>
                    {
                        tcs.TrySetCanceled();
                        firehose.StopSubscriptionAsync();
                        firehose.Dispose();
                    });
                    firehose.OnConnectionUpdated += (_, e) =>
                    {
                        if (ct.IsCancellationRequested) return;

                        if (!(e.State is System.Net.WebSockets.WebSocketState.Open or System.Net.WebSockets.WebSocketState.Connecting))
                        {
                            tcs.TrySetException(new Exception("Firehose is in state: " + e.State));
                        }
                    };
                    setupHandler(firehose, currentFirehoseCursor, watchdog);
                    await subscribeKind(firehose, currentFirehoseCursor.CommittedCursor != null ? long.Parse(currentFirehoseCursor.CommittedCursor) : null);
                    currentFirehoseCursor.State = FirehoseState.Running;
                    await tcs.Task;
                }
                catch (Exception ex)
                {
                    currentFirehoseCursor!.State = FirehoseState.Error;
                    currentFirehoseCursor.LastException = ex;
                    throw;
                }
                finally
                {
                    if (!ShutdownRequested.IsCancellationRequested)
                    {
                        if (useApproximateFirehoseCapture)
                        {
                            CaptureFirehoseCursor();
                        }
                        else Apis.DrainAndCaptureFirehoseCursors();
                    }
                }
            }, ct, retryPolicy: retryPolicy);

            CaptureFirehoseCursors -= CaptureFirehoseCursor;
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


        private long largestSeenFirehoseCursor;

        private void BumpLargestSeenFirehoseCursor(long cursor, DateTime eventDate)
        {
            while (true)
            {
                var oldCursor = this.largestSeenFirehoseCursor;
                if (oldCursor >= cursor) break;

                Interlocked.CompareExchange(ref largestSeenFirehoseCursor, cursor, oldCursor);
            }
            currentFirehoseCursor!.LastSeenEventDate = eventDate;
        }

        internal FirehoseCursor? currentFirehoseCursor;

        private void OnRepoFirehoseEvent(object? sender, SubscribedRepoEventArgs e)
        {
            if (!OnFirehoseEventBeginProcessing()) return;

            if (e.Message.Account is { } acct)
            {
                OnAccountStateChanged(acct.Did!.Handler, acct.Active, null); // TODO: pass status instead of null
                return;
            }

            var commitAuthor = e.Message.Commit?.Repo!.Handler;
            if (commitAuthor == null) return;
            var message = e.Message;

            VerifyValidForCurrentRelay!(commitAuthor);

            foreach (var op in message.Commit?.Ops ?? [])
            {
                if (op.Action == "delete")
                {
                    OnRecordDeleted(commitAuthor, op.Path!, ignoreIfDisposing: true);
                }
                else
                {
                    var record = e.Message.Records!.First(x => x.Cid == op.Cid).Value;
                    OnRecordCreated(commitAuthor, op.Path!, record, ignoreIfDisposing: true);
                }
            }
            
            var seq = e.Message.Commit!.Seq;
            BumpLargestSeenFirehoseCursor(seq, e.Message.Commit.Time!.Value);
        }

        private void OnAccountStateChanged(string did, bool active, string? status)
        {
            VerifyValidForCurrentRelay!(did);
            var ctx = RequestContext.CreateForFirehose("FirehoseAccountState");
            Apis.SetAccountState(did, active, status, ctx);
        }

        private void OnLabelFirehoseEvent(object? sender, SubscribedLabelEventArgs e)
        {
            var labels = e.Message.Labels?.LabelsValue;
            if (labels == null) return;

            var ctx = RequestContext.CreateForFirehose("Label");
            foreach (var label in labels)
            {
                VerifyValidForCurrentRelay!(label.Src.Handler);
                OnLabelCreated(label.Src.Handler, label, ctx);
            }

            this.BumpLargestSeenFirehoseCursor(e.Message!.Labels!.Seq, e.Message.Labels!.LabelsValue.Last().Cts!.Value);

        }

        private void OnLabelCreated(string labeler, Label label, RequestContext ctx)
        {
            var uri = new ATUri(label.Uri);
            if (string.IsNullOrEmpty(label.Val))
                throw new ArgumentException("OnLabelCreated: label is null or empty");

            // LogInfo("Label: " + label.Val +  " to " + label.Uri + " (from " + labeler + " via " + this.FirehoseUrl + ")");
            WithRelationshipsWriteLock(rels =>
            {

                var entry = new LabelEntry(rels.SerializeDid(labeler, ctx), (ApproximateDateTime32)(label.Cts ?? DateTime.UtcNow), BlueskyRelationships.HashLabelName(label.Val), label.Neg ?? false);

                if (!rels.LabelNames.ContainsKey(entry.KindHash))
                {
                    rels.LabelNames.AddRange(entry.KindHash, System.Text.Encoding.UTF8.GetBytes(label.Val));
                }

                if (!string.IsNullOrEmpty(uri.Pathname) && uri.Pathname != "/")
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
                    var labeledUser = rels.SerializeDid(uri.Did!.Handler, ctx);
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
#pragma warning disable CA2000 // Must be disposed by caller
            if (!stream.CanSeek) stream = new PositionAwareStream(stream);
#pragma warning restore CA2000
            using var importer = new CarImporter(did, probableDateOfEarliestRecord ?? await GetProbableDateOfEarliestRecordAsync(did, ctx), relationshipsUnlocked.CarSpillDirectory);
            importer.Log("Reading stream");

            await foreach (var item in CarDecoder.DecodeCarAsync(stream).ConfigureAwait(false))
            {
                ct.ThrowIfCancellationRequested();
                //Console.Error.WriteLine(item.Cid + " = " + Convert.ToHexString(item.Cid.ToArray()));

                importer.OnCarDecoded(new CarProgressStatusEvent(item.Cid, item.Bytes));
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

                var parts = record.Path.Split('/');
                if (parts.Length == 2 && Tid.TryParse(parts[1], out var tid))
                    progress?.Invoke(new CarImportProgress(totalSize, totalSize, recordCount, importer.TotalRecords, tid));

                await Apis.DangerousUnlockedRelationships.CarRecordInsertionSemaphore.WaitAsync(ct);
                try
                {

                    TryProcessRecord(() => OnRecordCreated(record.Did, record.Path, record.Record, isRepositoryImport: true), record.Did);
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

        private static void TryProcessRecord(Action action, string? authorForDebugging)
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
            using var at = await Apis.CreateProtocolForDidAsync(did, ctx);
            var pds = (await at.ResolveATDidHostAsync(new ATDid(did), ct)).HandleResult()!;
            var url = new Uri(new Uri(pds), FishyFlip.Lexicon.Com.Atproto.Sync.SyncEndpoints.GetRepo + "?did=" + did + (since != default ? "&since=" + since : null));
            return await at.Client.GetStreamAsync(url, ct);

        }

        public async Task<(Tid LastTid, Exception? Exception)> IndexUserCollectionAsync(string did, string recordType, Tid since, RequestContext ctx, CancellationToken ct, Action<CarImportProgress>? progress, bool slowImport)
        {
            using var at = await Apis.CreateProtocolForDidAsync(did, ctx);

            string? cursor = since != default ? since.ToString() : null;
            Tid lastTid = since;
            long recordCount = 0;
            DateTime oldestRecord = default;
            try
            {
                while (true)
                {
                    var page = (await at.Repo.ListRecordsAsync(new ATDid(did), recordType, 100, cursor, reverse: true, cancellationToken: ct)).HandleResult();
                    cursor = page!.Cursor;
                    foreach (var item in page.Records)
                    {
                        recordCount++;

                        await Apis.DangerousUnlockedRelationships.CarRecordInsertionSemaphore.WaitAsync(ct);
                        try
                        {
                            OnRecordCreated(did, item.Uri.Pathname.Substring(1), item.Value, isRepositoryImport: true);
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
                    rels.DidDocs.Flush(false);
                    rels.ProfileSearchPrefix2.Flush(false);
                    rels.ProfileSearchPrefix8.Flush(false);

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
            };


            proto.Pds = pds;

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

        private readonly static long LagBehindWarnIntervalMs = AppViewLiteConfiguration.GetInt64(AppViewLiteParameter.APPVIEWLITE_FIREHOSE_PROCESSING_LAG_WARN_INTERVAL_MS) ?? 500;
        private readonly static long LagBehindWarnThreshold = AppViewLiteConfiguration.GetInt64(AppViewLiteParameter.APPVIEWLITE_FIREHOSE_PROCESSING_LAG_WARN_THRESHOLD) ?? 100;
        public readonly static long LagBehindErrorThreshold = AppViewLiteConfiguration.GetInt64(AppViewLiteParameter.APPVIEWLITE_FIREHOSE_PROCESSING_LAG_ERROR_THRESHOLD) ?? 10000;
        private readonly static bool LagBehindErrorDropEvents = AppViewLiteConfiguration.GetBool(AppViewLiteParameter.APPVIEWLITE_FIREHOSE_PROCESSING_LAG_ERROR_DROP_EVENTS) ?? false;

        private static long RecordsReceived;
        private static long RecordsProcessed;
        private static Stopwatch? LastLagBehindWarningPrint;
        private static Stopwatch? LastDropEventsWarningPrint;
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
                Func<ATProtocol, Task<(GetFollowersOutput Response, bool Trusted)>> protocolFunc = async protocol =>
                {
                    ct.ThrowIfCancellationRequested();
                    var response = (await protocol.GetFollowersAsync(new ATDid(did), onePerPage ? 1 : 100, cursor, ct)).HandleResult()!;
                    return (response, PdsIsTrustedToProvideRealBackreferences(protocol));
                };

                var (response, isTrustedPds) = authenticatedCtx != null ? await Apis.PerformPdsActionAsync(protocolFunc, authenticatedCtx) : await protocolFunc(Apis.CreateQuickBackfillProtocol());
                cursor = response.Cursor;
                if (cursor == null) break;

                if (onePerPage)
                    await Task.Delay(1500, ct); // bsky rate limit is 3000 requests per 5 minutes (we could technically do 10 reqs per second)

                foreach (var follower in response.Followers)
                {
                    ct.ThrowIfCancellationRequested();
                    var followerDid = follower.Did.Handler;


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
                            OnRecordCreated(follower.Did.Handler, Follow.RecordType + "/" + followRkey, new Follow
                            {
                                Subject = new ATDid(did)
                            }, ctx: ctx, isRepositoryImport: true);
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

                        var follow = (Follow)(await Apis.GetRecordAsync(x.Did, Follow.RecordType, x.FollowRkey.ToString()!, ctx, ct)).Value;
                        OnRecordCreated(x.Did, Follow.RecordType + "/" + x.FollowRkey.ToString(), follow, ctx: ctx, isRepositoryImport: true);
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

        private static bool PdsIsTrustedToProvideRealBackreferences(ATProtocol url)
        {
            return url.Options.Url.HasHostSuffix("bsky.app") || url.Options.Url.HasHostSuffix("host.bsky.network");
        }
    }
    internal record struct ContinueOutsideLock(Action OutsideLock, Action<BlueskyRelationships> Complete);
}

