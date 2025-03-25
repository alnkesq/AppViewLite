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
using System.Net.Http;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Reflection.Metadata;
using DuckDbSharp;
using System.Runtime.CompilerServices;
using DuckDbSharp.Types;
using FishyFlip.Lexicon.Com.Atproto.Label;
using System.Buffers;
using System.Runtime.InteropServices;
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

        public void OnRecordDeleted(string commitAuthor, string path, bool ignoreIfDisposing = false, RequestContext? ctx = null)
        {
            using var _ = CreateIngestionThreadPriorityScope();

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
                        relationships.Follows.Delete(rel, deletionDate, preresolved.followTarget);
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
                        relationships.ThreadgateDeletions.Add(new PostId(rel.Actor, rel.RelationshipRKey), deletionDate);
                    }
                    else if (collection == Postgate.RecordType)
                    {
                        relationships.PostgateDeletions.Add(new PostId(rel.Actor, rel.RelationshipRKey), deletionDate);
                    }
                    else if (collection == Listblock.RecordType)
                    {
                        relationships.ListBlockDeletions.Add(new Relationship(rel.Actor, rel.RelationshipRKey), deletionDate);
                    }
                    //else LogInfo("Deletion of unknown object type: " + collection);
                }

                relationships.MaybeGlobalFlush();
            }), ctx);
        }





        private static bool HasNumericRKey(string path)
        {
            // Some spam bots?
            // Avoid noisy exceptions.
            var rkey = path.Split('/')[1];
            return long.TryParse(rkey, out _) || rkey.StartsWith("follow_", StringComparison.Ordinal);
        }






        public void OnRecordCreated(string commitAuthor, string path, ATObject record, bool generateNotifications = false, bool ignoreIfDisposing = false, RequestContext? ctx = null)
        {
            using var priorityScope = CreateIngestionThreadPriorityScope();

            if (Apis.AdministrativeBlocklist.ShouldBlockIngestion(commitAuthor)) return;

            var now = DateTime.UtcNow;

            ContinueOutsideLock? continueOutsideLock = null;
            ctx ??= RequestContext.CreateForFirehose("Create:" + record.Type, allowStale: true);

            var preresolved = WithRelationshipsLock(rels =>
            {
                if (ignoreIfDisposing && rels.IsDisposed) return default;

                var commitPlc = rels.TrySerializeDidMaybeReadOnly(commitAuthor, ctx);
                if (commitPlc == default) return default;
                return commitPlc;
            }, ctx);


            ctx.AllowStale = false;
            WithRelationshipsWriteLock(relationships =>
            {
                if (ignoreIfDisposing && relationships.IsDisposed) return;
                try
                {
                    if (!generateNotifications) relationships.SuppressNotificationGeneration++;
                    relationships.EnsureNotDisposed();

                    var commitPlc = preresolved != default ? preresolved : relationships.SerializeDid(commitAuthor, ctx);

                    if (commitAuthor.StartsWith("did:web:", StringComparison.Ordinal))
                    {
                        relationships.IndexHandle(null, commitAuthor, ctx);
                    }

                    if (record is Like l)
                    {
                        if (l.Subject!.Uri!.Collection == Post.RecordType)
                        {
                            // quick check to avoid noisy exceptions

                            var postId = relationships.GetPostId(l.Subject, ctx);

                            // So that Likes.GetApproximateActorCount can quickly skip most slices (MaximumKey)
                            BlueskyRelationships.EnsureNotExcessivelyFutureDate(postId.PostRKey);

                            var likeRkey = GetMessageTid(path, Like.RecordType + "/");
                            if (relationships.Likes.Add(postId, new Relationship(commitPlc, likeRkey)))
                            {
                                relationships.AddNotification(postId, NotificationKind.LikedYourPost, commitPlc, ctx, likeRkey.Date);

                                // TODO: here we perform the binary search twice: 1 for HasActor, 2 for GetApproximateActorCount.
                                var approxActorCount = relationships.Likes.GetApproximateActorCount(postId);
                                relationships.MaybeIndexPopularPost(postId, "likes", approxActorCount, BlueskyRelationships.SearchIndexPopularityMinLikes);
                                relationships.NotifyPostStatsChange(postId, commitPlc);



                                relationships.IncrementRecentPopularPostLikeCount(postId, 1);

                                if (relationships.IsRegisteredForNotifications(commitPlc))
                                    relationships.SeenPosts.Add(commitPlc, new PostEngagement(postId, PostEngagementKind.LikedOrBookmarked));
                            }
                        }
                        else if (l.Subject.Uri.Collection == Generator.RecordType)
                        {
                            // TODO: handle deletions for feed likes
                            var feedId = new RelationshipHashedRKey(relationships.SerializeDid(l.Subject.Uri.Did!.Handler, ctx), l.Subject.Uri.Rkey);

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
                        var followed = relationships.SerializeDid(f.Subject!.Handler, ctx);
                        var rkey = GetMessageTid(path, Follow.RecordType + "/");

                        if (relationships.Follows.Add(followed, new Relationship(commitPlc, rkey)))
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
                        var postId = relationships.GetPostId(r.Subject!, ctx);
                        BlueskyRelationships.EnsureNotExcessivelyFutureDate(postId.PostRKey);

                        var repostRKey = GetMessageTid(path, Repost.RecordType + "/");
                        if (relationships.Reposts.Add(postId, new Relationship(commitPlc, repostRKey)))
                        {
                            relationships.AddNotification(postId, NotificationKind.RepostedYourPost, commitPlc, ctx, repostRKey.Date);

                            relationships.MaybeIndexPopularPost(postId, "reposts", relationships.Reposts.GetApproximateActorCount(postId), BlueskyRelationships.SearchIndexPopularityMinReposts);
                            relationships.UserToRecentReposts.Add(commitPlc, new RecentRepost(repostRKey, postId));
                            relationships.NotifyPostStatsChange(postId, commitPlc);

                            if (relationships.IsRegisteredForNotifications(commitPlc))
                                relationships.SeenPosts.Add(commitPlc, new PostEngagement(postId, PostEngagementKind.LikedOrBookmarked));
                        }
                    }
                    else if (record is Block b)
                    {
                        relationships.Blocks.Add(relationships.SerializeDid(b.Subject!.Handler, ctx), new Relationship(commitPlc, GetMessageTid(path, Block.RecordType + "/")));

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
                    }
                    else if (record is Threadgate threadGate)
                    {
                        var rkey = GetMessageTid(path, Threadgate.RecordType + "/");
                        if (threadGate.Post!.Did!.Handler != commitAuthor) throw new UnexpectedFirehoseDataException("Threadgate for non-owned thread.");
                        if (threadGate.Post.Rkey != rkey.ToString()) throw new UnexpectedFirehoseDataException("Threadgate with mismatching rkey.");
                        if (threadGate.Post.Collection != Post.RecordType) throw new UnexpectedFirehoseDataException("Threadgate in non-threadgate collection.");
                        relationships.Threadgates.AddRange(new PostId(commitPlc, rkey), relationships.SerializeThreadgateToBytes(threadGate, ctx));
                    }
                    else if (record is Postgate postgate)
                    {
                        var rkey = GetMessageTid(path, Postgate.RecordType + "/");
                        if (postgate.Post!.Did!.Handler != commitAuthor) throw new UnexpectedFirehoseDataException("Postgate for non-owned post.");
                        if (postgate.Post.Rkey != rkey.ToString()) throw new UnexpectedFirehoseDataException("Postgate with mismatching rkey.");
                        if (postgate.Post.Collection != Post.RecordType) throw new UnexpectedFirehoseDataException("Threadgate in non-postgate collection.");
                        relationships.Postgates.AddRange(new PostId(commitPlc, rkey), relationships.SerializePostgateToBytes(postgate, ctx));
                    }
                    else if (record is Listblock listBlock)
                    {
                        var blockId = new Relationship(commitPlc, GetMessageTid(path, Listblock.RecordType + "/"));
                        if (listBlock.Subject!.Collection != List.RecordType) throw new UnexpectedFirehoseDataException("Listblock in non-listblock collection.");
                        var listId = new Relationship(relationships.SerializeDid(listBlock.Subject.Did!.Handler, ctx), Tid.Parse(listBlock.Subject.Rkey));

                        relationships.ListBlocks.Add(blockId, listId);
                    }
                    else if (record is Generator generator)
                    {
                        var rkey = GetMessageRKey(path, Generator.RecordType + "/");
                        relationships.IndexFeedGenerator(commitPlc, rkey, generator, now);
                    }
                    //else LogInfo("Creation of unknown object type: " + path);
                    relationships.MaybeGlobalFlush();
                }
                finally
                {
                    if (!generateNotifications) relationships.SuppressNotificationGeneration--;
                }
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

            VerifyValidForCurrentRelay!(e.Record.Did!.ToString());

            if (e.Record.Commit?.Operation is ATWebSocketCommitType.Create or ATWebSocketCommitType.Update)
            {
                OnRecordCreated(e.Record.Did!.ToString(), e.Record.Commit.Collection + "/" + e.Record.Commit.RKey, e.Record.Commit.Record!, generateNotifications: true, ignoreIfDisposing: true);
            }
            if (e.Record.Commit?.Operation == ATWebSocketCommitType.Delete)
            {
                OnRecordDeleted(e.Record.Did!.ToString(), e.Record.Commit.Collection + "/" + e.Record.Commit.RKey, ignoreIfDisposing: true);
            }
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
            await PluggableProtocol.RetryInfiniteLoopAsync(async ct =>
            {
                var tcs = new TaskCompletionSource();
                using var firehose = new ATJetStreamBuilder().WithInstanceUrl(FirehoseUrl).WithLogger(new LogWrapper()).Build();
                using var accounting = new LagBehindAccounting(Apis);
                using var watchdog = CreateFirehoseWatchdog(tcs);
                ct.Register(() =>
                {
                    tcs.TrySetCanceled();
                    firehose.Dispose();
                });
                firehose.OnRawMessageReceived += (s, e) =>
                {
                    // Called synchronously by the firehose socket reader
                    accounting.OnRecordReceived();
                };
                firehose.OnConnectionUpdated += (_, e) =>
                {
                    if (ct.IsCancellationRequested) return;

                    if (!(e.State is System.Net.WebSockets.WebSocketState.Open or System.Net.WebSockets.WebSocketState.Connecting))
                    {
                        tcs.TrySetException(new UnexpectedFirehoseDataException("Firehose is in state: " + e.State));
                    }
                };
                firehose.OnRecordReceived += (s, e) =>
                {
                    // Called from a Task.Run(() => ...) by the firehose socket reader
                    TryProcessRecord(() =>
                    {
                        OnJetStreamEvent(e);
                        watchdog.Kick();
                    }, e.Record.Did?.Handler, accounting);
                };
                await firehose.ConnectAsync(token: ct);
                await tcs.Task;
            }, ct);

        }


        public Task StartListeningToAtProtoFirehoseRepos(CancellationToken ct = default)
        {
            return StartListeningToAtProtoFirehoseCore(protocol => protocol.StartSubscribeReposAsync(token: ct), (protocol, watchdog, accounting) =>
            {
                protocol.OnSubscribedRepoMessage += (s, e) => TryProcessRecord(() =>
                {
                    OnRepoFirehoseEvent(s, e);
                    watchdog.Kick();
                }, e.Message.Commit?.Repo?.Handler, accounting);
            }, ct);
        }
        public Task StartListeningToAtProtoFirehoseLabels(string nameForDebugging, CancellationToken ct = default)
        {
            return StartListeningToAtProtoFirehoseCore(protocol => protocol.StartSubscribeLabelsAsync(token: ct), (protocol, watchdog, accounting) =>
            {
                protocol.OnSubscribedLabelMessage += (s, e) => TryProcessRecord(() =>
                {
                    OnLabelFirehoseEvent(s, e);
                    watchdog.Kick();
                }, nameForDebugging, accounting);
            }, ct);
        }
        private async Task StartListeningToAtProtoFirehoseCore(Func<ATWebSocketProtocol, Task> subscribeKind, Action<ATWebSocketProtocol, Watchdog, LagBehindAccounting> setupHandler, CancellationToken ct = default)
        {
            await Task.Yield();
            await PluggableProtocol.RetryInfiniteLoopAsync(async ct =>
            {
                var tcs = new TaskCompletionSource();
                using var firehose = new ATWebSocketProtocolBuilder().WithInstanceUrl(FirehoseUrl).WithLogger(new LogWrapper()).Build();
                using var accounting = new LagBehindAccounting(Apis);
                using var watchdog = CreateFirehoseWatchdog(tcs);
                ct.Register(() =>
                {
                    tcs.TrySetCanceled();
                    firehose.Dispose();
                });
                firehose.OnMessageReceived += (s, e) =>
                {
                    // Called synchronously by the firehose socket reader
                    accounting.OnRecordReceived();
                };
                firehose.OnConnectionUpdated += (_, e) =>
                {
                    if (ct.IsCancellationRequested) return;

                    if (!(e.State is System.Net.WebSockets.WebSocketState.Open or System.Net.WebSockets.WebSocketState.Connecting))
                    {
                        tcs.TrySetException(new Exception("Firehose is in state: " + e.State));
                    }
                };
                setupHandler(firehose, watchdog, accounting);
                await subscribeKind(firehose);
                await tcs.Task;
            }, ct);

        }

        private Watchdog CreateFirehoseWatchdog(TaskCompletionSource tcs)
        {
            // TODO: temporary debugging code
            return new Watchdog(TimeSpan.FromSeconds(120), () =>
            {
                File.AppendAllText(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile) + "/firehose-watchdog.txt", DateTime.UtcNow.ToString("O") + " " + FirehoseUrl + "\n");
                tcs.TrySetException(new Exception("Firehose watchdog"));
            });
        }

        private void OnRepoFirehoseEvent(object? sender, SubscribedRepoEventArgs e)
        {
            var record = e.Message.Record;
            var commitAuthor = e.Message.Commit?.Repo!.Handler;
            if (commitAuthor == null) return;
            var message = e.Message;

            VerifyValidForCurrentRelay!(commitAuthor);

            foreach (var del in (message.Commit?.Ops ?? []).Where(x => x.Action == "delete"))
            {
                OnRecordDeleted(commitAuthor, del.Path!, ignoreIfDisposing: true);
            }

            if (record != null)
            {
                OnRecordCreated(commitAuthor, message.Commit!.Ops![0].Path!, record, generateNotifications: true, ignoreIfDisposing: true);
            }
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


        }

        private void OnLabelCreated(string labeler, Label label, RequestContext ctx)
        {
            var uri = new ATUri(label.Uri);
            if (string.IsNullOrEmpty(label.Val))
                throw new ArgumentException();

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
                        rels.PostLabels.Add(rels.GetPostId(uri, ctx), entry);
                    }
                }
                else
                {
                    rels.ProfileLabels.Add(rels.SerializeDid(uri.Did!.Handler, ctx), entry);
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

                    TryProcessRecord(() => OnRecordCreated(record.Did, record.Path, record.Record), record.Did);
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

        private static void TryProcessRecord(Action action, string? authorForDebugging, LagBehindAccounting? accounting = null)
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
            finally
            {
                accounting?.OnRecordProcessed();
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

        public async Task<(Tid LastTid, Exception? Exception)> IndexUserCollectionAsync(string did, string recordType, Tid since, RequestContext ctx, CancellationToken ct = default, Action<CarImportProgress>? progress = null)
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
                            OnRecordCreated(did, item.Uri.Pathname.Substring(1), item.Value);
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
            var rows = mem.Execute<DidDocProto>($"from '{parquetFileOrDirectory}' where Date >= ?", prevDate ?? new DateTime(1980, 1, 1, 0, 0, 0, DateTimeKind.Utc))
                .Select(x =>
                {
                    if (prevDate == null)
                    {
                        if (checkGaps && x.Date > new DateTime(2022, 11, 18)) throw new Exception("PLC directory should start at 2022-11-17");
                        prevDate = x.Date;
                    }
                    x.TrustedDid = BlueskyRelationships.DeserializeDidPlcFromUInt128(Unsafe.BitCast<DuckDbUuid, UInt128>(x.PlcAsUInt128));
                    var delta = x.Date - prevDate.Value;
                    if (delta < TimeSpan.Zero) throw new Exception();

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
            void FlushBatch()
            {
                if (entries.Count == 0) return;


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

                            var plc = rels.SerializeDid(entry.TrustedDid!, ctx);
                            rels.CompressDidDoc(entry);
                            rels.DidDocs.AddRange(plc, entry.SerializeToBytes());

                            rels.IndexHandle(entry.Handle, entry.TrustedDid!, ctx);
                        }
                        LogInfo("PLC directory entries flushed.");
                    }
                    finally
                    {
                        if (!didResumeWrites)
                            rels.AvoidFlushes--;
                    }

                    rels.LastRetrievedPlcDirectoryEntry.Add(lastRetrievedDidDoc, 0);
                    rels.PlcDirectorySyncDate = lastRetrievedDidDoc;
                }, ctx);


                entries.Clear();
            }

            try
            {
                await foreach (var entry in sortedEntries)
                {
                    entries.Add(entry);
                    lastRetrievedDidDoc = entry.Date;

                    if (entries.Count >= 50000)
                    {
                        FlushBatch();
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
            return DidDocToProto(
                operation.service ?? operation.services?.atproto_pds?.endpoint,
                operation.services?.atproto_labeler?.endpoint,
                operation.handle != null ? ["at://" + operation.handle] : operation.alsoKnownAs,
                entry.did,
                entry.createdAt);
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



        private static string LimitLength(string handle)
        {
            if (handle.Length > 100) handle = string.Concat(handle.AsSpan(0, 50), "...");
            return handle;
        }

        public class LagBehindAccounting : IDisposable
        {
            public long RecordsReceived;
            public long RecordsProcessed;
            private bool disposed;


            public void OnRecordReceived()
            {

                BlueskyRelationships.FirehoseEventReceivedTimeSeries.Increment();
                var received = Interlocked.Increment(ref RecordsReceived);

                if (disposed) return;

                var processed = Interlocked.Read(in RecordsProcessed);

                var lagBehind = received - processed;
                BlueskyRelationships.FirehoseProcessingLagBehindTimeSeries.Set((int)lagBehind);
                if (lagBehind >= LagBehindErrorThreshold && !Debugger.IsAttached)
                {

                    Interlocked.Decrement(ref RecordsReceived);
                    var errorText = "Unable to process the firehose quickly enough, giving up. Lagging behind: " + lagBehind;
                    if (LagBehindErrorDropEvents)
                    {
                        lock (this)
                        {
                            if (LastDropEventsWarningPrint != null && LastDropEventsWarningPrint.ElapsedMilliseconds < 5000)
                                throw new UnexpectedFirehoseDataException(errorText);
                            LastDropEventsWarningPrint ??= Stopwatch.StartNew();
                            LastDropEventsWarningPrint.Restart();
                        }

                        throw new Exception(errorText);
                    }
                    else
                    {
                        apis.WithRelationshipsWriteLock(rels => rels.GlobalFlush(), RequestContext.CreateForFirehose("FlushBeforeLagBehindExit"));
                        BlueskyRelationships.ThrowFatalError(errorText);
                    }
                }


                if ((RecordsReceived % 30) == 0)
                {
                    if (lagBehind >= LagBehindWarnThreshold)
                    {
                        lock (this)
                        {
                            if (LastLagBehindWarningPrint != null && LastLagBehindWarningPrint.ElapsedMilliseconds < LagBehindWarnIntervalMs)
                                return;
                            LastLagBehindWarningPrint ??= Stopwatch.StartNew();
                            LastLagBehindWarningPrint.Restart();
                        }
                        LogInfo($"Struggling to process the firehose quickly enough, lagging behind: {lagBehind} ({processed}/{received}, {(100.0 * processed / received):0.0}%)");
                    }
                }


            }
            private Stopwatch? LastLagBehindWarningPrint;
            private Stopwatch? LastDropEventsWarningPrint;
            private BlueskyEnrichedApis apis;

            public void OnRecordProcessed()
            {
                BlueskyRelationships.FirehoseEventProcessedTimeSeries.Increment();
                Interlocked.Increment(ref RecordsProcessed);
            }

            public void Dispose()
            {
                disposed = true;
            }

            public readonly static long LagBehindWarnIntervalMs = AppViewLiteConfiguration.GetInt64(AppViewLiteParameter.APPVIEWLITE_FIREHOSE_PROCESSING_LAG_WARN_INTERVAL_MS) ?? 500;
            public readonly static long LagBehindWarnThreshold = AppViewLiteConfiguration.GetInt64(AppViewLiteParameter.APPVIEWLITE_FIREHOSE_PROCESSING_LAG_WARN_THRESHOLD) ?? 100;
            public readonly static long LagBehindErrorThreshold = AppViewLiteConfiguration.GetInt64(AppViewLiteParameter.APPVIEWLITE_FIREHOSE_PROCESSING_LAG_ERROR_THRESHOLD) ?? 10000;
            public readonly static bool LagBehindErrorDropEvents = AppViewLiteConfiguration.GetBool(AppViewLiteParameter.APPVIEWLITE_FIREHOSE_PROCESSING_LAG_ERROR_DROP_EVENTS) ?? false;

            public LagBehindAccounting(BlueskyEnrichedApis apis)
            {
                this.apis = apis;
            }
        }
    }
    internal record struct ContinueOutsideLock(Action OutsideLock, Action<BlueskyRelationships> Complete);
}

