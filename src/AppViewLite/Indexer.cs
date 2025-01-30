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

namespace AppViewLite
{
    public class Indexer : BlueskyRelationshipsClientBase
    {
        public Uri FirehoseUrl = new("https://bsky.network/");
        public HashSet<string>? DidBlocklist;
        public HashSet<string>? DidAllowlist;
        public AtProtocolProvider AlternatePdsConfiguration;
        public Indexer(BlueskyRelationships relationships, AtProtocolProvider altPdsConfig)
            : base(relationships)
        {
            this.AlternatePdsConfiguration = altPdsConfig;
        }

        public void OnRecordDeleted(string commitAuthor, string path, bool ignoreIfDisposing = false)
        {
            WithRelationshipsLock(relationships =>
            {
                if (ignoreIfDisposing && relationships.IsDisposed) return;
                var sw = Stopwatch.StartNew();
                relationships.EnsureNotDisposed();
                var slash = path!.IndexOf('/');
                var collection = path.Substring(0, slash);
                var rkey = path.Substring(slash + 1);
                var deletionDate = DateTime.UtcNow;
                var commitPlc = relationships.SerializeDid(commitAuthor);

                if (collection == Generator.RecordType)
                {
                    relationships.FeedGeneratorDeletions.Add(new RelationshipHashedRKey(commitPlc, rkey), deletionDate);
                }
                else
                {

                    if (!Tid.TryParse(rkey, out var tid)) return;

                    var rel = new Relationship(commitPlc, tid);
                    if (collection == Like.RecordType)
                    {
                        var target = relationships.Likes.Delete(rel, deletionDate);
                        if (target != null) relationships.NotifyPostStatsChange(target.Value, commitPlc);
                    }
                    else if (collection == Follow.RecordType)
                    {
                        relationships.Follows.Delete(rel, deletionDate);
                    }
                    else if (collection == Block.RecordType)
                    {
                        relationships.Blocks.Delete(rel, deletionDate);
                    }
                    else if (collection == Repost.RecordType)
                    {
                        var target = relationships.Reposts.Delete(rel, deletionDate);
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
                    //else Console.Error.WriteLine("Deletion of unknown object type: " + collection);
                }

                relationships.LogPerformance(sw, "Delete-" + path);
                relationships.MaybeGlobalFlush();
            });
        }

        record struct ContinueOutsideLock(Action OutsideLock, Action<BlueskyRelationships> Complete);



        private static bool HasNumericRKey(string path)
        {
            // Some spam bots?
            // Avoid noisy exceptions.
            var rkey = path.Split('/')[1];
            return long.TryParse(rkey, out _);
        }


        private void OnRecordCreated(string commitAuthor, string path, ATObject record, bool generateNotifications = false, bool ignoreIfDisposing = false)
        {
        

            ContinueOutsideLock? continueOutsideLock = null;
            WithRelationshipsLock(relationships =>
            {
                if (ignoreIfDisposing && relationships.IsDisposed) return;
                try
                {
                    if (!generateNotifications) relationships.SuppressNotificationGeneration++;
                    var sw = Stopwatch.StartNew();
                    relationships.EnsureNotDisposed();

                    var commitPlc = relationships.SerializeDid(commitAuthor);

                    if (commitAuthor.StartsWith("did:web:", StringComparison.Ordinal))
                    {
                        relationships.IndexHandle(commitAuthor.Substring(8), commitPlc);
                    }

                    if (record is Like l)
                    {
                        if (l.Subject!.Uri!.Collection == Post.RecordType)
                        {
                            // quick check to avoid noisy exceptions
                            var postId = relationships.GetPostId(l.Subject);

                            relationships.Likes.Add(postId, new Relationship(commitPlc, GetMessageTid(path, Like.RecordType + "/")));
                            relationships.AddNotification(postId, NotificationKind.LikedYourPost, commitPlc);
                            var approxActorCount = relationships.Likes.GetApproximateActorCount(postId);
                            relationships.MaybeIndexPopularPost(postId, "likes", approxActorCount, BlueskyRelationships.SearchIndexPopularityMinLikes);
                            relationships.NotifyPostStatsChange(postId, commitPlc);
                            
                        }
                        else if (l.Subject.Uri.Collection == Generator.RecordType)
                        {
                            // TODO: handle deletions for feed likes
                            var feedId = new RelationshipHashedRKey(relationships.SerializeDid(l.Subject.Uri.Did.Handler), l.Subject.Uri.Rkey);

                            relationships.FeedGeneratorLikes.Add(feedId, new Relationship(commitPlc, GetMessageTid(path, Like.RecordType + "/")));
                            var approxActorCount = relationships.FeedGeneratorLikes.GetApproximateActorCount(feedId);
                            relationships.MaybeIndexPopularFeed(feedId, "likes", approxActorCount, BlueskyRelationships.SearchIndexFeedPopularityMinLikes);
                            relationships.AddNotification(feedId.Plc, NotificationKind.LikedYourFeed, commitPlc, new Tid((long)feedId.RKeyHash) /*evil cast*/);
                            if (!relationships.FeedGenerators.ContainsKey(feedId))
                            {
                                ScheduleRecordIndexing(l.Subject.Uri);
                            }
                        }
                    }
                    else if (record is Follow f)
                    {
                        if (HasNumericRKey(path)) return;
                        var followed = relationships.SerializeDid(f.Subject.Handler);
                        if (relationships.IsRegisteredForNotifications(followed))
                            relationships.AddNotification(followed, relationships.Follows.HasActor(commitPlc, followed, out _) ? NotificationKind.FollowedYouBack : NotificationKind.FollowedYou, commitPlc);
                        var rkey = GetMessageTid(path, Follow.RecordType + "/");
                        relationships.Follows.Add(followed, new Relationship(commitPlc, rkey));
                        if (relationships.IsRegisteredForNotifications(commitPlc))
                        {
                            relationships.RegisteredUserToFollowees.AddIfMissing(commitPlc, new ListEntry(followed, rkey));
                        }

                    }
                    else if (record is Repost r)
                    {
                        var postId = relationships.GetPostId(r.Subject);
                        var repostRKey = GetMessageTid(path, Repost.RecordType + "/");
                        relationships.AddNotification(postId, NotificationKind.RepostedYourPost, commitPlc);
                        relationships.Reposts.Add(postId, new Relationship(commitPlc, repostRKey));
                        relationships.MaybeIndexPopularPost(postId, "reposts", relationships.Reposts.GetApproximateActorCount(postId), BlueskyRelationships.SearchIndexPopularityMinReposts);
                        relationships.UserToRecentReposts.Add(commitPlc, new RecentRepost(repostRKey, postId));
                        relationships.NotifyPostStatsChange(postId, commitPlc);
                    }
                    else if (record is Block b)
                    {
                        relationships.Blocks.Add(relationships.SerializeDid(b.Subject.Handler), new Relationship(commitPlc, GetMessageTid(path, Block.RecordType + "/")));

                    }
                    else if (record is Post p)
                    {
                        var postId = new PostId(commitPlc, GetMessageTid(path, Post.RecordType + "/"));
                        var proto = relationships.StorePostInfoExceptData(p, postId);

                        

                        byte[]? postBytes = null;
                        continueOutsideLock = new ContinueOutsideLock(() => postBytes = BlueskyRelationships.SerializePostData(proto), relationships =>
                        {
                            relationships.PostData.AddRange(postId, postBytes); // double insertions are fine, the second one wins.
                        });

                        //relationships.StorePostInfo(postId, p);
                    }
                    else if (record is Profile pf && GetMessageRKey(path, Profile.RecordType) == "/self")
                    {
                        relationships.StoreProfileBasicInfo(commitPlc, pf);
                    }
                    else if (record is List list)
                    {
                        relationships.Lists.AddRange(new Relationship(commitPlc, GetMessageTid(path, List.RecordType + "/")), BlueskyRelationships.SerializeProto(BlueskyRelationships.ListToProto(list)));
                    }
                    else if (record is Listitem listItem)
                    {
                        if (commitAuthor != listItem.List.Did.Handler) throw new UnexpectedFirehoseDataException("Listitem for non-owned list.");
                        if (listItem.List.Collection != List.RecordType) throw new UnexpectedFirehoseDataException("Listitem in non-listitem collection.");
                        var listRkey = Tid.Parse(listItem.List.Rkey);
                        var listItemRkey = GetMessageTid(path, Listitem.RecordType + "/");
                        var member = relationships.SerializeDid(listItem.Subject.Handler);

                        var listId = new Relationship(commitPlc, listRkey);
                        var entry = new ListEntry(member, listItemRkey);

                        relationships.ListItems.Add(listId, entry);
                        relationships.ListMemberships.Add(entry.Member, new ListMembership(commitPlc, listRkey, listItemRkey));
                    }
                    else if (record is Threadgate threadGate)
                    {
                        var rkey = GetMessageTid(path, Threadgate.RecordType + "/");
                        if (threadGate.Post.Did.Handler != commitAuthor) throw new UnexpectedFirehoseDataException("Threadgate for non-owned thread.");
                        if (threadGate.Post.Rkey != rkey.ToString()) throw new UnexpectedFirehoseDataException("Threadgate with mismatching rkey.");
                        if (threadGate.Post.Collection != Post.RecordType) throw new UnexpectedFirehoseDataException("Threadgate in non-threadgate collection.");
                        relationships.Threadgates.AddRange(new PostId(commitPlc, rkey), relationships.SerializeThreadgateToBytes(threadGate));
                    }
                    else if (record is Postgate postgate)
                    {
                        var rkey = GetMessageTid(path, Postgate.RecordType + "/");
                        if (postgate.Post.Did.Handler != commitAuthor) throw new UnexpectedFirehoseDataException("Postgate for non-owned post.");
                        if (postgate.Post.Rkey != rkey.ToString()) throw new UnexpectedFirehoseDataException("Postgate with mismatching rkey.");
                        if (postgate.Post.Collection != Post.RecordType) throw new UnexpectedFirehoseDataException("Threadgate in non-postgate collection.");
                        relationships.Postgates.AddRange(new PostId(commitPlc, rkey), relationships.SerializePostgateToBytes(postgate));
                    }
                    else if (record is Listblock listBlock)
                    {
                        var blockId = new Relationship(commitPlc, GetMessageTid(path, Listblock.RecordType + "/"));
                        if (listBlock.Subject.Collection != List.RecordType) throw new UnexpectedFirehoseDataException("Listblock in non-listblock collection.");
                        var listId = new Relationship(relationships.SerializeDid(listBlock.Subject.Did.Handler), Tid.Parse(listBlock.Subject.Rkey));

                        relationships.ListBlocks.Add(blockId, listId);
                    }
                    else if (record is Generator generator)
                    {
                        var rkey = GetMessageRKey(path, Generator.RecordType + "/");
                        relationships.IndexFeedGenerator(commitPlc, rkey, generator);
                    }
                    //else Console.Error.WriteLine("Creation of unknown object type: " + path);
                    relationships.LogPerformance(sw, "Create-" + path);
                    relationships.MaybeGlobalFlush();
                }
                finally
                {
                    if (!generateNotifications) relationships.SuppressNotificationGeneration--;
                }
            });

            if (continueOutsideLock != null)
            {
                
                continueOutsideLock.Value.OutsideLock();
                WithRelationshipsLock(relationships =>
                {
                    var sw = Stopwatch.StartNew();
                    continueOutsideLock.Value.Complete(relationships);
                    relationships.LogPerformance(sw, "WritePost");
                });
                
            }
        }


        private ConcurrentSet<string> currentlyRunningRecordRetrievals = new();
        private void ScheduleRecordIndexing(ATUri uri)
        {
            if (!currentlyRunningRecordRetrievals.TryAdd(uri.ToString())) return;
            Task.Run(async () =>
            {
                Console.Error.WriteLine("Fetching record " + uri);
                using var proto = AlternatePdsConfiguration.CreateProtocolForDid(uri.Did!.Handler);
                var record = (await proto.Repo.GetRecordAsync(uri.Did, uri.Collection, uri.Rkey)).HandleResult()!.Value;

                OnRecordCreated(uri.Did.Handler, uri.Pathname.Substring(1), record, ignoreIfDisposing: true);

                currentlyRunningRecordRetrievals.Remove(uri.ToString());

            });
        }

        public void OnJetStreamEvent(JetStreamATWebSocketRecordEventArgs e)
        {

            VerifyValidForCurrentRelay(e.Record.Did.ToString());

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
            var first = message.Commit.Ops[0].Path;
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


        public async Task ListenJetStreamFirehoseAsync()
        {
            using var firehose2 = new ATJetStream(new ATJetStreamOptions());
            firehose2.OnConnectionUpdated += async (_, e) =>
            {
                if (!(e.State is System.Net.WebSockets.WebSocketState.Open or System.Net.WebSockets.WebSocketState.Connecting))
                {
                    Console.Error.WriteLine("CONNECTION DROPPED! Reconnecting soon...");
                    await Task.Delay(5000);
                    await firehose2.ConnectAsync();
                }
            };
            firehose2.OnRecordReceived += (s, e) =>
            {
                TryProcessRecord(() => OnJetStreamEvent(e), e.Record.Did?.Handler);
            };

            await firehose2.ConnectAsync();
            await new TaskCompletionSource().Task;
        }




        public async Task ListenBlueskyFirehoseAsync()
        {

            var firehose = new FishyFlip.ATWebSocketProtocolBuilder().WithInstanceUrl(FirehoseUrl).Build();
            firehose.OnConnectionUpdated += async (_, e) =>
            {
                if (!(e.State is System.Net.WebSockets.WebSocketState.Open or System.Net.WebSockets.WebSocketState.Connecting))
                {
                    Console.Error.WriteLine("CONNECTION DROPPED! Reconnecting soon...");
                    await Task.Delay(5000);
                    await firehose.StartSubscribeReposAsync();
                }
            };
            firehose.OnSubscribedRepoMessage += (s, e) =>
            {
                TryProcessRecord(() => OnFirehoseEvent(e), e.Message.Commit?.Repo.Handler);
            };
            await firehose.StartSubscribeReposAsync();
            await new TaskCompletionSource().Task;
        }

        private void OnFirehoseEvent(SubscribedRepoEventArgs e)
        {
            var record = e.Message.Record;
            var commitAuthor = e.Message.Commit?.Repo!.Handler;
            if (commitAuthor == null) return;
            var message = e.Message;

            VerifyValidForCurrentRelay(commitAuthor);

            foreach (var del in (message.Commit?.Ops ?? []).Where(x => x.Action == "delete"))
            {
                OnRecordDeleted(commitAuthor, del.Path, ignoreIfDisposing: true);
            }

            if (record != null)
            {
                OnRecordCreated(commitAuthor, message.Commit.Ops[0].Path, record, generateNotifications: true, ignoreIfDisposing: true);
            }
        }

        private void VerifyValidForCurrentRelay(string commitAuthor)
        {
            if (DidBlocklist != null && DidBlocklist.Contains(commitAuthor)) throw new UnexpectedFirehoseDataException($"Ignoring {commitAuthor} for firehose {FirehoseUrl}");
            if (DidAllowlist != null && !DidAllowlist.Contains(commitAuthor)) throw new UnexpectedFirehoseDataException($"Ignoring {commitAuthor} for firehose {FirehoseUrl}");
        }

        public async Task<Tid> ImportCarAsync(string did, string carPath)
        {
            using var stream = File.Open(carPath, FileMode.Open, FileAccess.Read, FileShare.Read);
            return await ImportCarAsync(did, stream);
        }

        
        public async Task<Tid> ImportCarAsync(string did, Stream stream)
        {
            var importer = new CarImporter(did);
            importer.Log("Reading stream");

            await CarDecoder.DecodeCarAsync(stream, importer.OnCarDecoded);
            importer.LogStats();
            foreach (var record in importer.EnumerateRecords())
            {
                TryProcessRecord(() => OnRecordCreated(record.Did, record.Path, record.Record), record.Did);
            }
            importer.Log("Done.");
            return importer.LargestSeenRev;
        }

        private static void TryProcessRecord(Action action, string? did)
        {
            try
            {
                action();
            }
            catch (UnexpectedFirehoseDataException ex)
            {
                Console.Error.WriteLine(did + ": " + ex.Message);
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(did + ": " + ex);
            }
        }

        public async Task<Tid> ImportCarAsync(string did, Tid since = default, CancellationToken ct = default)
        {
            using var at = AlternatePdsConfiguration.CreateProtocolForDid(did);
            var importer = new CarImporter(did);
            importer.Log("Reading stream");

            var result = (await at.Sync.GetRepoAsync(new ATDid(did), importer.OnCarDecoded, since: since != default ? since.ToString() : null, cancellationToken: ct)).HandleResult();
            importer.LogStats();
            
            foreach (var record in importer.EnumerateRecords())
            {
                TryProcessRecord(() => OnRecordCreated(record.Did, record.Path, record.Record), did);
                await Task.Delay(10, ct);
            }
            
            importer.Log("Done.");
            return importer.LargestSeenRev;
        }


        public async Task<(Tid LastTid, Exception? Exception)> IndexUserCollectionAsync(string did, string recordType, Tid since, CancellationToken ct = default)
        {
            using var at = AlternatePdsConfiguration.CreateProtocolForDid(did);

            string? cursor = since != default ? since.ToString() : null;
            Tid lastTid = since;
            try
            {
                while (true)
                {
                    var page = (await at.Repo.ListRecordsAsync(new ATDid(did), recordType, 100, cursor, reverse: true, cancellationToken: ct)).HandleResult();
                    cursor = page!.Cursor;
                    foreach (var item in page.Records)
                    {
                        OnRecordCreated(did, item.Uri.Pathname.Substring(1), item.Value);
                        if (Tid.TryParse(item.Uri.Rkey, out var tid))
                            lastTid = tid;
                    }

                    if (cursor == null) break;
                }
                return (lastTid, null);
            }
            catch (Exception ex)
            {
                return (lastTid, ex);
            }
            
        }

        public async Task RetrievePlcDirectoryLoopAsync()
        {
            while (true)
            {
                try
                {
                    await RetrievePlcDirectoryAsync();
                }
                catch (Exception ex)
                {

                    Log("PLC directory sync failed: " + ex.ToString());
                }
                await Task.Delay(TimeSpan.FromMinutes(10));
            }
        }

        private async Task RetrievePlcDirectoryAsync()
        {
            var lastRetrievedDidDoc = WithRelationshipsLock(rels => rels.LastRetrievedPlcDirectoryEntry.MaximumKey) ?? new DateTime(1980, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            using var httpClient = new HttpClient();

            var entries = new List<PlcDirectoryEntry>();

            void FlushBatch()
            {
                if (entries.Count == 0) return;


                var protos = entries.Select(x => (x.did, proto: DidDocToProto(x))).ToArray();
                Log("Flushing " + entries.Count + " PLC directory entries");
                WithRelationshipsLock(rels =>
                {
                    rels.AvoidFlushes++; // We'll perform many writes, avoid frequent intermediate flushes.
                    var didResumeWrites = false;
                    try
                    {
                        foreach (var (index, entry) in protos.Index())
                        {
                            if (index == protos.Length - 1)
                            {
                                // Last entry of the batch, allow the flushes to happen (if necessary)
                                rels.AvoidFlushes--;
                                didResumeWrites = true;

                            }

                            var plc = rels.SerializeDid(entry.did);
                            var handle = entry.proto.Handle;
                            rels.CompressDidDoc(entry.proto);
                            rels.DidDocs.AddRange(plc, BlueskyRelationships.SerializeProto(entry.proto));

                            if (handle != null)
                            {
                                rels.IndexHandle(handle, plc);
                                rels.HandleToPossibleDids.Add(BlueskyRelationships.HashWord(handle), plc);
                            }
                        }
                        Log("PLC directory entries flushed.");
                    }
                    finally
                    {
                        if (!didResumeWrites)
                            rels.AvoidFlushes--;
                    }
                });



                WithRelationshipsLock(rels => rels.LastRetrievedPlcDirectoryEntry.Add(lastRetrievedDidDoc, 0));


                entries.Clear();
            }

            try
            {
                while (true)
                {
                    Log("Fetching PLC directory: " + lastRetrievedDidDoc.ToString("o"));
                    using var stream = await httpClient.GetStreamAsync("https://plc.directory/export?count=1000&after=" + lastRetrievedDidDoc.ToString("o"));
                    var prevLastRetrievedDidDoc = lastRetrievedDidDoc;
                    var itemInPage = 0;
                    await foreach (var entry in JsonSerializer.DeserializeAsyncEnumerable<PlcDirectoryEntry>(stream, topLevelValues: true))
                    {
                        entries.Add(entry!);
                        itemInPage++;
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
                    if (entries!.Count == 0)
                    {
                        Log("PLC directory returned no items.");
                        break;
                    }


                    if ((DateTime.UtcNow - lastRetrievedDidDoc).TotalSeconds < 60)
                    {
                        Log("PLC directory sync completed.");
                        break;
                    }

                    if (entries.Count >= 50000)
                    {
                        FlushBatch();
                    }

                    await Task.Delay(500);
                }
            }
            finally
            {
                FlushBatch();
            }

        }

        private void Log(string v)
        {
            Console.Error.WriteLine(v);
        }

        private static DidDocProto DidDocToProto(PlcDirectoryEntry x)
        {
            var proto = new DidDocProto
            {
                Date = x.createdAt,
            };

            var operation = x.operation;

            proto.Pds = operation.service ?? operation.services?.atproto_pds?.endpoint;

            var handle = operation.handle ?? operation.alsoKnownAs?.FirstOrDefault();


            if (handle != null)
            {
                if (handle.StartsWith("at://", StringComparison.Ordinal))
                    handle = handle.Substring(5);

                if (Regex.IsMatch(handle, @"^[\w\.\-]{1,1000}$"))
                {
                    if (handle.EndsWith(".bsky.social", StringComparison.Ordinal))
                    {
                        proto.BskySocialUserName = handle.Substring(0, handle.Length - ".bsky.social".Length);
                    }
                    else
                    {
                        proto.CustomHandle = handle;
                    }
                }
                else
                {
                    if (handle.Length > 100) handle = string.Concat(handle.AsSpan(0, 50), "...");
                    Console.Error.WriteLine("PLC directory invalid handle: " + handle);
                }
            }

            return proto;
        }
    }
}

