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

namespace AppViewLite
{
    public class Indexer
    {
        private BlueskyRelationships relationships;
        public Indexer(BlueskyRelationships relationships)
        {
            this.relationships = relationships;
        }


        public void OnRecordDeleted(string commitAuthor, string path)
        {
            lock (relationships)
            {
                var sw = Stopwatch.StartNew();
                relationships.EnsureNotDisposed();
                var slash = path!.IndexOf('/');
                var collection = path.Substring(0, slash);
                var rkey = path.Substring(slash + 1);
                var deletionDate = DateTime.UtcNow;
                if (!Tid.TryParse(rkey, out var tid)) return;
                var rel = new Relationship(relationships.SerializeDid(commitAuthor), tid);
                if (collection == Like.RecordType)
                {
                    relationships.Likes.Delete(rel, deletionDate);
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
                    relationships.Reposts.Delete(rel, deletionDate);
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

                relationships.LogPerformance(sw, "Delete-" + path);
            }
        }

        record struct ContinueOutsideLock(Action OutsideLock, Action<BlueskyRelationships> Complete);



        private static bool HasNumericRKey(string path)
        {
            // Some spam bots?
            // Avoid noisy exceptions.
            var rkey = path.Split('/')[1];
            return long.TryParse(rkey, out _);
        }


        private void OnRecordCreated(string commitAuthor, string path, ATObject record)
        {
        

            ContinueOutsideLock? continueOutsideLock = null;
            lock (relationships)
            {
                var sw = Stopwatch.StartNew();
                relationships.EnsureNotDisposed();
                var commitPlc = relationships.SerializeDid(commitAuthor);
                if (record is Like l)
                {
                    if (l.Subject!.Uri!.Collection == Post.RecordType)
                    {
                        // quick check to avoid noisy exceptions
                        var postId = relationships.GetPostId(l.Subject);

                        relationships.Likes.Add(postId, new Relationship(commitPlc, GetMessageTid(path, Like.RecordType + "/")));
                        relationships.AddNotification(postId, NotificationKind.LikedYourPost, commitPlc);
                        relationships.MaybeIndexPopularPost(postId, "likes", relationships.Likes.GetApproximateActorCount(postId), BlueskyRelationships.SearchIndexPopularityMinLikes);
                    }
                }
                else if (record is Follow f)
                {
                    if (HasNumericRKey(path)) return;
                    var followed = relationships.SerializeDid(f.Subject.Handler);
                    relationships.AddNotification(followed, NotificationKind.FollowedYou, commitPlc);
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
                    continueOutsideLock = new ContinueOutsideLock(() => postBytes = BlueskyRelationships.CompressPostDataToBytes(proto), relationships =>
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
                    relationships.Lists.AddRange(new Relationship(commitPlc, GetMessageTid(path, List.RecordType + "/")), BlueskyRelationships.SerializeListToBytes(list));
                }
                else if (record is Listitem listItem)
                {
                    if (commitAuthor != listItem.List.Did.Handler) throw new Exception();
                    if (listItem.List.Collection != List.RecordType) throw new Exception();
                    var listId = new Relationship(commitPlc, Tid.Parse(listItem.List.Rkey));
                    var entry = new ListEntry(relationships.SerializeDid(listItem.Subject.Handler), GetMessageTid(path, Listitem.RecordType + "/"));
                    relationships.ListItems.Add(listId, entry);
                }
                else if (record is Threadgate threadGate)
                {
                    var rkey = GetMessageTid(path, Threadgate.RecordType + "/");
                    if (threadGate.Post.Did.Handler != commitAuthor) throw new Exception();
                    if (threadGate.Post.Rkey != rkey.ToString()) throw new Exception();
                    if (threadGate.Post.Collection != Post.RecordType) throw new Exception();
                    relationships.Threadgates.AddRange(new PostId(commitPlc, rkey), relationships.SerializeThreadgateToBytes(threadGate));
                }
                else if (record is Postgate postgate)
                {
                    var rkey = GetMessageTid(path, Postgate.RecordType + "/");
                    if (postgate.Post.Did.Handler != commitAuthor) throw new Exception();
                    if (postgate.Post.Rkey != rkey.ToString()) throw new Exception();
                    if (postgate.Post.Collection != Post.RecordType) throw new Exception();
                    relationships.Postgates.AddRange(new PostId(commitPlc, rkey), relationships.SerializePostgateToBytes(postgate));
                }
                else if (record is Listblock listBlock)
                {
                    var blockId = new Relationship(commitPlc, GetMessageTid(path, Listblock.RecordType + "/"));
                    if (listBlock.Subject.Collection != List.RecordType) throw new Exception();
                    var listId = new Relationship(relationships.SerializeDid(listBlock.Subject.Did.Handler), Tid.Parse(listBlock.Subject.Rkey));

                    relationships.ListBlocks.Add(blockId, listId);
                }
                //else Console.Error.WriteLine("Creation of unknown object type: " + path);
                relationships.LogPerformance(sw, "Create-" + path);
            }

            if (continueOutsideLock != null)
            {
                
                continueOutsideLock.Value.OutsideLock();
                lock (relationships)
                {
                    var sw = Stopwatch.StartNew();
                    continueOutsideLock.Value.Complete(relationships);
                    relationships.LogPerformance(sw, "WritePost");
                }
                
            }
        }



        public void OnJetStreamEvent(JetStreamATWebSocketRecordEventArgs e)
        {
            if (e.Record.Commit?.Operation is ATWebSocketCommitType.Create or ATWebSocketCommitType.Update)
            {
                OnRecordCreated(e.Record.Did!.ToString(), e.Record.Commit.Collection + "/" + e.Record.Commit.RKey, e.Record.Commit.Record!);
            }
            if (e.Record.Commit?.Operation == ATWebSocketCommitType.Delete)
            {
                OnRecordDeleted(e.Record.Did!.ToString(), e.Record.Commit.Collection + "/" + e.Record.Commit.RKey);
            }
        }

        public static string GetMessageRKey(SubscribeRepoMessage message, string prefix)
        {
            var first = message.Commit.Ops[0].Path;
            return GetMessageRKey(first, prefix);
        }
        public static string GetMessageRKey(string path, string prefix)
        {
            if (!path.StartsWith(prefix, StringComparison.Ordinal)) throw new Exception();
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
                try
                {
                    OnJetStreamEvent(e);
                }
                catch (Exception ex)
                {
                    Console.Error.WriteLine(ex);
                }
            };

            await firehose2.ConnectAsync();
            await new TaskCompletionSource().Task;
        }

        public async Task ListenBlueskyFirehoseAsync()
        {

            // NOTE! For some reason (perhaps CBOR deserialization) Firehose blob CIDs are not deserialized as bafkrei, but something much shorter...
            // On the other hand, getRecord and JetStream work properly.

            var firehose = new FishyFlip.ATWebSocketProtocolBuilder().Build();

            firehose.OnSubscribedRepoMessage += (s, e) =>
            {
                try
                {
                    OnFirehoseEvent(e);
                }
                catch (Exception ex)
                {
                    Console.Error.WriteLine(ex);
                }
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

            foreach (var del in (message.Commit?.Ops ?? []).Where(x => x.Action == "delete"))
            {
                OnRecordDeleted(commitAuthor, del.Path);
            }

            OnRecordCreated(commitAuthor, message.Commit.Ops[0].Path, record);
        }



        public async Task ImportCarAsync(string did, string carPath)
        {
            using var stream = File.Open(carPath, FileMode.Open, FileAccess.Read, FileShare.Read);
            await ImportCarAsync(did, stream);
        }

        
        public async Task ImportCarAsync(string did, Stream stream)
        {
            var importer = new CarImporter(did);
            importer.Log("Reading stream");

            await CarDecoder.DecodeCarAsync(stream, importer.OnCarDecoded);
            importer.LogStats();
            foreach (var record in importer.EnumerateRecords())
            {
                OnRecordCreated(record.Did, record.Path, record.Record);
            }
            importer.Log("Done.");
        }
        public async Task ImportCarAsync(string did, CancellationToken ct = default)
        {
            using var at = CreateAtProto();
            var importer = new CarImporter(did);
            importer.Log("Reading stream");

            var result = (await at.Sync.GetRepoAsync(new ATDid(did), importer.OnCarDecoded, cancellationToken: ct)).HandleResult();
            importer.LogStats();
            foreach (var record in importer.EnumerateRecords())
            {
                OnRecordCreated(record.Did, record.Path, record.Record);
            }
            importer.Log("Done.");
        }

        private static ATProtocol CreateAtProto()
        {
            return new ATProtocolBuilder().WithInstanceUrl(new Uri("https://bsky.network")).Build();
        }

        public async Task IndexUserCollectionAsync(string did, string recordType, CancellationToken ct = default)
        {
            using var at = CreateAtProto();

            string? cursor = null;
            while (true)
            {
                var page = (await at.Repo.ListRecordsAsync(new ATDid(did), recordType, 100, cursor, cancellationToken: ct)).HandleResult();
                cursor = page.Cursor;
                foreach (var item in page.Records)
                {
                    OnRecordCreated(did, item.Uri.Pathname.Substring(1), item.Value);
                }
                if (cursor == null) break;
            }
            
        }
    }
}

