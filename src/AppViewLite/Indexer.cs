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
using Ipfs;
using System.Collections.Generic;
using System.IO;
using FishyFlip.Tools;
using PeterO.Cbor;
using System.Text;
using System.Threading;

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
                relationships.EnsureNotDisposed();
                var slash = path!.IndexOf('/');
                var collection = path.Substring(0, slash);
                var rkey = path.Substring(slash + 1);
                var deletionDate = DateTime.UtcNow;
                if (!Tid.TryParse(rkey, out var tid)) return;
                var rel = new Relationship(relationships.SerializeDid(commitAuthor), tid);
                if (collection == "app.bsky.feed.like")
                {
                    relationships.Likes.Delete(rel, deletionDate);
                }
                else if (collection == "app.bsky.graph.follow")
                {
                    relationships.Follows.Delete(rel, deletionDate);
                }
                else if (collection == "app.bsky.graph.block")
                {
                    relationships.Blocks.Delete(rel, deletionDate);
                }
                else if (collection == "app.bsky.feed.repost")
                {
                    relationships.Reposts.Delete(rel, deletionDate);
                }
                else if (collection == "app.bsky.feed.post")
                {
                    relationships.PostDeletions.Add(new PostId(rel.Actor, rel.RelationshipRKey), deletionDate);
                }
                //else if (collection == "app.bsky.graph.listitem")
                //{ 

                //}
            }
        }

        private void OnRecordCreated(string commitAuthor, string path, ATObject record)
        {
            lock (relationships)
            {
                relationships.EnsureNotDisposed();
                var commitPlc = relationships.SerializeDid(commitAuthor);
                if (record is Like l)
                {
                    if (l.Subject!.Uri!.Collection == "app.bsky.feed.post") // quick check to avoid noisy exceptions
                        relationships.Likes.Add(relationships.GetPostId(l.Subject), new Relationship(commitPlc, GetMessageTid(path, "app.bsky.feed.like/")));
                }
                else if (record is Follow f)
                {
                    relationships.Follows.Add(relationships.SerializeDid(f.Subject.Handler), new Relationship(commitPlc, GetMessageTid(path, "app.bsky.graph.follow/")));
                }
                else if (record is Repost r)
                {
                    relationships.Reposts.Add(relationships.GetPostId(r.Subject), new Relationship(commitPlc, GetMessageTid(path, "app.bsky.feed.repost/")));
                }
                else if (record is Block b)
                {
                    relationships.Blocks.Add(relationships.SerializeDid(b.Subject.Handler), new Relationship(commitPlc, GetMessageTid(path, "app.bsky.graph.block/")));

                }
                else if (record is Post p)
                {
                    var postId = new PostId(commitPlc, GetMessageTid(path, "app.bsky.feed.post/"));
                    relationships.StorePostInfo(postId, p);
                }
                else if (record is Profile pf && GetMessageRKey(path, "app.bsky.actor.profile") == "/self")
                {
                    relationships.StoreProfileBasicInfo(commitPlc, pf);
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
            var proto = new ATProtocolBuilder().WithInstanceUrl(new Uri("https://bsky.network")).Build();
            var importer = new CarImporter(did);
            importer.Log("Reading stream");

            var result = await proto.Sync.GetRepoAsync(new ATDid(did), importer.OnCarDecoded, cancellationToken: ct);
            if (!result.IsT0)
                throw new Exception(result.AsT1.Detail!.Error);
            importer.LogStats();
            foreach (var record in importer.EnumerateRecords())
            {
                OnRecordCreated(record.Did, record.Path, record.Record);
            }
            importer.Log("Done.");
        }
    }
}

