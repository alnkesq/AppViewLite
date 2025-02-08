using AppViewLite.Models;
using AppViewLite.Numerics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace AppViewLite.PluggableProtocols
{
    public abstract class PluggableProtocol
    {
        public PluggableProtocol(string didPrefix)
        {
            if (didPrefix == "did:plc:" || didPrefix == "did:web:" || !Regex.IsMatch(didPrefix, @"^did:[\w\-]+:$"))
                throw new ArgumentException();
            DidPrefix = didPrefix;

        }
        public string DidPrefix { get; private set; }
        internal BlueskyEnrichedApis Apis;

        private void EnsureOwnDid(string did)
        {
            if (!did.StartsWith(DidPrefix, StringComparison.Ordinal))
                throw new ArgumentException();
        }

        public void OnProfileDiscovered(string did, BlueskyProfileBasicInfo data, bool shouldIndex)
        {
            EnsureOwnDid(did);
            var didWords = StringUtils.GetDistinctWords(GetIndexableDidText(did));

            Apis.WithRelationshipsWriteLock(rels =>
            {
                var plc = rels.SerializeDid(did);
                if (shouldIndex)
                {
                    rels.IndexProfile(plc, data);
                    foreach (var word in didWords)
                    {
                        rels.IndexProfileWord(word, plc);
                    }
                    
                }
                
                rels.StoreProfileBasicInfo(plc, data);
            });
        }

        public void OnPostDiscovered(QualifiedPluggablePostId postId, QualifiedPluggablePostId? inReplyTo, QualifiedPluggablePostId? rootPostId, BlueskyPostData data, bool shouldIndex)
        {
            rootPostId ??= inReplyTo ?? postId;
            EnsureOwnDid(postId.Did);
            if(inReplyTo != null) EnsureOwnDid(inReplyTo.Value.Did);
            EnsureOwnDid(rootPostId.Value.Did);

            Apis.WithRelationshipsWriteLock(rels =>
            {
                var authorPlc = rels.SerializeDid(postId.Did);

                data.PluggablePostId = postId.PostId;
                data.PostId = new PostId(authorPlc, postId.PostId.Tid);

                if (inReplyTo != null)
                {
                    data.InReplyToPlc = rels.SerializeDid(inReplyTo.Value.Did).PlcValue;
                    data.InReplyToRKey = inReplyTo.Value.PostId.Tid.TidValue;
                    data.PluggableInReplyToPostId = inReplyTo.Value.PostId;
                }

                data.RootPostPlc = rels.SerializeDid(rootPostId.Value.Did).PlcValue;
                data.RootPostRKey = rootPostId.Value.PostId.Tid.TidValue;
                data.PluggableRootPostId = rootPostId.Value.PostId;

                if (shouldIndex)
                {
                    rels.IndexPost(data);

                    foreach (var hashtag in StringUtils.GuessHashtags(data.Text))
                    {
                        rels.AddToSearchIndex(hashtag.ToLowerInvariant(), BlueskyRelationships.GetApproxTime32(data.PostId.PostRKey));
                    }
                }

                if (inReplyTo != null)
                {
                    rels.DirectReplies.Add(data.InReplyToPostId!.Value, data.PostId);
                }

                if (data.RootPostId != data.PostId)
                {
                    rels.RecursiveReplies.Add(data.RootPostId, data.PostId);
                }

                rels.UserToRecentPosts.Add(data.PostId.Author, new RecentPost(data.PostId.PostRKey, new Plc(data.InReplyToPlc.GetValueOrDefault())));


                rels.PostData.AddRange(new PostId(authorPlc, postId.PostId.Tid), BlueskyRelationships.SerializePostData(data, postId.Did));



            });
        }

        public bool RequiresExplicitPostIdStorage(NonQualifiedPluggablePostId? postId)
        {
            if (postId == null) return false;
            var reversed = TryGetPostIdFromTid(postId.Value.Tid);
            if (!reversed.HasValue) return true;
            return false;
        }

        public virtual NonQualifiedPluggablePostId? TryGetPostIdFromTid(Tid tid)
        {
            return null;
        }

        public abstract string? GetOriginalUrl(string did, BlueskyPostData postData);

        public abstract Task DiscoverAsync(CancellationToken ct);

        internal static PluggableProtocol Register(Type type)
        {
            var instance = RegisteredPluggableProtocols.FirstOrDefault(x => x.GetType() == type);

            if (instance == null)
            {
                instance = (PluggableProtocol)Activator.CreateInstance(type)!;
                RegisteredPluggableProtocols.Add(instance);
            }
            return instance;
        }

        public readonly static List<PluggableProtocol> RegisteredPluggableProtocols = new();

        public static PluggableProtocol? TryGetPluggableProtocolForDid(string did)
        {
            foreach (var item in RegisteredPluggableProtocols)
            {
                if (did.StartsWith(item.DidPrefix, StringComparison.Ordinal))
                    return item;
            }
            return null;
        }

        public static async Task RetryInfiniteLoopAsync(Func<CancellationToken, Task> attempt, CancellationToken ct)
        {

            while (true)
            {
                try
                {
                    await attempt(ct);
                }
                catch when (ct.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    Console.Error.WriteLine("Pluggable protocol error:");
                    Console.Error.WriteLine(ex);
                }
                await Task.Delay(TimeSpan.FromSeconds(30));
            }
        }

        public static Tid CreateSyntheticTid(DateTime date, ReadOnlySpan<char> hashableData)
        {
            var roundedDate = new DateTime(date.Ticks / TimeSpan.TicksPerSecond * TimeSpan.TicksPerSecond, DateTimeKind.Utc);

            var hash = System.IO.Hashing.XxHash64.HashToUInt64(MemoryMarshal.AsBytes(hashableData));

            var fakeMicros = hash % 0x80000;
            if (fakeMicros >= 1_000_000) throw new Exception();
            var fakeClock = hash >> 64 - 5;
            roundedDate = roundedDate.AddTicks((long)fakeMicros * TimeSpan.TicksPerMicrosecond);
            var tid = Tid.FromDateTime(roundedDate, (uint)fakeClock);
            return tid;
        }

        internal abstract protected void EnsureValidDid(string did);

        public virtual string? TryHandleToDid(string handle)
        {
            return null;
        }

        public virtual string? TryGetHandleFromDid(string did)
        {
            return null;
        }

        internal void DecompressPluggablePostId(ref NonQualifiedPluggablePostId? postId, Tid tid, NonQualifiedPluggablePostId? fallback)
        {
            if (postId != null) return;
            postId = TryGetPostIdFromTid(tid);
            if (postId != null) return;

            postId = fallback!.Value;


        }

        public virtual string? TryGetOriginalPostUrl(QualifiedPluggablePostId postId)
        {
            return null;
        }

        public virtual string? TryGetOriginalProfileUrl(string did)
        {
            return null;
        }

        public virtual Task<byte[]> GetBlobAsync(string did, byte[] bytes, ThumbnailSize preferredSize)
        {
            throw new NotSupportedException();
        }

        public virtual string? GetIndexableDidText(string did)
        {
            return null;
        }
    }
}

