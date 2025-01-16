using FishyFlip.Lexicon.App.Bsky.Actor;
using FishyFlip.Lexicon.App.Bsky.Embed;
using FishyFlip.Lexicon.App.Bsky.Feed;
using FishyFlip.Lexicon.App.Bsky.Richtext;
using FishyFlip.Lexicon.Com.Atproto.Repo;
using FishyFlip.Models;
using Ipfs;
using AppViewLite.Models;
using AppViewLite.Numerics;
using AppViewLite.Storage;
using AppViewLite.Numerics;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.IO.Hashing;
using System.Linq;
using System.Numerics;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.RegularExpressions;

namespace AppViewLite
{
    public class BlueskyRelationships : IDisposable
    {
        public CombinedPersistentMultiDictionary<DuckDbUuid, Plc> DidHashToUserId;
        public RelationshipDictionary<PostIdTimeFirst> Likes;
        public RelationshipDictionary<PostIdTimeFirst> Reposts;
        public RelationshipDictionary<Plc> Follows;
        public RelationshipDictionary<Plc> Blocks;
        public CombinedPersistentMultiDictionary<PostId, PostId> DirectReplies;
        public CombinedPersistentMultiDictionary<PostId, PostId> RecursiveReplies;
        public CombinedPersistentMultiDictionary<PostId, PostId> Quotes;
        public CombinedPersistentMultiDictionary<PostId, DateTime> PostDeletions;
        public CombinedPersistentMultiDictionary<Plc, byte> PlcToBasicInfo;
        public CombinedPersistentMultiDictionary<Plc, byte> PlcToDid;
        public CombinedPersistentMultiDictionary<PostIdTimeFirst, byte> PostData;
        public CombinedPersistentMultiDictionary<ulong, ApproximateDateTime32> PostTextSearch;
        public CombinedPersistentMultiDictionary<Plc, DateTime> FailedProfileLookups;
        public CombinedPersistentMultiDictionary<PostId, DateTime> FailedPostLookups;
        private List<IDisposable> disposables = new();
        public const int DefaultBufferedItems = 16 * 1024;
        public const int DefaultBufferedItemsForDeletion = 2 * 1024;

        public BlueskyRelationships()
            : this(Environment.GetEnvironmentVariable("APPVIEWLITE_DIRECTORY") ?? Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "BskyAppViewLiteData"))
        { 
        }

        public string BaseDirectory { get; }
        public BlueskyRelationships(string basedir)
        {
            this.BaseDirectory = basedir;
            Directory.CreateDirectory(basedir);
            T Register<T>(T r) where T : IDisposable
            {
                disposables.Add(r);
                return r;
            }
            Register(new FileStream(basedir + "/.lock", FileMode.Create, FileAccess.Write, FileShare.None, 1024, FileOptions.DeleteOnClose));

            DidHashToUserId = new CombinedPersistentMultiDictionary<DuckDbUuid, Plc>(basedir + "/did-hash-to-user-id", PersistentDictionaryBehavior.SingleValue);
            PlcToDid = Register(new CombinedPersistentMultiDictionary<Plc, byte>(basedir + "/plc-to-did", PersistentDictionaryBehavior.PreserveOrder));


            EventHandler flushMappings = (s, e) =>
            {
                PlcToDid.Flush();
                DidHashToUserId.Flush();
            };



            Likes = Register(new RelationshipDictionary<PostIdTimeFirst>(basedir + "/post-like-time-first", GetApproxTime16));
            Reposts = Register(new RelationshipDictionary<PostIdTimeFirst>(basedir + "/post-repost-time-first", GetApproxTime16));
            Follows = Register(new RelationshipDictionary<Plc>(basedir + "/follow", GetApproxPlc));
            Blocks = Register(new RelationshipDictionary<Plc>(basedir + "/block", GetApproxPlc));
            DirectReplies = Register(new CombinedPersistentMultiDictionary<PostId, PostId>(basedir + "/post-reply-direct") { ItemsToBuffer = DefaultBufferedItems });
            RecursiveReplies = Register(new CombinedPersistentMultiDictionary<PostId, PostId>(basedir + "/post-reply-recursive") { ItemsToBuffer = DefaultBufferedItems });
            Quotes = Register(new CombinedPersistentMultiDictionary<PostId, PostId>(basedir + "/post-quote") { ItemsToBuffer = DefaultBufferedItems });
            PostDeletions = Register(new CombinedPersistentMultiDictionary<PostId, DateTime>(basedir + "/post-deletion", PersistentDictionaryBehavior.SingleValue) { ItemsToBuffer = DefaultBufferedItemsForDeletion });
            PlcToBasicInfo = Register(new CombinedPersistentMultiDictionary<Plc, byte>(basedir + "/profile-basic", PersistentDictionaryBehavior.PreserveOrder) { ItemsToBuffer = 512 });
            PostData = Register(new CombinedPersistentMultiDictionary<PostIdTimeFirst, byte>(basedir + "/post-data-time-first", PersistentDictionaryBehavior.PreserveOrder) { ItemsToBuffer = DefaultBufferedItems });
            PostTextSearch = Register(new CombinedPersistentMultiDictionary<ulong, ApproximateDateTime32>(basedir + "/post-text-approx-time-32") { ItemsToBuffer = DefaultBufferedItems });
            FailedProfileLookups = Register(new CombinedPersistentMultiDictionary<Plc, DateTime>(basedir + "/profile-basic-failed"));
            FailedPostLookups = Register(new CombinedPersistentMultiDictionary<PostId, DateTime>(basedir + "/post-data-failed"));
            // using var relListItems = Register(new RelationshipDictionary<Relationship>(basedir + "/list-item"));

            Likes.BeforeFlush += flushMappings;
            Reposts.BeforeFlush += flushMappings;
            Follows.BeforeFlush += flushMappings;
            Blocks.BeforeFlush += flushMappings;
            DirectReplies.BeforeFlush += flushMappings;
            RecursiveReplies.BeforeFlush += flushMappings;
            Quotes.BeforeFlush += flushMappings;
            PostDeletions.BeforeFlush += flushMappings;
            PlcToBasicInfo.BeforeFlush += flushMappings;
        }

        private static ApproximateDateTime32 GetApproxTime32(Tid tid)
        {
            return (ApproximateDateTime32)tid.Date;
        }

        private static ushort? GetApproxTime16(PostIdTimeFirst postId, bool saturate)
        {
            var ts = postId.PostRKey.Date - new DateTime(2023, 1, 1, 0, 0, 0, DateTimeKind.Utc);

            // 40 mins granularity, will last for a few years

            // [TimeSpan]::TicksPerDay * 365 * 5 / [ushort]::MaxValue
            var value = ts.Ticks / 24060425726;
            if (value < 0 || value > ushort.MaxValue)
            {
                if (saturate) return value < 0 ? (ushort)0 : ushort.MaxValue;
                return null;
            }
            return (ushort)value;
        }
        private static ushort? GetApproxPlc(Plc plc, bool saturate)
        {
            return (ushort)(((uint)plc.PlcValue) >> 16);
        }

        public void Dispose()
        {
            lock (this)
            {
                foreach (var d in disposables)
                {
                    d.Dispose();
                }
                _disposed = true;
            }
        }
        private bool _disposed;

        public string? TryGetDid(Plc plc)
        {
            if (PlcToDid.TryGetPreserveOrderSpan(plc, out var r))
            {
                return Encoding.UTF8.GetString(r);
            }
            return null;
        }
        public Plc SerializeDid(string did)
        {
            return SerializeDidCore(did, addIfMissing: true);
        }

        public Plc SerializeDidCore(string did, bool addIfMissing)
        {
            if (!did.StartsWith("did:", StringComparison.Ordinal)) throw new ArgumentException();
            if (!did.StartsWith("did:web:", StringComparison.Ordinal) && !did.StartsWith("did:plc:", StringComparison.Ordinal)) throw new ArgumentException();
        
            var hash = StringUtils.HashUnicodeToUuid(did);

            

            if (!DidHashToUserId.TryGetSingleValue(hash, out var v))
            {
                v = new(checked((int)DidHashToUserId.GroupCount + 1));
                if (!addIfMissing) throw new Exception("Unknown DID.");
                DidHashToUserId.Add(hash, v);
            }

            if (!PlcToDid.ContainsKey(v))
            {
                if (!addIfMissing) throw new Exception("Unknown DID.");
                PlcToDid.AddRange(v, Encoding.UTF8.GetBytes(did));
            }
            return v;

        }
        public PostId GetPostId(StrongRef subject, bool ignoreIfNotPost = false)
        {
            var uri = subject.Uri;
            if (uri.Collection != Post.RecordType)
            {
                if (ignoreIfNotPost) return default;
                throw new ArgumentException("Unexpected URI type: " + uri.Collection);
            }
            return new PostId(SerializeDid(uri.Did.Handler), Tid.Parse(uri.Rkey));
        }

        public BlueskyProfileBasicInfo? GetProfileBasicInfo(Plc plc)
        {
            if (PlcToBasicInfo.TryGetPreserveOrderSpan(plc, out var arr))
            {
                var span = arr.AsSpan();
                var nul = span.IndexOf((byte)0);
                var displayName = Encoding.UTF8.GetString(span.Slice(0, nul));
                if (string.IsNullOrEmpty(displayName)) displayName = null;
                var avatar = span.Slice(nul + 1);
                return new BlueskyProfileBasicInfo
                {
                    DisplayName = displayName,
                    AvatarCid = avatar.IsEmpty ? null : Cid.Read(avatar.ToArray()).ToString()
                };

            }
            return null;
        }

        private List<BlueskyPostData> testDataForCompression = new();
        public BlueskyPostData PostRecordToPostData(Post p, PostId postId)
        {
            var addToInverseDictionaries = postId != default;
            var proto = new BlueskyPostData
            {
                Text = string.IsNullOrEmpty(p.Text) ? null : p.Text,
                PostId = postId,

                // We will change them later if necessary.
                RootPostPlc = postId.Author.PlcValue,
                RootPostRKey = postId.PostRKey.TidValue,
            };
            var approxPostDate = GetApproxTime32(postId.PostRKey);

            proto.Facets = (p.Facets ?? []).Select(x =>
            {
                var feature = x.Features!.FirstOrDefault();
                if (feature == null) return null;

                var facet = new FacetData
                {
                    Start = ((int)x.Index!.ByteStart!),
                    Length = (int)(x.Index.ByteEnd!.Value - x.Index.ByteStart.Value),
                };

                if (feature is Mention m)
                {
                    facet.Did = m.Did!.Handler;
                }
                else if (feature is Link l)
                {
                    facet.Link = l.Uri!;
                }
                else return null;

                return facet;
            }).Where(x => x != null).ToArray()!;
            if (proto.Facets.Length == 0)
                proto.Facets = null;


            if (addToInverseDictionaries)
            {
                this.PostTextSearch.AddIfMissing(HashPlcForTextSearch(postId.Author), approxPostDate);

                if (proto.Text != null)
                {
                    var words = StringUtils.GetDistinctWords(proto.Text);
                    foreach (var word in words)
                    {
                        ulong hash = HashWord(word);
                        this.PostTextSearch.AddIfMissing(hash, approxPostDate);
                    }
                }

                if (p.Facets != null)
                {
                    foreach (var facet in p.Facets)
                    {
                        foreach (var feature in facet.Features!.OfType<Tag>())
                        {
                            var tag = feature.TagValue;
                            if (!string.IsNullOrEmpty(tag))
                            {
                                ulong hash = HashWord("#" + tag.ToLowerInvariant());
                                this.PostTextSearch.AddIfMissing(hash, approxPostDate);
                            }
                        }
                    }
                }

            }

            if (p.Reply?.Root is { } root)
            {
                if (addToInverseDictionaries)
                    this.RecursiveReplies.Add(this.GetPostId(root), postId);

                var rootPost = this.GetPostId(root);
                proto.RootPostPlc = rootPost.Author.PlcValue;
                proto.RootPostRKey = rootPost.PostRKey.TidValue;
            }
            if (p.Reply?.Parent is { } parent)
            {
                var inReplyTo = this.GetPostId(parent);
                proto.InReplyToPlc = inReplyTo.Author.PlcValue;
                proto.InReplyToRKey = inReplyTo.PostRKey.TidValue;
                if (addToInverseDictionaries)
                    this.DirectReplies.Add(inReplyTo, postId);
            }


            var embed = p.Embed;
            if (embed is EmbedRecord { } er)
            {
                var quoted = this.GetPostId(er.Record!, ignoreIfNotPost: true);
                if (quoted != default)
                {
                    proto.QuotedPlc = quoted.Author.PlcValue;
                    proto.QuotedRKey = quoted.PostRKey.TidValue;

                    if (addToInverseDictionaries)
                        this.Quotes.Add(quoted, postId);
                    embed = null;
                }
            }
            else if (embed is RecordWithMedia { } rm)
            {
                var quoted = this.GetPostId(rm.Record!.Record!, ignoreIfNotPost: true);
                if (quoted != default)
                {
                    proto.QuotedPlc = quoted.Author.PlcValue;
                    proto.QuotedRKey = quoted.PostRKey.TidValue;

                    embed = rm.Media;
                    if (addToInverseDictionaries)
                        this.Quotes.Add(quoted, postId);
                }
            }


            if (embed is EmbedImages { } ei)
            {
                proto.Media = ei.Images!.Select(x => new BlueskyMediaData { AltText = string.IsNullOrEmpty(x.Alt) ? null : x.Alt, Cid = x.ImageValue.Ref.Link.ToArray() }).ToArray();
            }
            else if (embed is EmbedExternal { } ext)
            {
                proto.ExternalTitle = ext.External!.Title;
                proto.ExternalUrl = ext.External.Uri;
                proto.ExternalDescription = ext.External.Description;
                proto.ExternalThumbCid = ext.External.Thumb?.Ref?.Link?.ToArray();
            }
            else if (embed is EmbedVideo { } vid)
            {
                proto.Media = (proto.Media ?? []).Concat([new BlueskyMediaData
                {
                    AltText = vid.Alt,
                    Cid = vid.Video!.Ref!.Link!.ToArray(),
                    IsVideo = true,
                }]).ToArray();
            }
            else if (embed is EmbedRecord { } rec)
            {
                proto.EmbedRecordUri = rec.Record!.Uri!.ToString();
            }

            return proto;
        }



        private static ulong HashPlcForTextSearch(Plc author)
        {
            return XxHash64.HashToUInt64(MemoryMarshal.AsBytes<int>([author.PlcValue]));
        }

        private static ulong HashWord(ReadOnlySpan<char> word)
        {
            return System.IO.Hashing.XxHash64.HashToUInt64(MemoryMarshal.AsBytes<char>(word), 4662323635092061535);
        }

        public IEnumerable<ApproximateDateTime32> SearchPosts(string[] searchTerms, ApproximateDateTime32 since, ApproximateDateTime32? until, Plc author)
        {
            var searchTermsArray = searchTerms.Select(x => HashWord(x)).Distinct().ToArray();
            if (author != default)
                searchTermsArray = [..searchTermsArray, HashPlcForTextSearch(author)];
            if (searchTermsArray.Length == 0) yield break;
            var words = searchTermsArray
                .Select(x => PostTextSearch.GetValuesChunked(x).ToList())
                .Select(x => (TotalCount: x.Sum(x => x.Count), Slices: x))
                .OrderBy(x => x.TotalCount)
                .ToArray();


            var mostRecentCommonPost = until ?? ApproximateDateTime32.MaxValue;
            while (true)
            {
                var approxDate = PeelUntilNextCommonPost(words, mostRecentCommonPost);
                if (approxDate == default) break;

                yield return approxDate;

                var firstWord = words[0].Slices;
                var sliceIndex = firstWord.FindIndex(x => x[x.Count - 1] == approxDate);
                if (sliceIndex == -1) throw new Exception();
                firstWord[sliceIndex] = firstWord[sliceIndex].Slice(0, firstWord[sliceIndex].Count - 1);
                firstWord.RemoveAll(x => x.Count == 0);
                if (firstWord.Count == 0) break;

                if ((DateTime)mostRecentCommonPost < since) break;
            }

        }

        private static T PeelUntilNextCommonPost<T>((long TotalCount, List<ManagedOrNativeArray<T>> Slices)[] words, T mostRecentCommonPost) where T : unmanaged, IComparable<T>
        {
            var first = true;
            while (true)
            {
                if (words.Any(x => x.Slices.Count == 0)) return default;
                var anyChanges = first; // so that we always do the trimming for the first run (in case the user set a until:yyyy-MM-dd parameter)
                first = false;
                for (int wordIdx = 0; wordIdx < words.Length; wordIdx++)
                {
                    // what's the latest possible post id for this specific word (across all slices)?
                    var postId = words[wordIdx].Slices.Max(x => x[x.Count - 1]);
                    if (postId.CompareTo(mostRecentCommonPost) < 0) // postId < mostRecentCommonPost
                    {
                        // because of this word, we know the latest possible post must be moved to a value closer to zero.
                        mostRecentCommonPost = postId;
                        anyChanges = true;
                    }
                }

                if (!anyChanges) break;

                // now, trim all the spans to exclude post ids later than the latest possible post id
                foreach (var word in words)
                {
                    TrimAwayPostsAboveThreshold(word.Slices, mostRecentCommonPost);
                }

            }

            return mostRecentCommonPost;
        }

        private static void TrimAwayPostsAboveThreshold<T>(List<ManagedOrNativeArray<T>> slices, T maxPost) where T : unmanaged, IComparable<T>
        {
            for (int sliceIdx = 0; sliceIdx < slices.Count; sliceIdx++)
            {
                var slice = slices[sliceIdx];
                var index = slice.AsSpan().BinarySearch(maxPost);
                int trimPosition;
                if (index < 0)
                {
                    var indexOfNextLargest = ~index;
                    trimPosition = indexOfNextLargest;
                }
                else
                {
                    trimPosition = index + 1;
                }

                if (trimPosition != slice.Count)
                {
                    slices[sliceIdx] = slice.Slice(0, trimPosition);
                }
            }
            slices.RemoveAll(x => x.Count == 0);
        }

        internal void StoreProfileBasicInfo(Plc plc, Profile pf)
        {
            PlcToBasicInfo.AddRange(plc, [.. Encoding.UTF8.GetBytes(pf.DisplayName ?? string.Empty), 0, .. (pf.Avatar?.Ref?.Link?.ToArray() ?? [])]);
        }

        internal EfficientTextCompressor textCompressor = new();
        internal void StorePostInfo(PostId postId, Post p)
        {

            var proto = PostRecordToPostData(p, postId);

            // var (a, b, c) = (proto.PostId, proto.InReplyToPostId, proto.RootPostId);
            Compress(proto);


            using var protoMs = new MemoryStream();
            ProtoBuf.Serializer.Serialize(protoMs, proto);
            protoMs.Seek(0, SeekOrigin.Begin);
            
            using var dest = new System.IO.MemoryStream();
            using (var compressed = new System.IO.Compression.BrotliStream(dest, System.IO.Compression.CompressionLevel.SmallestSize))
            {
                protoMs.CopyTo(compressed);
            }

            var z = dest.ToArray();


            //var test = DeserializePostData(z, postId);
            //var (aa, bb, cc) = (test.PostId, test.InReplyToPostId, test.RootPostId);
            //if ((aa, bb, cc) != (a, b, c))
            //    throw new Exception("Bad roundtrip for post IDs");

            this.PostData.AddRange(postId, z);
        }

        private void Compress(BlueskyPostData proto)
        {
            textCompressor.CompressInPlace(ref proto.Text, ref proto.TextBpe);

            PostId postId = proto.PostId;
            PostId rootPostId = proto.RootPostId;
            PostId? inReplyToPostId = proto.InReplyToPostId;

            if (rootPostId == postId || rootPostId == inReplyToPostId)
            {
                // Either a root post, or a direct reply to a root post.
                proto.RootPostPlc = null;
                proto.RootPostRKey = null;
            }
            else if (rootPostId.Author == postId.Author)
            {
                // Self-reply. No need to store the author twice.
                proto.RootPostPlc = null;
            }


            if (inReplyToPostId?.Author == postId.Author)
            {
                // Same author as previous post. No need to explicitly store the parent author.
                proto.InReplyToPlc = null;
            }
        }
        private void Decompress(BlueskyPostData proto, PostId postId)
        {
            if (proto.TextBpe != null)
            {
                proto.Text = textCompressor.Decompress(proto.TextBpe);
                proto.TextBpe = null;
            }

            proto.PostId = postId;

            // Decompression in reverse order, compared to compression.

            if (proto.InReplyToRKey != null && proto.InReplyToPlc == null)
            {
                proto.InReplyToPlc = postId.Author.PlcValue;
            }

            if (proto.RootPostRKey != null)
            {
                if (proto.RootPostPlc != null)
                {
                    // Nothing to do.
                }
                else
                {
                    proto.RootPostPlc = postId.Author.PlcValue;
                }
            }
            else
            {
                var rootPostId = proto.InReplyToPostId ?? postId;
                proto.RootPostPlc = rootPostId.Author.PlcValue;
                proto.RootPostRKey = rootPostId.PostRKey.TidValue;
            }

        }


        public static void VerifyNotEnumerable<T>()
        {
            if (typeof(T).IsGenericType && typeof(T).GetGenericTypeDefinition() == typeof(IEnumerable<>))
                throw new InvalidOperationException("Enumeration runs outside the lock.");
        }


        public BlueskyProfile[] GetPostLikers(string profile, string rkey, Relationship continuation, int limit)
        {
            return Likes.GetRelationshipsSorted(GetPostId(profile, rkey), continuation).Take(limit).Select(x => GetProfile(x.Actor, x.RelationshipRKey)).ToArray();
        }
        public BlueskyProfile[] GetPostReposts(string profile, string rkey, Relationship continuation, int limit)
        {
            return Reposts.GetRelationshipsSorted(GetPostId(profile, rkey), continuation).Take(limit).Select(x => GetProfile(x.Actor, x.RelationshipRKey)).ToArray();
        }
        public BlueskyPost[] GetPostQuotes(string profile, string rkey, PostId continuation, int limit)
        {
            return Quotes.GetValuesSorted(GetPostId(profile, rkey), continuation).Take(limit).Select(GetPost).ToArray();
        }

        public BlueskyPost GetPost(string did, string rkey)
        {
            return GetPost(GetPostId(did, rkey));
        }
        public BlueskyPost GetPost(ATUri uri)
        {
            return GetPost(GetPostId(uri));
        }

        public BlueskyPost GetPost(PostId id)
        {
            var post = GetPostWithoutData(id);
            (post.Data, post.InReplyToUser) = TryGetPostDataAndInReplyTo(id);
            return post;
        }

        public BlueskyPost GetPost(PostId id, BlueskyPostData? data)
        {
            var post = GetPostWithoutData(id);
            post.Data = data;
            return post;
        }

        public BlueskyPost GetPostWithoutData(PostId id)
        {
            return new BlueskyPost
            {
                Author = GetProfile(id.Author),
                RKey = id.PostRKey.ToString()!,
                LikeCount = Likes.GetActorCount(id),
                QuoteCount = Quotes.GetValueCount(id),
                ReplyCount = DirectReplies.GetValueCount(id),
                RepostCount = Reposts.GetActorCount(id),
                Date = DateTime.UnixEpoch.AddMicroseconds(id.PostRKey.Timestamp),
                PostId = id,
            };
        }

        internal (BlueskyPostData? Data, BlueskyProfile? InReplyTo) TryGetPostDataAndInReplyTo(PostId id)
        {
            var d = TryGetPostData(id);
            if (d == null) return default;
            if (d.InReplyToPlc == null) return (d, null);
            return (d, GetProfile(new Plc(d.InReplyToPlc.Value)));

        }

        internal BlueskyPostData? TryGetPostData(PostId id)
        {
            if (PostDeletions.ContainsKey(id))
            {
                return new BlueskyPostData { Deleted = true, Error = "This post was deleted." };
            }

            BlueskyPostData? proto = null;
            if (PostData.TryGetPreserveOrderSpan(id, out var postDataCompressed))
            {
                proto = DeserializePostData(postDataCompressed.AsSpan(), id);
            }
            else if (FailedPostLookups.ContainsKey(id))
            {
                proto = new BlueskyPostData { Error = "This post could not be retrieved." };
            }
            return proto;
        }

        internal BlueskyPostData DeserializePostData(ReadOnlySpan<byte> postDataCompressed, PostId postId)
        {
            using var ms = new MemoryStream(postDataCompressed.Length);
            ms.Write(postDataCompressed);
            ms.Seek(0, SeekOrigin.Begin);
            using var decompress = new BrotliStream(ms, CompressionMode.Decompress);
            var proto = ProtoBuf.Serializer.Deserialize<BlueskyPostData>(decompress);
            Decompress(proto, postId);
            return proto;
        }


        public BlueskyProfile GetProfile(Plc plc)
        {
            return GetProfile(plc, null);
        }
        public BlueskyProfile GetProfile(Plc plc, Tid? relationshipRKey)
        {
            var basic = GetProfileBasicInfo(plc);
            return new BlueskyProfile()
            {
                PlcId = plc.PlcValue,
                Did = TryGetDid(plc),
                BasicData = basic,
                RelationshipRKey = relationshipRKey,
            };
        }

        public PostId GetPostId(string did, string rkey)
        {
            return new PostId(SerializeDid(did), Tid.Parse(rkey));
        }
        public PostId GetPostId(ATUri uri)
        {
            if (uri.Collection != Post.RecordType) throw new ArgumentException();
            return GetPostId(uri.Did!.ToString(), uri.Rkey);
        }

        public BlueskyProfile[] GetFollowers(string did, Relationship continuation, int limit)
        {
            return Follows.GetRelationshipsSorted(SerializeDid(did), continuation).Take(limit).Select(x => GetProfile(x.Actor, x.RelationshipRKey)).ToArray();
        }

        public BlueskyFullProfile GetFullProfile(string did)
        {
            var plc = SerializeDid(did);
            return new BlueskyFullProfile
            {
                Profile = GetProfile(plc),
                Followers = Follows.GetActorCount(plc),
            };
        }

        internal void EnsureNotDisposed()
        {
            if (_disposed) throw new ObjectDisposedException(nameof(BlueskyRelationships));
        }

        internal IEnumerable<BlueskyPost> GetRecentPosts(CombinedPersistentMultiDictionary<PostIdTimeFirst, byte>.SliceInfo slice, PostIdTimeFirst maxPostIdExlusive)
        {
            var index = slice.Reader.BinarySearch(maxPostIdExlusive);
            if (index >= 0) index--;
            else index = ~index;
            for (long i = index - 1; i >= 0; i--)
            {
                var postId = slice.Reader.Keys[i];
                if (PostDeletions.ContainsKey(postId)) continue;
                var postData = DeserializePostData(slice.Reader.GetValues(i).Span.AsSmallSpan, postId);
                yield return GetPost(postId, postData);
            }
        }



        internal const int LikeCountSearchIndexMinLikes = 4;

        internal void MaybeIndexPopularPost(PostId postId)
        {

            var approxLikeCount = Likes.creations.GetValueCount(postId);
            if (BitOperations.IsPow2(approxLikeCount) && approxLikeCount >= LikeCountSearchIndexMinLikes)
            {
                PostTextSearch.AddIfMissing(HashWord("%likes-" + approxLikeCount), GetApproxTime32(postId.PostRKey));
            }
        }

        internal static string GetPopularityIndexConstraint(string name, int minLikes)
        {
            if (!BitOperations.IsPow2(minLikes))
                minLikes = (int)BitOperations.RoundUpToPowerOf2((uint)minLikes) / 2;
            return "%" + name + "-" + minLikes;
        }
    }
}

