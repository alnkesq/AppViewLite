using FishyFlip.Lexicon.App.Bsky.Actor;
using FishyFlip.Lexicon.App.Bsky.Embed;
using FishyFlip.Lexicon.App.Bsky.Feed;
using FishyFlip.Lexicon.App.Bsky.Richtext;
using FishyFlip.Lexicon.Com.Atproto.Repo;
using FishyFlip.Models;
using Ipfs;
using AppViewLite.Models;
using AppViewLite.Numerics;
using AppViewLite;
using AppViewLite.Storage;
using AppViewLite.Numerics;
using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.IO.Hashing;
using System.Linq;
using System.Numerics;
using System.Runtime.InteropServices;
using System.Text;

namespace AppViewLite
{
    public class BlueskyRelationships : IDisposable
    {
        public CombinedPersistentMultiDictionary<DuckDbUuid, Plc> DidHashToUserId;
        public RelationshipDictionary<PostIdTimeFirst> Likes;
        public RelationshipDictionary<PostIdTimeFirst> Reposts;
        public RelationshipDictionary<Plc> Follows;
        public RelationshipDictionary<Plc> Blocks;
        public CombinedPersistentMultiDictionary<Relationship, ListEntry> ListItems;
        public CombinedPersistentMultiDictionary<Relationship, DateTime> ListItemDeletions;
        public CombinedPersistentMultiDictionary<Relationship, byte> Lists;
        public CombinedPersistentMultiDictionary<Relationship, DateTime> ListDeletions;
        public CombinedPersistentMultiDictionary<PostId, byte> Threadgates;
        public CombinedPersistentMultiDictionary<PostId, DateTime> ThreadgateDeletions;
        public CombinedPersistentMultiDictionary<PostId, byte> Postgates;
        public CombinedPersistentMultiDictionary<PostId, DateTime> PostgateDeletions;
        public CombinedPersistentMultiDictionary<Relationship, Relationship> ListBlocks;
        public CombinedPersistentMultiDictionary<Relationship, DateTime> ListBlockDeletions;
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
        public CombinedPersistentMultiDictionary<Plc, DateTime> LastSeenNotifications;
        public CombinedPersistentMultiDictionary<Plc, Notification> Notifications;
        public CombinedPersistentMultiDictionary<Plc, ListEntry> RegisteredUserToFollowees;
        public CombinedPersistentMultiDictionary<Plc, RecentPost> UserToRecentPosts;
        public CombinedPersistentMultiDictionary<Plc, RecentRepost> UserToRecentReposts;

        private HashSet<Plc> registerForNotificationsCache = new();
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
            ProtoBuf.Serializer.PrepareSerializer<BlueskyPostData>();
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



            Likes = Register(new RelationshipDictionary<PostIdTimeFirst>(basedir + "/post-like-time-first", GetApproxTime24));
            Reposts = Register(new RelationshipDictionary<PostIdTimeFirst>(basedir + "/post-repost-time-first", GetApproxTime24));
            Follows = Register(new RelationshipDictionary<Plc>(basedir + "/follow", GetApproxPlc24));
            Blocks = Register(new RelationshipDictionary<Plc>(basedir + "/block", GetApproxPlc24));
            DirectReplies = Register(new CombinedPersistentMultiDictionary<PostId, PostId>(basedir + "/post-reply-direct") { ItemsToBuffer = DefaultBufferedItems });
            RecursiveReplies = Register(new CombinedPersistentMultiDictionary<PostId, PostId>(basedir + "/post-reply-recursive") { ItemsToBuffer = DefaultBufferedItems });
            Quotes = Register(new CombinedPersistentMultiDictionary<PostId, PostId>(basedir + "/post-quote") { ItemsToBuffer = DefaultBufferedItems });
            PostDeletions = Register(new CombinedPersistentMultiDictionary<PostId, DateTime>(basedir + "/post-deletion", PersistentDictionaryBehavior.SingleValue) { ItemsToBuffer = DefaultBufferedItemsForDeletion });
            PlcToBasicInfo = Register(new CombinedPersistentMultiDictionary<Plc, byte>(basedir + "/profile-basic", PersistentDictionaryBehavior.PreserveOrder) { ItemsToBuffer = 512 });
            PostData = Register(new CombinedPersistentMultiDictionary<PostIdTimeFirst, byte>(basedir + "/post-data-time-first", PersistentDictionaryBehavior.PreserveOrder) { ItemsToBuffer = DefaultBufferedItems });
            PostTextSearch = Register(new CombinedPersistentMultiDictionary<ulong, ApproximateDateTime32>(basedir + "/post-text-approx-time-32") { ItemsToBuffer = DefaultBufferedItems, OnCompactation = x => x.DistinctAssumingOrderedInput() });
            FailedProfileLookups = Register(new CombinedPersistentMultiDictionary<Plc, DateTime>(basedir + "/profile-basic-failed"));
            FailedPostLookups = Register(new CombinedPersistentMultiDictionary<PostId, DateTime>(basedir + "/post-data-failed"));

            ListItems = Register(new CombinedPersistentMultiDictionary<Relationship, ListEntry>(basedir + "/list-item") { ItemsToBuffer = DefaultBufferedItems });
            ListItemDeletions = Register(new CombinedPersistentMultiDictionary<Relationship, DateTime>(basedir + "/list-item-deletion", PersistentDictionaryBehavior.SingleValue) { ItemsToBuffer = DefaultBufferedItemsForDeletion });

            Lists = Register(new CombinedPersistentMultiDictionary<Relationship, byte>(basedir + "/list", PersistentDictionaryBehavior.PreserveOrder) { ItemsToBuffer = DefaultBufferedItems });
            ListDeletions = Register(new CombinedPersistentMultiDictionary<Relationship, DateTime>(basedir + "/list-deletion", PersistentDictionaryBehavior.SingleValue) { ItemsToBuffer = DefaultBufferedItemsForDeletion });

            Threadgates = Register(new CombinedPersistentMultiDictionary<PostId, byte>(basedir + "/threadgate", PersistentDictionaryBehavior.PreserveOrder) { ItemsToBuffer = DefaultBufferedItems });
            ThreadgateDeletions = Register(new CombinedPersistentMultiDictionary<PostId, DateTime>(basedir + "/threadgate-deletion", PersistentDictionaryBehavior.SingleValue) { ItemsToBuffer = DefaultBufferedItemsForDeletion });

            Postgates = Register(new CombinedPersistentMultiDictionary<PostId, byte>(basedir + "/postgate", PersistentDictionaryBehavior.PreserveOrder) { ItemsToBuffer = DefaultBufferedItems });
            PostgateDeletions = Register(new CombinedPersistentMultiDictionary<PostId, DateTime>(basedir + "/postgate-deletion", PersistentDictionaryBehavior.SingleValue) { ItemsToBuffer = DefaultBufferedItemsForDeletion });

            ListBlocks = Register(new CombinedPersistentMultiDictionary<Relationship, Relationship>(basedir + "/list-block", PersistentDictionaryBehavior.SingleValue) { ItemsToBuffer = DefaultBufferedItems });
            ListBlockDeletions = Register(new CombinedPersistentMultiDictionary<Relationship, DateTime>(basedir + "/list-block-deletion", PersistentDictionaryBehavior.SingleValue) { ItemsToBuffer = DefaultBufferedItemsForDeletion });

            Notifications = Register(new CombinedPersistentMultiDictionary<Plc, Notification>(basedir + "/notification") { ItemsToBuffer = DefaultBufferedItems });

            RegisteredUserToFollowees = Register(new CombinedPersistentMultiDictionary<Plc, ListEntry>(basedir + "/registered-user-to-followees") { ItemsToBuffer = DefaultBufferedItems });

            UserToRecentPosts = Register(new CombinedPersistentMultiDictionary<Plc, RecentPost>(basedir + "/user-to-recent-posts") { ItemsToBuffer = DefaultBufferedItems });
            UserToRecentReposts = Register(new CombinedPersistentMultiDictionary<Plc, RecentRepost>(basedir + "/user-to-recent-reposts") { ItemsToBuffer = DefaultBufferedItems });

            LastSeenNotifications = Register(new CombinedPersistentMultiDictionary<Plc, DateTime>(basedir + "/last-seen-notification", PersistentDictionaryBehavior.SingleValue) { ItemsToBuffer = DefaultBufferedItems });
            registerForNotificationsCache = new();
            foreach (var chunk in LastSeenNotifications.EnumerateKeyChunks())
            {
                var span = chunk.AsSpan();
                var length = span.Length;
                for (long i = 0; i < length; i++)
                {
                    registerForNotificationsCache.Add(span[i]);
                }
            }

            Likes.BeforeFlush += flushMappings;
            Reposts.BeforeFlush += flushMappings;
            Follows.BeforeFlush += flushMappings;
            Blocks.BeforeFlush += flushMappings;
            DirectReplies.BeforeFlush += flushMappings;
            RecursiveReplies.BeforeFlush += flushMappings;
            Quotes.BeforeFlush += flushMappings;
            PostDeletions.BeforeFlush += flushMappings;
            PlcToBasicInfo.BeforeFlush += flushMappings;
            ListItems.BeforeFlush += flushMappings;
            ListItemDeletions.BeforeFlush += flushMappings;
            Lists.BeforeFlush += flushMappings;
            ListDeletions.BeforeFlush += flushMappings;
            Threadgates.BeforeFlush += flushMappings;
            ThreadgateDeletions.BeforeFlush += flushMappings;
            Postgates.BeforeFlush += flushMappings;
            PostgateDeletions.BeforeFlush += flushMappings;
            ListBlocks.BeforeFlush += flushMappings;
            ListBlockDeletions.BeforeFlush += flushMappings;
            LastSeenNotifications.BeforeFlush += flushMappings;
            Notifications.BeforeFlush += flushMappings;
            RegisteredUserToFollowees.BeforeFlush += flushMappings;
            UserToRecentPosts.BeforeFlush += flushMappings;
            UserToRecentReposts.BeforeFlush += flushMappings;
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

        private static UInt24? GetApproxTime24(PostIdTimeFirst postId, bool saturate)
        {
            var date = postId.PostRKey.Date;
            if (date < ApproximateDateTime24.MinValueAsDateTime)
            {
                return saturate ? 0 : null;
            }
            if (date > ApproximateDateTime24.MaxValueAsDateTime)
            {
                return saturate ? ApproximateDateTime24.MaxValue.Value : null;
            }
            return ((ApproximateDateTime24)date).Value;
        }

        private static ushort? GetApproxPlc16(Plc plc, bool saturate)
        {
            return (ushort)(((uint)plc.PlcValue) >> 16);
        }
        private static UInt24? GetApproxPlc24(Plc plc, bool saturate)
        {
            return (UInt24)(((uint)plc.PlcValue) >> 8);
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

            lock (recordTypeDurations)
            {
                foreach (var item in recordTypeDurations.OrderByDescending(x => x.Value.TotalTime))
                {
                    Console.Error.WriteLine(item.Value + " " + item.Key + " (" + item.Value.Count + ", avg. " + (long)(item.Value.TotalTime / item.Value.Count).TotalMicroseconds  + ")");
                }
            }
        }
        private bool _disposed;

        public string? TryGetDid(Plc plc)
        {
            if (PlcToDid.TryGetPreserveOrderSpanAny(plc, out var r))
            {
                var s = Encoding.UTF8.GetString(r.AsSmallSpan());
                return s;
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
            if (PlcToBasicInfo.TryGetPreserveOrderSpanLatest(plc, out var arr))
            {
                var span = arr.AsSmallSpan();
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

        private readonly static FrozenDictionary<string, LanguageEnum> languageToEnum = Enum.GetValues<LanguageEnum>().ToFrozenDictionary(x => x.ToString().Replace('_', '-'), x => x);



        private List<BlueskyPostData> testDataForCompression = new();
        public BlueskyPostData StorePostInfoExceptData(Post p, PostId postId)
        {
            if (postId == default) throw new Exception();

            var proto = new BlueskyPostData
            {
                Text = string.IsNullOrEmpty(p.Text) ? null : p.Text,
                PostId = postId,

                // We will change them later if necessary.
                RootPostPlc = postId.Author.PlcValue,
                RootPostRKey = postId.PostRKey.TidValue,
            };
            var approxPostDate = GetApproxTime32(postId.PostRKey);

            var lang = p.Langs?.FirstOrDefault();

            var langEnum = ParseLanguage(lang);
            
            proto.Language = langEnum;

            if (/*langEnum != LanguageEnum.en &&*/ langEnum != LanguageEnum.Unknown)
                AddToSearchIndex("%lang-" + langEnum.ToString(), approxPostDate);
            




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


  
            AddToSearchIndex(HashPlcForTextSearch(postId.Author), approxPostDate);

            if (proto.Text != null)
            {
                var words = StringUtils.GetDistinctWords(proto.Text);
                foreach (var word in words)
                {
                    AddToSearchIndex(word, approxPostDate);
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
                            AddToSearchIndex("#" + tag.ToLowerInvariant(), approxPostDate);
                        }
                    }
                }
            }

            

            if (p.Reply?.Root is { } root)
            {
                this.RecursiveReplies.Add(this.GetPostId(root), postId);

                var rootPost = this.GetPostId(root);
                proto.RootPostPlc = rootPost.Author.PlcValue;
                proto.RootPostRKey = rootPost.PostRKey.TidValue;
                AddNotification(rootPost.Author, NotificationKind.RepliedtoYourThread, postId);
            }
            if (p.Reply?.Parent is { } parent)
            {
                var inReplyTo = this.GetPostId(parent);
                proto.InReplyToPlc = inReplyTo.Author.PlcValue;
                proto.InReplyToRKey = inReplyTo.PostRKey.TidValue;
                this.DirectReplies.Add(inReplyTo, postId);
                AddNotification(inReplyTo.Author, NotificationKind.RepliedToYourPost, postId);
            }


            var embed = p.Embed;
            if (embed is EmbedRecord { } er)
            {
                var quoted = this.GetPostId(er.Record!, ignoreIfNotPost: true);
                if (quoted != default)
                {
                    proto.QuotedPlc = quoted.Author.PlcValue;
                    proto.QuotedRKey = quoted.PostRKey.TidValue;

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
                    this.Quotes.Add(quoted, postId);
                    AddNotification(quoted.Author, NotificationKind.QuotedYourPost, postId);
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

            UserToRecentPosts.Add(postId.Author, new RecentPost(postId.PostRKey, new Plc(proto.InReplyToPlc.GetValueOrDefault())));

            return proto;
        }

        public static LanguageEnum ParseLanguage(string? lang)
        {
            if (string.IsNullOrEmpty(lang)) return LanguageEnum.Unknown;
            if (!languageToEnum.TryGetValue(lang, out var langEnum))
            {
                var dash = lang.IndexOf('-');
                if (dash != -1 && languageToEnum.TryGetValue(lang.Substring(0, dash), out langEnum)) { /* empty */ }
            }

            return langEnum;
        }

        private void AddToSearchIndex(ReadOnlySpan<char> word, ApproximateDateTime32 approxPostDate)
        {
            ulong hash = HashWord(word);
            AddToSearchIndex(hash, approxPostDate);
        }
        private void AddToSearchIndex(UInt64 hash, ApproximateDateTime32 approxPostDate)
        {
            if (this.PostTextSearch.QueuedItems.Contains(hash, approxPostDate)) return;
            this.PostTextSearch.Add(hash, approxPostDate); // Will be deduped on compactation (AddIfMissing is too expensive)
        }

        private static ulong HashPlcForTextSearch(Plc author)
        {
            return XxHash64.HashToUInt64(MemoryMarshal.AsBytes<int>([author.PlcValue]));
        }

        private static ulong HashWord(ReadOnlySpan<char> word)
        {
            return System.IO.Hashing.XxHash64.HashToUInt64(MemoryMarshal.AsBytes<char>(word), 4662323635092061535);
        }

        public IEnumerable<ApproximateDateTime32> SearchPosts(string[] searchTerms, ApproximateDateTime32 since, ApproximateDateTime32? until, Plc author, LanguageEnum language)
        {
            var searchTermsArray = searchTerms.Select(x => HashWord(x)).Distinct().ToArray();
            if (author != default)
                searchTermsArray = [..searchTermsArray, HashPlcForTextSearch(author)];
            if (language != LanguageEnum.Unknown /* && language != LanguageEnum.en*/)
                searchTermsArray = [.. searchTermsArray, HashWord("%lang-" + language)];
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
                long trimPosition;
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

        internal readonly static EfficientTextCompressor textCompressorUnlocked = new();

        internal void StorePostInfo(PostId postId, Post p)
        {
            // PERF: This method performs the slow brotli compression while holding the lock. Avoid if possible.

            var proto = StorePostInfoExceptData(p, postId);
            this.PostData.AddRange(postId, CompressPostDataToBytes(proto));
        }

        internal static byte[] CompressPostDataToBytes(BlueskyPostData proto)
        {
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

            return dest.ToArray();
        }

        private static void Compress(BlueskyPostData proto)
        {
            lock (textCompressorUnlocked)
            {
                textCompressorUnlocked.CompressInPlace(ref proto.Text, ref proto.TextBpe);
            }

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

            if (proto.Language == LanguageEnum.en) proto.Language = null;
        }
        private static void Decompress(BlueskyPostData proto, PostId postId)
        {
            if (proto.TextBpe != null)
            {
                lock (textCompressorUnlocked)
                {
                    proto.Text = textCompressorUnlocked.Decompress(proto.TextBpe);
                }
                proto.TextBpe = null;
            }

            proto.PostId = postId;

            // Decompression in reverse order, compared to compression.

            proto.Language ??= LanguageEnum.en;

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
            if (PostData.TryGetPreserveOrderSpanAny(id, out var postDataCompressed))
            {
                proto = DeserializePostData(postDataCompressed.AsSmallSpan(), id);
            }
            else if (FailedPostLookups.ContainsKey(id))
            {
                proto = new BlueskyPostData { Error = "This post could not be retrieved." };
            }
            return proto;
        }

        internal static BlueskyPostData DeserializePostData(ReadOnlySpan<byte> postDataCompressed, PostId postId)
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



        internal const int SearchIndexPopularityMinLikes = 4;
        internal const int SearchIndexPopularityMinReposts = 1;

        internal void MaybeIndexPopularPost(PostId postId, string indexName, long approxPopularity, int minPopularityForIndex)
        {

            if (BitOperations.IsPow2(approxPopularity) && approxPopularity >= minPopularityForIndex)
            {
                AddToSearchIndex("%" + indexName + "-" + approxPopularity, GetApproxTime32(postId.PostRKey));
            }
        }

        internal static string GetPopularityIndexConstraint(string name, int minPopularity)
        {
            if (!BitOperations.IsPow2(minPopularity))
                minPopularity = (int)BitOperations.RoundUpToPowerOf2((uint)minPopularity) / 2;
            return "%" + name + "-" + minPopularity;
        }
        
        public void LogPerformance(Stopwatch sw, string operationAndPath)
        {
            
            var slash = operationAndPath.IndexOf('/');
            if (slash != -1)
                operationAndPath = operationAndPath.Substring(0, slash);
            sw.Stop();
            lock (recordTypeDurations)
            {
                recordTypeDurations.TryGetValue(operationAndPath, out var total);
                total.TotalTime += sw.Elapsed;
                total.Count++;
                recordTypeDurations[operationAndPath] = total;
            }
        }

        internal static ReadOnlySpan<byte> SerializeListToBytes(FishyFlip.Lexicon.App.Bsky.Graph.List list)
        {
            var proto = new ListData
            {
                Description = !string.IsNullOrEmpty(list.Description) ? list.Description : null,
                DisplayName = list.Name,
                Purpose = list.Purpose switch 
                { 
                    FishyFlip.Lexicon.App.Bsky.Graph.ListPurpose.Curatelist => ListPurposeEnum.Curation,
                    FishyFlip.Lexicon.App.Bsky.Graph.ListPurpose.Modlist => ListPurposeEnum.Moderation,
                    FishyFlip.Lexicon.App.Bsky.Graph.ListPurpose.Referencelist => ListPurposeEnum.Reference,
                    _ => ListPurposeEnum.Unknown,
                },
                AvatarCid = list.Avatar?.Ref?.Link?.ToArray(),
            };

            using var protoMs = new MemoryStream();
            ProtoBuf.Serializer.Serialize(protoMs, proto);
            return protoMs.ToArray();
        }



        internal ReadOnlySpan<byte> SerializeThreadgateToBytes(Threadgate threadGate)
        {
            var proto = new BlueskyThreadgate
            {
                HiddenReplies = threadGate.HiddenReplies?.Select(x => RelationshipProto.FromPostId(GetPostId(x))).ToArray(),
                AllowlistedOnly = threadGate.Allow != null,
                AllowFollowing = threadGate.Allow?.Any(x => x is FollowingRule) ?? false,
                AllowMentioned = threadGate.Allow?.Any(x => x is MentionRule) ?? false,
                AllowLists = threadGate.Allow?.OfType<ListRule>().Select(x => {
                    return new RelationshipProto { Plc = SerializeDid(x.List.Did.Handler).PlcValue, Tid = Tid.Parse(x.List.Rkey).TidValue };
                }).ToArray()
            };
            return SerializeProtoToBytes(proto, x => x.Dummy = true);
        }
        internal ReadOnlySpan<byte> SerializePostgateToBytes(Postgate postgate)
        {
            var proto = new BlueskyPostgate
            {
                 DetachedEmbeddings = postgate.DetachedEmbeddingUris?.Select(x => RelationshipProto.FromPostId(GetPostId(x))).ToArray(),
                 DisallowQuotes = postgate.EmbeddingRules?.Any(x => x is DisableRule) ?? false
            };
            return SerializeProtoToBytes(proto, x => x.Dummy = true);
        }

        private static ReadOnlySpan<byte> SerializeProtoToBytes<T>(T proto, Action<T> setDummyValue)
        {
            using var protoMs = new MemoryStream();
            ProtoBuf.Serializer.Serialize(protoMs, proto);
            if (protoMs.Length == 0)
            {
                // Zero-length values are not supported in CombinedPersistentMultiDictionary
                setDummyValue(proto);
                ProtoBuf.Serializer.Serialize(protoMs, proto);
                if (protoMs.Length == 0) throw new Exception();
            }
            return protoMs.ToArray();
        }

        public bool UserDirectlyBlocksUser(Plc blocker, Plc blockee)
        {
            return Blocks.HasActor(blockee, blocker, out _);
        }

        public BlockReason UserBlocksUser(Plc blocker, Plc blockee)
        {
            if (UserDirectlyBlocksUser(blocker, blockee)) return new BlockReason(BlockReasonKind.Blocks, default);
            foreach (var subscription in GetSubscribedBlockLists(blocker))
            {
                if (IsMemberOfList(subscription, blockee)) return new BlockReason(BlockReasonKind.Blocks, subscription);
            }
            return default;
        }

        public BlockReason UsersHaveBlockRelationship(Plc a, Plc b)
        {
            var direct = UserBlocksUser(a, b);
            var inverse = UserBlocksUser(b, a);
            var directKind = direct.Kind;
            var inverseKind = inverse.Kind;

            if (directKind == default && inverseKind == default) return default;

            if (directKind != default && inverseKind != default)
            {
                return new BlockReason(BlockReasonKind.MutualBlock, direct.List);
            }

            if (directKind != default) return direct;
            if (inverseKind != default) return new BlockReason(BlockReasonKind.BlockedBy, inverse.List);

            throw new Exception();
        }

        public bool IsMemberOfList(Relationship list, Plc member)
        {
            
            foreach (var memberChunk in ListItems.GetValuesChunked(list))
            {
                var members = memberChunk.AsSpan();
                var index = members.BinarySearch(new ListEntry(member, default));
                if (index >= 0) throw new Exception();

                index = ~index;

                for (long i = index; i < members.Length; i++)
                {
                    var entry = members[i];
                    if (entry.Member != member) break;

                    var listItem = new Relationship(list.Actor, entry.ListItemRKey);
                    if (ListItemDeletions.ContainsKey(listItem))
                        continue;

                    return true;
                }
            }

            return false;
        }

        public List<Relationship> GetSubscribedBlockLists(Plc subscriber)
        {
            var lists = new List<Relationship>();

            foreach (var (subscriptionId, singleList) in ListBlocks.GetInRangeUnsorted(new Relationship(subscriber, default), new Relationship(new Plc(subscriber.PlcValue + 1), default)))
            {
                if (ListBlockDeletions.ContainsKey(subscriptionId))
                    continue;

                if (singleList.Count != 1) throw new Exception(); // it's a SingleValue

                lists.Add(lists[0]);
            }

            return lists;
        }

        public void RegisterForNotifications(Plc user)
        {
            if (IsRegisteredForNotifications(user)) return;
            LastSeenNotifications.Add(user, DateTime.MinValue);
            registerForNotificationsCache.Add(user);
        }

        public bool IsRegisteredForNotifications(Plc user)
        {
            return registerForNotificationsCache.Contains(user);
        }
        public void AddNotification(PostId destination, NotificationKind kind, Plc actor)
        {
            AddNotification(destination.Author, kind, actor, destination.PostRKey);
        }
        public void AddNotification(Plc destination, NotificationKind kind, Plc actor)
        {
            AddNotification(destination, kind, actor, default);
        }
        public void AddNotification(Plc destination, NotificationKind kind, PostId replyId)
        {
            AddNotification(destination, kind, replyId.Author, replyId.PostRKey);
        }
        public void AddNotification(Plc destination, NotificationKind kind, Plc actor, Tid rkey)
        {
            if (destination == actor) return;
            if (!IsRegisteredForNotifications(destination)) return;
            Notifications.Add(destination, new Notification((ApproximateDateTime32)DateTime.UtcNow, actor, rkey, kind));
        }


        public static Notification GetNotificationThresholdForDate(DateTime threshold)
        {
            return new Notification(threshold != default ? (ApproximateDateTime32)threshold : default, default, default, default);
        }



        public BlueskyNotification? RehydrateNotification(Notification notification, Plc destination)
        {
            (PostId post, Plc actor) = notification.Kind switch
            {
                NotificationKind.FollowedYou => (default, notification.Actor),
                NotificationKind.LikedYourPost or NotificationKind.RepostedYourPost => (new PostId(destination, notification.RKey), notification.Actor),
                NotificationKind.RepliedToYourPost or NotificationKind.RepliedtoYourThread or NotificationKind.QuotedYourPost => (new PostId(notification.Actor, notification.RKey), notification.Actor),
                _ => default
            };
            if (post == default && actor == default) return null;

            return new BlueskyNotification { EventDate = notification.EventDate, Kind = notification.Kind, Post = post != default ? GetPost(post) : null, Profile = actor != default ? GetProfile(actor) : default };
            
        }

        public long GetNotificationCount(Plc user)
        {
            if (!LastSeenNotifications.TryGetLatestValue(user, out var threshold)) return 0;

            long count = 0;
            foreach (var chunk in Notifications.GetValuesChunked(user, GetNotificationThresholdForDate(threshold)))
            {
                count += chunk.Count;
            }
            return count;
        }

        public BlueskyNotification[] GetNotificationsForUser(Plc user)
        {
            if (!LastSeenNotifications.TryGetLatestValue(user, out var threshold)) return [];
            return
                Notifications.GetValuesSortedDescending(user, BlueskyRelationships.GetNotificationThresholdForDate(threshold))
                .Take(200)
                .Select(x => RehydrateNotification(x, user))
                .Where(x => x != null)
                .ToArray();
        }

        internal IEnumerable<(PostId PostId, Plc InReplyTo)> EnumerateRecentPosts(Plc author, Tid minDate)
        {
            return this.UserToRecentPosts.GetValuesSortedDescending(author, new RecentPost(minDate, default)).Select(x => (new PostId(author, x.RKey), x.InReplyTo));
        }
        internal IEnumerable<RecentRepost> EnumerateRecentReposts(Plc author, Tid minDate)
        {
            return this.UserToRecentReposts.GetValuesSortedDescending(author, new RecentRepost(minDate, default));
        }

        public IEnumerable<BlueskyPost> EnumerateFollowingFeed(Plc loggedInUser, DateTime minDate)
        {
            var thresholdDate = minDate != default ? Tid.FromDateTime(minDate, 0) : default;

            
            var follows = RegisteredUserToFollowees.GetValuesUnsorted(loggedInUser).Select(x => x.Member).ToArray();
            var followsHashSet = follows.ToHashSet();

            var usersRecentPosts =
                follows
                .Select(author =>
                {
                    return this
                        .EnumerateRecentPosts(author, thresholdDate)
                        .Take(100)
                        .Where(x => x.InReplyTo == default || followsHashSet.Contains(x.InReplyTo))
                        .Select(x => GetPost(x.PostId))
                        .Where(x => x.Data != null && (x.IsRootPost || followsHashSet.Contains(x.Data.RootPostId.Author)));
                });
            var usersRecentReposts =
                follows
                .Select(reposter =>
                {
                    BlueskyProfile? reposterProfile = null;
                    return this
                        .EnumerateRecentReposts(reposter, thresholdDate)
                        .Select(x =>
                        {
                            var post = GetPost(x.PostId);
                            post.RepostedBy = (reposterProfile ??= GetProfile(reposter));
                            post.RepostDate = x.RepostRKey.Date;
                            return post;
                        });
                });
            return SimpleJoin.ConcatPresortedEnumerablesKeepOrdered(usersRecentPosts.Concat(usersRecentReposts).ToArray(), x => x.RepostDate != null ? Tid.FromDateTime(x.RepostDate.Value, 0) : x.PostId.PostRKey, new ReverseComparer<Tid>());
        }

        internal IEnumerable<BlueskyPost> EnumerateFeedWithNormalization(IEnumerable<BlueskyPost> posts, HashSet<PostId> alreadyReturned)
        {

            foreach (var post in posts)
            {
                var postId = post.PostId;
                if (!alreadyReturned.Add(postId)) continue;
                if (post.Data?.Deleted == true) continue;
                if (post.IsReply && !post.IsRepost)
                {
                    var parentId = post.InReplyToPostId!.Value;
                    if (alreadyReturned.Add(parentId))
                    {
                        var rootId = post.RootPostId;
                        if (rootId != postId && rootId != parentId)
                        {
                            if (alreadyReturned.Add(rootId))
                            {
                                yield return GetPost(rootId);
                            }
                        }
                        yield return GetPost(parentId);
                    }
                }
                yield return GetPost(postId);
            }
        }

        private Dictionary<string, (TimeSpan TotalTime, long Count)> recordTypeDurations = new();

    }
}

