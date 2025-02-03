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
using DuckDbSharp.Types;
using AppViewLite.Numerics;
using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Hashing;
using System.Linq;
using System.Numerics;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;

namespace AppViewLite
{
    public class BlueskyRelationships : IDisposable
    {
        private Stopwatch lastGlobalFlush = Stopwatch.StartNew();
        public CombinedPersistentMultiDictionary<DuckDbUuid, Plc> DidHashToUserId;
        public RelationshipDictionary<PostIdTimeFirst> Likes;
        public RelationshipDictionary<PostIdTimeFirst> Reposts;
        public RelationshipDictionary<Plc> Follows;
        public RelationshipDictionary<Plc> Blocks;
        public CombinedPersistentMultiDictionary<RelationshipHashedRKey, byte> FeedGenerators;
        public CombinedPersistentMultiDictionary<RelationshipHashedRKey, DateTime> FeedGeneratorDeletions;
        public CombinedPersistentMultiDictionary<HashedWord, RelationshipHashedRKey> FeedGeneratorSearch;
        public CombinedPersistentMultiDictionary<HashedWord, Plc> ProfileSearchLong;
        public CombinedPersistentMultiDictionary<HashedWord, Plc> ProfileSearchDescriptionOnly;
        public CombinedPersistentMultiDictionary<SizeLimitedWord8, Plc> ProfileSearchPrefix8;
        public CombinedPersistentMultiDictionary<SizeLimitedWord2, Plc> ProfileSearchPrefix2;
        public RelationshipDictionary<RelationshipHashedRKey> FeedGeneratorLikes;
        public CombinedPersistentMultiDictionary<Plc, ListMembership> ListMemberships;
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
        public CombinedPersistentMultiDictionary<Plc, byte> Profiles;
        public CombinedPersistentMultiDictionary<Plc, byte> PlcToDid;
        public CombinedPersistentMultiDictionary<PostIdTimeFirst, byte> PostData;
        public CombinedPersistentMultiDictionary<HashedWord, ApproximateDateTime32> PostTextSearch;
        public CombinedPersistentMultiDictionary<Plc, DateTime> FailedProfileLookups;
        public CombinedPersistentMultiDictionary<Relationship, DateTime> FailedListLookups;
        public CombinedPersistentMultiDictionary<PostId, DateTime> FailedPostLookups;
        public CombinedPersistentMultiDictionary<Plc, Notification> LastSeenNotifications;
        public CombinedPersistentMultiDictionary<Plc, Notification> Notifications;
        public CombinedPersistentMultiDictionary<Plc, ListEntry> RegisteredUserToFollowees;
        public CombinedPersistentMultiDictionary<Plc, RecentPost> UserToRecentPosts;
        public CombinedPersistentMultiDictionary<Plc, RecentRepost> UserToRecentReposts;
        public CombinedPersistentMultiDictionary<RepositoryImportKey, byte> CarImports;
        public CombinedPersistentMultiDictionary<Plc, byte> AppViewLiteProfiles;
        public CombinedPersistentMultiDictionary<Plc, byte> DidDocs;
        public CombinedPersistentMultiDictionary<HashedWord, Plc> HandleToPossibleDids;
        public CombinedPersistentMultiDictionary<Pds, byte> PdsIdToString;
        public CombinedPersistentMultiDictionary<DuckDbUuid, Pds> PdsHashToPdsId;
        public CombinedPersistentMultiDictionary<DateTime, int> LastRetrievedPlcDirectoryEntry;
        public CombinedPersistentMultiDictionary<DuckDbUuid, HandleVerificationResult> HandleToDidVerifications;

        public DateTime PlcDirectorySyncDate;
        private Plc LastAssignedPlc;
        public TimeSpan PlcDirectoryStaleness => DateTime.UtcNow - PlcDirectorySyncDate;
       

        private HashSet<Plc> registerForNotificationsCache = new();
        private List<ICheckpointable> disposables = new();
        public const int DefaultBufferedItems = 16 * 1024;
        public int AvoidFlushes;

        public BlueskyRelationships()
            : this(Environment.GetEnvironmentVariable("APPVIEWLITE_DIRECTORY") ?? Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "BskyAppViewLiteData"), false)
        { 
        }

        public string BaseDirectory { get; }
        private IDisposable lockFile;
        private Dictionary<string, string[]>? checkpointToLoad;
        private GlobalCheckpoint loadedCheckpoint;

        private T Register<T>(T r) where T : ICheckpointable
        {
            disposables.Add(r);
            return r;
        }
        private CombinedPersistentMultiDictionary<TKey, TValue> RegisterDictionary<TKey, TValue>(string name, PersistentDictionaryBehavior behavior = PersistentDictionaryBehavior.SortedValues, Func<IEnumerable<TValue>, IEnumerable<TValue>>? onCompactation = null) where TKey : unmanaged, IComparable<TKey> where TValue : unmanaged, IComparable<TValue>, IEquatable<TValue>
        {
            return Register(new CombinedPersistentMultiDictionary<TKey, TValue>(
                BaseDirectory + "/" + name,
                checkpointToLoad.TryGetValue(name, out var slices) ? slices : [],
                behavior
            ) {
                ItemsToBuffer = DefaultBufferedItems,
                OnCompactation = onCompactation 
            });
        }
        private RelationshipDictionary<TTarget> RegisterRelationshipDictionary<TTarget>(string name, Func<TTarget, bool, UInt24?>? targetToApproxTarget) where TTarget : unmanaged, IComparable<TTarget>
        {
            return Register(new RelationshipDictionary<TTarget>(BaseDirectory, name, checkpointToLoad, targetToApproxTarget));
        }

        public bool IsReadOnly { get; private set; }
        public BlueskyRelationships(string basedir, bool isReadOnly)
        {
            ProtoBuf.Serializer.PrepareSerializer<BlueskyPostData>();
            ProtoBuf.Serializer.PrepareSerializer<RepositoryImportEntry>();
            ProtoBuf.Serializer.PrepareSerializer<ListData>();
            ProtoBuf.Serializer.PrepareSerializer<BlueskyThreadgate>();
            ProtoBuf.Serializer.PrepareSerializer<BlueskyPostgate>();
            ProtoBuf.Serializer.PrepareSerializer<AppViewLiteProfileProto>();
            this.BaseDirectory = basedir;
            this.IsReadOnly = isReadOnly;
            Directory.CreateDirectory(basedir);

            


            lockFile = new FileStream(basedir + "/.lock", FileMode.Create, FileAccess.Write, FileShare.None, 1024, FileOptions.DeleteOnClose);

            var checkpointsDir = new DirectoryInfo(basedir + "/checkpoints");
            checkpointsDir.Create();
            var latestCheckpoint = checkpointsDir.EnumerateFiles("*.pb").MaxBy(x => (DateTime?)x.LastWriteTimeUtc);
            loadedCheckpoint = latestCheckpoint != null ? DeserializeProto<GlobalCheckpoint>(File.ReadAllBytes(latestCheckpoint.FullName)) : new GlobalCheckpoint();
            checkpointToLoad = (loadedCheckpoint.Tables ?? []).ToDictionary(x => x.Name, x => (x.Slices ?? []).Select(x => x.StartTime + "-" + x.EndTime).ToArray());


            DidHashToUserId = RegisterDictionary<DuckDbUuid, Plc>("did-hash-to-user-id", PersistentDictionaryBehavior.SingleValue);
            PlcToDid = RegisterDictionary<Plc, byte>("plc-to-did", PersistentDictionaryBehavior.PreserveOrder);
            PdsIdToString = RegisterDictionary<Pds, byte>("pds-id-to-string", PersistentDictionaryBehavior.PreserveOrder);
            PdsHashToPdsId = RegisterDictionary<DuckDbUuid, Pds>("pds-hash-to-id", PersistentDictionaryBehavior.SingleValue);


            Likes = RegisterRelationshipDictionary<PostIdTimeFirst>("post-like-time-first", GetApproxTime24);
            Reposts = RegisterRelationshipDictionary<PostIdTimeFirst>("post-repost-time-first", GetApproxTime24);
            Follows = RegisterRelationshipDictionary<Plc>("follow", GetApproxPlc24);
            Blocks = RegisterRelationshipDictionary<Plc>("block", GetApproxPlc24);
            DirectReplies = RegisterDictionary<PostId, PostId>("post-reply-direct") ;
            RecursiveReplies = RegisterDictionary<PostId, PostId>("post-reply-recursive") ;
            Quotes = RegisterDictionary<PostId, PostId>("post-quote") ;
            PostDeletions = RegisterDictionary<PostId, DateTime>("post-deletion", PersistentDictionaryBehavior.SingleValue);
            Profiles = RegisterDictionary<Plc, byte>("profile-basic-2", PersistentDictionaryBehavior.PreserveOrder);
            ProfileSearchLong = RegisterDictionary<HashedWord, Plc>("profile-search-long");
            ProfileSearchDescriptionOnly = RegisterDictionary<HashedWord, Plc>("profile-search-description-only");
            ProfileSearchPrefix8 = RegisterDictionary<SizeLimitedWord8, Plc>("profile-search-prefix");
            ProfileSearchPrefix2 = RegisterDictionary<SizeLimitedWord2, Plc>("profile-search-prefix-2-letters");

            PostData = RegisterDictionary<PostIdTimeFirst, byte>("post-data-time-first-2", PersistentDictionaryBehavior.PreserveOrder) ;
            PostTextSearch = RegisterDictionary<HashedWord, ApproximateDateTime32>("post-text-approx-time-32");
            FailedProfileLookups = RegisterDictionary<Plc, DateTime>("profile-basic-failed");
            FailedPostLookups = RegisterDictionary<PostId, DateTime>("post-data-failed");
            FailedListLookups = RegisterDictionary<Relationship, DateTime>("list-data-failed");

            ListItems = RegisterDictionary<Relationship, ListEntry>("list-item") ;
            ListItemDeletions = RegisterDictionary<Relationship, DateTime>("list-item-deletion", PersistentDictionaryBehavior.SingleValue);
            ListMemberships = RegisterDictionary<Plc, ListMembership>("list-membership-2");

            Lists = RegisterDictionary<Relationship, byte>("list", PersistentDictionaryBehavior.PreserveOrder) ;
            ListDeletions = RegisterDictionary<Relationship, DateTime>("list-deletion", PersistentDictionaryBehavior.SingleValue);

            Threadgates = RegisterDictionary<PostId, byte>("threadgate", PersistentDictionaryBehavior.PreserveOrder) ;
            ThreadgateDeletions = RegisterDictionary<PostId, DateTime>("threadgate-deletion", PersistentDictionaryBehavior.SingleValue);

            Postgates = RegisterDictionary<PostId, byte>("postgate", PersistentDictionaryBehavior.PreserveOrder) ;
            PostgateDeletions = RegisterDictionary<PostId, DateTime>("postgate-deletion", PersistentDictionaryBehavior.SingleValue);

            ListBlocks = RegisterDictionary<Relationship, Relationship>("list-block", PersistentDictionaryBehavior.SingleValue) ;
            ListBlockDeletions = RegisterDictionary<Relationship, DateTime>("list-block-deletion", PersistentDictionaryBehavior.SingleValue);

            Notifications = RegisterDictionary<Plc, Notification>("notification-2") ;

            RegisteredUserToFollowees = RegisterDictionary<Plc, ListEntry>("registered-user-to-followees");

            UserToRecentPosts = RegisterDictionary<Plc, RecentPost>("user-to-recent-posts-2") ;
            UserToRecentReposts = RegisterDictionary<Plc, RecentRepost>("user-to-recent-reposts-2") ;

            CarImports = RegisterDictionary<RepositoryImportKey, byte>("car-import-proto-2", PersistentDictionaryBehavior.PreserveOrder) ;

            LastSeenNotifications = RegisterDictionary<Plc, Notification>("last-seen-notification-3", PersistentDictionaryBehavior.SingleValue) ;

            AppViewLiteProfiles = RegisterDictionary<Plc, byte>("appviewlite-profile", PersistentDictionaryBehavior.PreserveOrder);
            FeedGenerators = RegisterDictionary<RelationshipHashedRKey, byte>("feed-generator", PersistentDictionaryBehavior.PreserveOrder);
            FeedGeneratorSearch = RegisterDictionary<HashedWord, RelationshipHashedRKey>("feed-generator-search");
            FeedGeneratorLikes = RegisterRelationshipDictionary<RelationshipHashedRKey>("feed-generator-like-2", GetApproxRkeyHash24);
            FeedGeneratorDeletions = RegisterDictionary<RelationshipHashedRKey, DateTime>("feed-deletion");
            DidDocs = RegisterDictionary<Plc, byte>("did-doc-2", PersistentDictionaryBehavior.PreserveOrder);
            HandleToPossibleDids = RegisterDictionary<HashedWord, Plc>("handle-to-possible-dids");
            LastRetrievedPlcDirectoryEntry = RegisterDictionary<DateTime, int>("last-retrieved-plc-directory", PersistentDictionaryBehavior.SingleValue);
            HandleToDidVerifications = RegisterDictionary<DuckDbUuid, HandleVerificationResult>("handle-verifications");
            
           

            

            PlcDirectorySyncDate = LastRetrievedPlcDirectoryEntry.MaximumKey ?? new DateTime(2022, 11, 17, 00, 35, 16, DateTimeKind.Utc) /* first event on the PLC directory */;
            LastAssignedPlc = PlcToDid.MaximumKey ?? default;
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


            if (IsReadOnly)
            {
                foreach (var item in disposables)
                {
                    item.BeforeFlush += (_, _) => throw new InvalidOperationException("ReadOnly mode.");
                    item.ShouldFlush += (_, _) => throw new InvalidOperationException("ReadOnly mode.");
                }
            }
            else
            {

                foreach (var item in disposables)
                {
                    item.ShouldFlush += (table, e) =>
                    {
                        if (AvoidFlushes > 0 && !(table == Follows || table == PostData || table == Likes /* big tables unrelated to PLC directory indexing */))
                            e.Cancel = true;


                    };
                }

            }
            checkpointToLoad = null;

            GarbageCollectOldSlices(allowTempFileDeletion: true);
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
        private static UInt24? GetApproxRkeyHash24(RelationshipHashedRKey rkeyHash, bool saturate)
        {
            return GetApproxPlc24(rkeyHash.Plc, saturate);
        }

        public bool IsDisposed => _disposed;
        public void Dispose()
        {
            lock (this)
            {
                if (!_disposed)
                {
                    foreach (var d in disposables)
                    {
                        d.Dispose();
                    }
                    CaptureCheckpoint();
                    lockFile.Dispose();
                    _disposed = true;
                }
                
            }

            lock (recordTypeDurations)
            {
                foreach (var item in recordTypeDurations.OrderByDescending(x => x.Value.TotalTime))
                {
                    Console.Error.WriteLine(item.Value + " " + item.Key + " (" + item.Value.Count + ", avg. " + (long)(item.Value.TotalTime / item.Value.Count).TotalMicroseconds  + ")");
                }
            }
        }

        private void CaptureCheckpoint()
        {
            if (IsReadOnly) return;
            try
            {
                loadedCheckpoint.Tables ??= new();
                foreach (var table in disposables)
                {
                    foreach (var activeSlices in table.GetActiveSlices())
                    {
                        var checkpointTable = loadedCheckpoint.Tables.FirstOrDefault(x => x.Name == activeSlices.TableName);
                        if (checkpointTable == null)
                        {
                            checkpointTable = new() { Name = activeSlices.TableName };
                            loadedCheckpoint.Tables.Add(checkpointTable);
                        }

                        checkpointTable.Slices = activeSlices.ActiveSlices.Select(x =>
                        {
                            var interval = CombinedPersistentMultiDictionary.GetSliceInterval(x + ".");
                            return new GlobalCheckpointSlice()
                            {
                                StartTime = interval.StartTime.Ticks,
                                EndTime = interval.EndTime.Ticks,
                            };
                        }).ToArray();
                    }
                }


                var checkpointFile = Path.Combine(BaseDirectory, "checkpoints", DateTime.UtcNow.ToString("yyyyMMdd-HHmmss") + ".pb");
                File.WriteAllBytes(checkpointFile + ".tmp", SerializeProto(loadedCheckpoint, x => x.Dummy = true));
                File.Move(checkpointFile + ".tmp", checkpointFile);

                GarbageCollectOldSlices();
            }
            catch (Exception ex)
            {
                CombinedPersistentMultiDictionary.Abort(ex);
            }
        }

        private void GarbageCollectOldSlices(bool allowTempFileDeletion = false)
        {
            if (IsReadOnly) return;
            var keep = new DirectoryInfo(BaseDirectory + "/checkpoints")
                .EnumerateFiles("*.pb")
                .OrderByDescending(x => x.LastWriteTimeUtc)
                .Take(3)
                .Select(x => DeserializeProto<GlobalCheckpoint>(File.ReadAllBytes(x.FullName)))
                .SelectMany(x => x.Tables ?? [])
                .GroupBy(x => x.Name)
                .Select(x => (TableName: x.Key, SlicesToKeep: x.SelectMany(x => x.Slices ?? []).Select(x => x.StartTime + "-" + x.EndTime).Distinct().ToArray()));
            foreach (var table in keep)
            {
                foreach (var file in new DirectoryInfo(Path.Combine(BaseDirectory, table.TableName)).EnumerateFiles())
                {
                    var name = file.Name;
                    if (name.EndsWith(".tmp", StringComparison.Ordinal))
                    {
                        if (allowTempFileDeletion) file.Delete();
                        else continue; // might be a parallel compactation
                    }

                    var dot = name.IndexOf('.');
                    if (dot != -1) name = name.Substring(0, dot);
                    
                    if (!table.SlicesToKeep.Any(k => k.Contains(name)))
                    {
                        file.Delete();
                    }
                }
            }
        }

        private bool _disposed;

        public string GetDid(Plc plc)
        {
            if (PlcToDid.TryGetPreserveOrderSpanAny(plc, out var r))
            {
                var s = Encoding.UTF8.GetString(r.AsSmallSpan());
                if (SerializeDid(s) != plc) CombinedPersistentMultiDictionary.Abort(new Exception("Did serialization did not roundtrip for " + plc + "/" + s));
                return s;
            }
            throw new Exception("Missing DID string for Plc(" + plc + ")");
        }



        public Plc SerializeDid(string did)
        {
            if (!did.StartsWith("did:", StringComparison.Ordinal)) throw new ArgumentException();
            if (!did.StartsWith("did:web:", StringComparison.Ordinal) && !did.StartsWith("did:plc:", StringComparison.Ordinal)) throw new ArgumentException();
        
            var hash = StringUtils.HashUnicodeToUuid(did);




            if (DidHashToUserId.TryGetSingleValue(hash, out var plc))
            {
                return plc;
            }

            plc = new Plc(checked(LastAssignedPlc.PlcValue + 1));
            LastAssignedPlc = plc;
            PlcToDid.AddRange(plc, Encoding.UTF8.GetBytes(did));
            DidHashToUserId.Add(hash, plc);

            return plc;

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
            if (Profiles.TryGetPreserveOrderSpanLatest(plc, out var arr))
            {
                var span = arr.AsSmallSpan();
                var proto = DeserializeProto<BlueskyProfileBasicInfo>(arr.AsSmallSpan());
                lock (textCompressorUnlocked)
                {
                    textCompressorUnlocked.DecompressInPlace(ref proto.DisplayName, ref proto.DisplayNameBpe);
                    textCompressorUnlocked.DecompressInPlace(ref proto.Description, ref proto.DescriptionBpe);
                }
                proto.AvatarCid = proto.AvatarCidBytes != null ? Cid.Read(proto.AvatarCidBytes).ToString() : null;
                return proto;

            }
            else if(FailedProfileLookups.ContainsKey(plc))
            {
                return new BlueskyProfileBasicInfo
                {
                    Error = "This profile could not be retrieved."
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
            proto.Facets = GetFacetsAsProtos(p.Facets);



            PostTextSearch.Add(HashPlcForTextSearch(postId.Author), approxPostDate);

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
                AddNotification(rootPost.Author, NotificationKind.RepliedToYourThread, postId);
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

            if (proto.QuotedRKey != null)
                NotifyPostStatsChange(postId, postId.Author);
            return proto;
        }

        private static FacetData[]? GetFacetsAsProtos(List<Facet>? facets)
        {
            if (facets == null) return null;
            var facetProtos = (facets ?? []).Select(x =>
            {
                var feature = x.Features!.FirstOrDefault();
                if (feature == null) return null;

                var facet = new FacetData
                {
                    Start = ((int)x.Index!.ByteStart!),
                    Length = (int)(x.Index.ByteEnd! - x.Index.ByteStart),
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
            if (facetProtos.Length == 0) return null;
            return facetProtos;
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
            PostTextSearch.Add(HashWord(word), approxPostDate);
        }


        private static HashedWord HashPlcForTextSearch(Plc author)
        {
            return new(XxHash64.HashToUInt64(MemoryMarshal.AsBytes<int>([author.PlcValue])));
        }

        public static HashedWord HashWord(ReadOnlySpan<char> word)
        {
            return new(System.IO.Hashing.XxHash64.HashToUInt64(MemoryMarshal.AsBytes<char>(word), 4662323635092061535));
        }


        public IEnumerable<RelationshipHashedRKey> SearchFeeds(string[] searchTerms, RelationshipHashedRKey maxExclusive)
        {
            var searchTermsArray = searchTerms.Select(x => HashWord(x)).Distinct().ToArray();
            if (searchTermsArray.Length == 0) yield break;
            var words = searchTermsArray
                .Select(x => FeedGeneratorSearch.GetValuesChunked(x).ToList())
                .Select(x => (TotalCount: x.Sum(x => x.Count), Slices: x))
                .OrderBy(x => x.TotalCount)
                .ToArray();

            while (true)
            {
                PeelUntilNextCommonPost(words, ref maxExclusive);
                if (maxExclusive == default) break;

                yield return maxExclusive;

                if (!RemoveEmptySearchSlices(words, maxExclusive)) break;
            }

        }



        public IEnumerable<ManagedOrNativeArray<Plc>> SearchProfilesPrefixOnly(SizeLimitedWord8 prefix)
        {
            if (prefix.Length <= 2)
            {
                var prefix2 = SizeLimitedWord2.Create(prefix);
                var maxExclusive = prefix2.GetMaxExclusiveForPrefixRange();
                return ProfileSearchPrefix2.GetInRangeUnsorted(prefix2, maxExclusive).Select(x => x.Values);
            }
            else
            {
                var maxExclusive = prefix.GetMaxExclusiveForPrefixRange();
                return ProfileSearchPrefix8.GetInRangeUnsorted(prefix, maxExclusive).Select(x => x.Values);
            }
            
        }

        public IEnumerable<Plc> SearchProfiles(string[] searchTerms, SizeLimitedWord8 searchTermsLastPrefix, Plc maxExclusive, bool alsoSearchDescriptions)
        {
            var searchTermsArray = searchTerms.Select(x => HashWord(x)).Distinct().ToArray();

            var toIntersect = new List<List<ManagedOrNativeArray<Plc>>>();

            foreach (var word in searchTerms)
            {
                var slices = new List<ManagedOrNativeArray<Plc>>();
                var sizeLimited = SizeLimitedWord8.Create(word, out var truncated);

                if (truncated)
                {
                    slices.AddRange(ProfileSearchLong.GetValuesChunked(HashWord(word)));
                }
                else
                {
                    slices.AddRange(ProfileSearchPrefix8.GetValuesChunked(sizeLimited));
                }


                if (alsoSearchDescriptions)
                {
                    slices.AddRange(ProfileSearchDescriptionOnly.GetValuesChunked(HashWord(word)));
                }

                toIntersect.Add(slices);
            }

            if (!searchTermsLastPrefix.IsEmpty)
            {
                var prefixWords = SearchProfilesPrefixOnly(searchTermsLastPrefix).ToList();
                toIntersect.Add(prefixWords);
            }

            var words = toIntersect
                .Select(x => (TotalCount: x.Sum(x => x.Count), Slices: x))
                .OrderBy(x => x.TotalCount)
                .ToArray();

            if (words.Length == 0) throw new Exception();

            while (true)
            {
                PeelUntilNextCommonPost(words, ref maxExclusive);
                if (maxExclusive == default) break;

                yield return maxExclusive;

                if (!RemoveEmptySearchSlices(words, maxExclusive)) break;
            }

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
                PeelUntilNextCommonPost(words, ref mostRecentCommonPost);
                if (mostRecentCommonPost == default) break;

                yield return mostRecentCommonPost;

                if (!RemoveEmptySearchSlices(words, mostRecentCommonPost)) break;
                if ((DateTime)mostRecentCommonPost < since) break;
            }

        }

        private static bool RemoveEmptySearchSlices<T>((long TotalCount, List<ManagedOrNativeArray<T>> Slices)[] words, T approxDate) where  T : unmanaged, IEquatable<T>
        {
            var firstWord = words[0].Slices;
            var sliceIndex = firstWord.FindIndex(x => x[x.Count - 1].Equals(approxDate));
            if (sliceIndex == -1) throw new Exception();
            firstWord[sliceIndex] = firstWord[sliceIndex].Slice(0, firstWord[sliceIndex].Count - 1);
            firstWord.RemoveAll(x => x.Count == 0);
            return firstWord.Count != 0;
        }

        private static void PeelUntilNextCommonPost<T>((long TotalCount, List<ManagedOrNativeArray<T>> Slices)[] words, ref T mostRecentCommonPost) where T : unmanaged, IComparable<T>
        {
            var first = true;
            while (true)
            {
                if (words.Any(x => x.Slices.Count == 0))
                {
                    mostRecentCommonPost = default;
                    return;
                }
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
            //PlcToBasicInfo.AddRange(plc, [.. Encoding.UTF8.GetBytes(pf.DisplayName ?? string.Empty), 0, .. (pf.Avatar?.Ref?.Link?.ToArray() ?? [])]);

            var pinnedPost = pf.PinnedPost?.Uri;
            if (pinnedPost != null && SerializeDid(pinnedPost.Did!.Handler) != plc)
                pinnedPost = null;

            var proto = new BlueskyProfileBasicInfo
            {
                Description = pf.Description,
                DisplayName = pf.DisplayName,
                AvatarCidBytes = pf.Avatar?.Ref?.Link?.ToArray(),
                BannerCidBytes = pf.Banner?.Ref?.Link?.ToArray(),
                PinnedPostTid = pinnedPost != null ? Tid.Parse(pinnedPost.Rkey).TidValue : null,
            };

            var nameWords = StringUtils.GetDistinctWords(proto.DisplayName);
            var descriptionWords = StringUtils.GetDistinctWords(proto.Description);

            foreach (var word in nameWords)
            {
                IndexProfileWord(word, plc);
            }
            foreach (var word in descriptionWords.Except(nameWords))
            {
                var hash = HashWord(word);
                ProfileSearchDescriptionOnly.AddIfMissing(hash, plc);
            }

            lock (textCompressorUnlocked)
            {
                textCompressorUnlocked.CompressInPlace(ref proto.Description, ref proto.DescriptionBpe);
                textCompressorUnlocked.CompressInPlace(ref proto.DisplayName, ref proto.DisplayNameBpe);
            }

            

            Profiles.AddRange(plc, SerializeProto(proto, x => x.Dummy = true));
        }

        private void IndexProfileWord(string word, Plc plc)
        {
            var hash = HashWord(word);
            
            ProfileSearchPrefix8.Add(SizeLimitedWord8.Create(word, out var truncated), plc);
            if (truncated)
            {
                ProfileSearchLong.Add(hash, plc);
            }
            ProfileSearchPrefix2.Add(SizeLimitedWord2.Create(word), plc);
        }

        internal readonly static EfficientTextCompressor textCompressorUnlocked = new();

        internal void StorePostInfo(PostId postId, Post p)
        {
            // PERF: This method performs the slow brotli compression while holding the lock. Avoid if possible.

            var proto = StorePostInfoExceptData(p, postId);
            this.PostData.AddRange(postId, SerializePostData(proto));
        }

        internal static byte[] SerializePostData(BlueskyPostData proto)
        {
            // var (a, b, c) = (proto.PostId, proto.InReplyToPostId, proto.RootPostId);
            Compress(proto);

            var slim = TryCompressSlim(proto);
            if (slim != null)
            {
                //var roundtrip = DeserializePostData(slim, proto.PostId);
                //Compress(roundtrip);
                //if (!SimpleJoin.AreProtosEqual(proto, roundtrip))
                //    throw new Exception();
                return slim;
            }

            using var protoMs = new MemoryStream();
            protoMs.WriteByte((byte)PostDataEncoding.Proto);

            ProtoBuf.Serializer.Serialize(protoMs, proto);
            protoMs.Seek(0, SeekOrigin.Begin);
            return protoMs.ToArray(); 
            

            // Brotli rarely improves things now that opengraph and alt-text are also TikToken-compressed.
#if false
            var uncompressedLength = protoMs.Length;
            dest.WriteByte((byte)PostDataEncoding.BrotliProto);

            using (var compressed = new System.IO.Compression.BrotliStream(dest, System.IO.Compression.CompressionLevel.SmallestSize))
            {
                protoMs.CopyTo(compressed);
            }
            var compressedBytes = dest.ToArray();
            var ratio = (double)compressedBytes.Length / uncompressedLength;
            Console.WriteLine(ratio.ToString("0.00"));
            if (ratio > 1.2)
            {
                
            }
            else if (ratio < 0.8)
            { 
            
            }
            //     Console.WriteLine(uncompressedLength + " -> " + compressedBytes.Length);

            return compressedBytes;
#endif
        }

        private static byte[]? TryCompressSlim(BlueskyPostData proto)
        {
            if (!proto.IsSlimCandidate()) return null;
            using var ms = new MemoryStream();
            using var bw = new BinaryWriter(ms);
            bw.Write((byte)0); // reserve space

            var encoding = PostDataEncoding.BpeOnly;
            

            if (proto.Language != null)
            {
                encoding |= PostDataEncoding.Language;
                bw.Write(checked((byte)(proto!.Language.Value)));
            }

            if (proto.InReplyToRKey != null)
            {
                encoding |= PostDataEncoding.InReplyToRKey;
                bw.Write(proto.InReplyToRKey.Value);
                if (proto.InReplyToPlc != null)
                {
                    encoding |= PostDataEncoding.InReplyToPlc;
                    bw.Write(proto.InReplyToPlc.Value);
                }
            }

            if (proto.RootPostRKey != null)
            {
                encoding |= PostDataEncoding.RootPostRKey;
                bw.Write(proto.RootPostRKey.Value);
                if (proto.RootPostPlc != null)
                {
                    encoding |= PostDataEncoding.RootPostPlc;
                    bw.Write(proto.RootPostPlc.Value);
                }
            }

            bw.Write(proto.TextBpe!);

            bw.Flush();
            var arr = ms.ToArray();
            arr[0] = (byte)encoding;
            return arr;
        }

        internal static void Compress(BlueskyPostData proto)
        {
            lock (textCompressorUnlocked)
            {
                textCompressorUnlocked.CompressInPlace(ref proto.Text, ref proto.TextBpe);
                textCompressorUnlocked.CompressInPlace(ref proto.ExternalTitle, ref proto.ExternalTitleBpe);
                textCompressorUnlocked.CompressInPlace(ref proto.ExternalDescription, ref proto.ExternalDescriptionBpe);
                textCompressorUnlocked.CompressInPlace(ref proto.ExternalUrl, ref proto.ExternalUrlBpe);
                if (proto.Media != null)
                {
                    foreach (var media in proto.Media)
                    {
                        textCompressorUnlocked.CompressInPlace(ref media.AltText, ref media.AltTextBpe);
                    }
                }
                if (proto.Facets != null)
                {
                    foreach (var facet in proto.Facets)
                    {
                        textCompressorUnlocked.CompressInPlace(ref facet.Link, ref facet.LinkBpe);
                    }
                }
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
         
            lock (textCompressorUnlocked)
            {
                textCompressorUnlocked.DecompressInPlace(ref proto.Text, ref proto.TextBpe);
                textCompressorUnlocked.DecompressInPlace(ref proto.ExternalTitle, ref proto.ExternalTitleBpe);
                textCompressorUnlocked.DecompressInPlace(ref proto.ExternalDescription, ref proto.ExternalDescriptionBpe);
                textCompressorUnlocked.DecompressInPlace(ref proto.ExternalUrl, ref proto.ExternalUrlBpe);
                if (proto.Media != null)
                {
                    foreach (var media in proto.Media)
                    {
                        textCompressorUnlocked.DecompressInPlace(ref media.AltText, ref media.AltTextBpe);
                    }
                }
                if (proto.Facets != null)
                {
                    foreach (var facet in proto.Facets)
                    {
                        textCompressorUnlocked.DecompressInPlace(ref facet.Link, ref facet.LinkBpe);
                    }
                }
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
            var isDeleted = PostDeletions.ContainsKey(id);
            
            BlueskyPostData? proto = null;
            if (PostData.TryGetPreserveOrderSpanAny(id, out var postDataCompressed))
            {
                proto = DeserializePostData(postDataCompressed.AsSmallSpan(), id);
            }
            else if (!isDeleted && FailedPostLookups.ContainsKey(id))
            {
                proto = new BlueskyPostData { Error = "This post could not be retrieved." };
            }

            if (isDeleted)
            {
                return new BlueskyPostData 
                { 
                    Deleted = true, 
                    Error = "This post was deleted.", 
                    RootPostPlc = proto?.RootPostPlc,
                    RootPostRKey = proto?.RootPostRKey,
                    InReplyToPlc = proto?.InReplyToPlc,
                    InReplyToRKey = proto?.InReplyToRKey,
                };
            }

            return proto;
        }

        internal static BlueskyPostData DeserializePostData(ReadOnlySpan<byte> postDataCompressed, PostId postId)
        {
            var encoding = (PostDataEncoding)postDataCompressed[0];
            postDataCompressed = postDataCompressed.Slice(1);

            using var ms = new MemoryStream(postDataCompressed.Length);
            ms.Write(postDataCompressed);
            ms.Seek(0, SeekOrigin.Begin);

            if (encoding == PostDataEncoding.Proto)
            {

                var proto = ProtoBuf.Serializer.Deserialize<BlueskyPostData>(ms);
                Decompress(proto, postId);
                return proto;
            }
            //if (encoding == PostDataEncoding.BrotliProto)
            //{

            //    using var decompress = new BrotliStream(ms, CompressionMode.Decompress);
            //    var proto = ProtoBuf.Serializer.Deserialize<BlueskyPostData>(decompress);
            //    Decompress(proto, postId);
            //    return proto;
            //}
            else
            {
                var proto = new BlueskyPostData();
                
                using var br = new BinaryReader(ms);
                if ((encoding & PostDataEncoding.Language) != 0)
                {
                    proto.Language = (LanguageEnum)br.ReadByte();
                }

                if ((encoding & PostDataEncoding.InReplyToRKey) != 0)
                {
                    proto.InReplyToRKey = br.ReadInt64();
                    if ((encoding & PostDataEncoding.InReplyToPlc) != 0)
                    {
                        proto.InReplyToPlc = br.ReadInt32();
                    }
                }

                if ((encoding & PostDataEncoding.RootPostRKey) != 0)
                {
                    proto.RootPostRKey = br.ReadInt64();
                    if ((encoding & PostDataEncoding.RootPostPlc) != 0)
                    {
                        proto.RootPostPlc = br.ReadInt32();
                    }
                }
                var bpeLength = ms.Length - ms.Position;
                proto.TextBpe = br.ReadBytes((int)bpeLength);
                
                Decompress(proto, postId);
                return proto;
            }
        }

        public static T DeserializeProto<T>(ReadOnlySpan<byte> bytes)
        {
            using var ms = new MemoryStream(bytes.Length);
            ms.Write(bytes);
            ms.Seek(0, SeekOrigin.Begin);
            return ProtoBuf.Serializer.Deserialize<T>(ms);
        }



        public BlueskyProfile GetProfile(Plc plc, Tid? relationshipRKey = null)
        {
            var basic = GetProfileBasicInfo(plc);
            var did = GetDid(plc);
            var didDoc = TryGetLatestDidDoc(plc);
            return new BlueskyProfile()
            {
                PlcId = plc.PlcValue,
                Did = did,
                BasicData = basic,
                RelationshipRKey = relationshipRKey,
                PossibleHandle = didDoc?.Handle,
            };
        }

        public DidDocProto? TryGetLatestDidDoc(Plc plc)
        {
            if (DidDocs.TryGetPreserveOrderSpanLatest(plc, out var bytes))
            {
                //var proto = DeserializeProto<DidDocProto>(bytes.AsSmallSpan());
                var proto = DidDocProto.DeserializeFromBytes(bytes.AsSmallSpan());
                DecompressDidDoc(proto);
                return proto;
            }
            return null;
        }

        public BlockReason GetBlockReason(Plc plc, RequestContext? ctx)
        {
            return ctx?.IsLoggedIn == true ? UsersHaveBlockRelationship(ctx.LoggedInUser, plc) : default;
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

        public BlueskyFullProfile GetFullProfile(string did, RequestContext ctx, int followersYouFollowToLoad)
        {
            var plc = SerializeDid(did);
            return new BlueskyFullProfile
            {
                Profile = GetProfile(plc),
                Followers = Follows.GetActorCount(plc),
                FollowedByPeopleYouFollow = ctx.IsLoggedIn ? GetFollowersYouFollow(plc, ctx.LoggedInUser)?.Select((x, i) => i < followersYouFollowToLoad ? GetProfile(x) : new BlueskyProfile { PlcId = x.PlcValue } ).ToList() : null,
                HasFeeds = FeedGenerators.GetInRangeUnsorted(new RelationshipHashedRKey(plc, 0), new RelationshipHashedRKey(plc.GetNext(), 0)).Any(),
                HasLists = Lists.GetInRangeUnsorted(new Relationship(plc, default), new Relationship(plc.GetNext(), default)).Any(),
            };
        }

        public ProfilesAndContinuation GetFollowersYouFollow(string did, string? continuation, int limit, RequestContext ctx)
        {
            if (!ctx.IsLoggedIn) return new ProfilesAndContinuation();
            var offset = continuation != null ? int.Parse(continuation) : 0;
            var plc = SerializeDid(did);
            var everything = GetFollowersYouFollow(plc, ctx.LoggedInUser);
            var r = everything.AsEnumerable();
            if (continuation != null) r = r.Skip(int.Parse(continuation));
            r = r.Take(limit);
            var page = r.Select(x => GetProfile(x)).ToArray();
            var end = offset + page.Length;
            return new ProfilesAndContinuation(page, end < everything.Length ? end.ToString() : null);
        }

        private Plc[] GetFollowersYouFollow(Plc plc, Plc loggedInUser)
        {
            var myFollowees = RegisteredUserToFollowees
                .GetValuesSorted(loggedInUser)
                .DistinctByAssumingOrderedInputLatest(x => x.Member);
            var followers = Follows.GetRelationshipsSorted(plc, default).Select(x => x.Actor).DistinctAssumingOrderedInput();

            return SimpleJoin.JoinPresortedAndUnique(myFollowees, x => x.Member, followers, x => x)
                .Where(x => x.Left != default && x.Right != default && !Follows.deletions.ContainsKey(new Relationship(loggedInUser, x.Left.ListItemRKey)))
                .Select(x => x.Key)
                .ToArray();
        }

        internal void EnsureNotDisposed()
        {
            if (_disposed)
            {
                ShutdownRequested.Token.ThrowIfCancellationRequested();
                throw new ObjectDisposedException(nameof(BlueskyRelationships));
            }
        }


        private CancellationTokenSource ShutdownRequested = new CancellationTokenSource();
        public void NotifyShutdownRequested()
        {
            lock (this)
            {
                ShutdownRequested.Cancel();
                Dispose();
            }
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
        internal const int SearchIndexFeedPopularityMinLikes = 2;

        internal void MaybeIndexPopularPost(PostId postId, string indexName, long approxPopularity, int minPopularityForIndex)
        {

            if (BitOperations.IsPow2(approxPopularity) && approxPopularity >= minPopularityForIndex)
            {
                AddToSearchIndex("%" + indexName + "-" + approxPopularity, GetApproxTime32(postId.PostRKey));
            }
        }
        internal void MaybeIndexPopularFeed(RelationshipHashedRKey feedId, string indexName, long approxPopularity, int minPopularityForIndex)
        {

            if (BitOperations.IsPow2(approxPopularity) && approxPopularity >= minPopularityForIndex)
            {
                FeedGeneratorSearch.Add(HashWord("%" + indexName + "-" + approxPopularity), feedId);
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

        internal static ListData ListToProto(FishyFlip.Lexicon.App.Bsky.Graph.List list)
        {
            return new ListData
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
                DescriptionFacets = GetFacetsAsProtos(list.DescriptionFacets),
            };

            
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
            return SerializeProto(proto, x => x.Dummy = true);
        }
        internal ReadOnlySpan<byte> SerializePostgateToBytes(Postgate postgate)
        {
            var proto = new BlueskyPostgate
            {
                 DetachedEmbeddings = postgate.DetachedEmbeddingUris?.Select(x => RelationshipProto.FromPostId(GetPostId(x))).ToArray(),
                 DisallowQuotes = postgate.EmbeddingRules?.Any(x => x is DisableRule) ?? false
            };
            return SerializeProto(proto, x => x.Dummy = true);
        }

        public static ReadOnlySpan<byte> SerializeProto<T>(T proto, Action<T>? setDummyValue = null)
        {
            using var protoMs = new MemoryStream();
            ProtoBuf.Serializer.Serialize(protoMs, proto);
            if (protoMs.Length == 0)
            {
                // Zero-length values are not supported in CombinedPersistentMultiDictionary
                if (setDummyValue == null) throw new Exception("Cannot serialize zero-length-serializing protos unless setDummyValue is provided.");
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
            if (a == b) return default;
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
            if (ListDeletions.ContainsKey(list))
                return false;

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

            foreach (var (subscriptionId, singleList) in ListBlocks.GetInRangeUnsorted(new Relationship(subscriber, default), new Relationship(subscriber.GetNext(), default)))
            {
                if (ListBlockDeletions.ContainsKey(subscriptionId))
                    continue;

                if (singleList.Count != 1) throw new Exception(); // it's a SingleValue

                lists.Add(singleList[0]);
            }

            return lists;
        }

        public void RegisterForNotifications(Plc user)
        {
            if (IsRegisteredForNotifications(user)) return;
            LastSeenNotifications.Add(user, new Notification((ApproximateDateTime32)DateTime.UtcNow, default, default, default));
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
            if (SuppressNotificationGeneration != 0) return;
            if (destination == actor) return;
            if (!IsRegisteredForNotifications(destination)) return;
            if (UsersHaveBlockRelationship(destination, actor) != default) return;
            Notifications.Add(destination, new Notification((ApproximateDateTime32)DateTime.UtcNow, actor, rkey, kind));
            UserNotificationSubscribersThreadSafe.MaybeNotify(destination, handler => handler(GetNotificationCount(destination)));
        }

        public int SuppressNotificationGeneration;




        public BlueskyNotification? RehydrateNotification(Notification notification, Plc destination)
        {
            (PostId post, Plc actor) = notification.Kind switch
            {
                NotificationKind.FollowedYou or NotificationKind.FollowedYouBack => (default, notification.Actor),
                NotificationKind.LikedYourPost or NotificationKind.RepostedYourPost => (new PostId(destination, notification.RKey), notification.Actor),
                NotificationKind.RepliedToYourPost or NotificationKind.RepliedToYourThread or NotificationKind.QuotedYourPost => (new PostId(notification.Actor, notification.RKey), notification.Actor),
                _ => default
            };

            BlueskyFeedGenerator? feed = null;
            if (notification.Kind == NotificationKind.LikedYourFeed)
            {
                feed = TryGetFeedGenerator(new RelationshipHashedRKey(destination, (ulong)notification.RKey.TidValue));
                actor = notification.Actor;
                post = default;
            }

            if (post == default && actor == default && feed == null) return null;

            
            return new BlueskyNotification 
            { 
                EventDate = notification.EventDate,
                Kind = notification.Kind, 
                Post = post != default ? GetPost(post) : null, Profile = actor != default ? GetProfile(actor) : default,
                Hidden = actor != default && UsersHaveBlockRelationship(destination, actor) != default,
                NotificationCore = notification,
                Feed = feed,
            };

            
            
        }

        public long GetNotificationCount(Plc user)
        {
            if (!LastSeenNotifications.TryGetLatestValue(user, out var threshold)) return 0;

            long count = 0;
            foreach (var chunk in Notifications.GetValuesChunked(user, threshold))
            {
                count += chunk.Count;
            }
            return count;
        }

        public (BlueskyNotification[] NewNotifications, BlueskyNotification[] OldNotifications, Notification NewestNotification) GetNotificationsForUser(Plc user)
        {
            if (!LastSeenNotifications.TryGetLatestValue(user, out var threshold)) return ([], [], default);
            var newNotificationsCore = Notifications.GetValuesSortedDescending(user, threshold, null).ToArray();

            Notification? newestNotification = newNotificationsCore.Length != 0 ? newNotificationsCore[0] : null;

            var newNotifications = 
                newNotificationsCore
                .Select(x => RehydrateNotification(x, user))
                .Where(x => x != null)
                .ToArray();

            Notification? oldestNew = newNotificationsCore.Length != 0 ? newNotificationsCore[^1] : null;

            var distinctOldCoalesceKeys = new HashSet<NotificationCoalesceKey>();

            var oldNotifications =
                Notifications.GetValuesSortedDescending(user, null, oldestNew)
                .Select(x =>
                {
                    newestNotification ??= x;
                    return RehydrateNotification(x, user);
                })
                .Where(x => x != null)
                .TakeWhile(x => 
                {
                    distinctOldCoalesceKeys.Add(x.CoalesceKey);
                    if (distinctOldCoalesceKeys.Count > 10) return false;
                    return true;
                })
                .ToArray();

            return (newNotifications, oldNotifications, newestNotification ?? default);

        }

        internal IEnumerable<(PostId PostId, Plc InReplyTo)> EnumerateRecentPosts(Plc author, Tid minDate, Tid? maxDate)
        {
            return this.UserToRecentPosts.GetValuesSortedDescending(author, new RecentPost(minDate, default), maxDate != null ? new RecentPost(maxDate.Value, default) : null).Select(x => (new PostId(author, x.RKey), x.InReplyTo));
        }
        internal IEnumerable<RecentRepost> EnumerateRecentReposts(Plc author, Tid minDate, Tid? maxDate)
        {
            return this.UserToRecentReposts.GetValuesSortedDescending(author, new RecentRepost(minDate, default), maxDate != null ? new RecentRepost(maxDate.Value, default) : null);
        }

        public IEnumerable<BlueskyPost> EnumerateFollowingFeed(Plc loggedInUser, DateTime minDate, Tid? maxTid)
        {
            var thresholdDate = minDate != default ? Tid.FromDateTime(minDate, 0) : default;

            
            var follows = RegisteredUserToFollowees.GetValuesUnsorted(loggedInUser).ToArray();
            var plcToLatestFollowRkey = new Dictionary<Plc, Tid>();
            foreach (var follow in follows)
            {
                ref var f = ref CollectionsMarshal.GetValueRefOrAddDefault(plcToLatestFollowRkey, follow.Member, out _);
                if (f.CompareTo(follow.ListItemRKey) < 0)
                    f = follow.ListItemRKey;
            }
            var requireFollowStillValid = new Dictionary<BlueskyPost, (Tid A, Tid B, Tid C)>();
            var followPrecise = new Dictionary<Tid, bool>();

            bool IsFollowStillValid(Tid followRkey)
            {
                if (followRkey == default) return true;
                ref var stillValid = ref CollectionsMarshal.GetValueRefOrAddDefault(followPrecise, followRkey, out var exists);
                if (!exists)
                    stillValid = !Follows.deletions.ContainsKey(new Relationship(loggedInUser, followRkey));
                return stillValid;
            }
            bool IsFollowPossiblyStillValid(Tid followRkey)
            {
                if (followRkey == default) return true;
                if (followPrecise.TryGetValue(followRkey, out var stillValid))
                    return stillValid;
                return true;
            }

            var usersRecentPosts =
                follows
                .Select(author =>
                {
                    return this
                        .EnumerateRecentPosts(author.Member, thresholdDate, maxTid)
                        .Select(x =>
                        {
                            Tid inReplyToTid = default;
                            Tid rootTid = default;
                            var postAuthor = author.Member;
                            var parentAuthor = x.InReplyTo;

                            if (!IsFollowPossiblyStillValid(author.ListItemRKey))
                                return null;

                            if (parentAuthor != default && parentAuthor != postAuthor)
                            {
                                if (!plcToLatestFollowRkey.TryGetValue(parentAuthor, out inReplyToTid))
                                    return null;
                                if (!IsFollowPossiblyStillValid(inReplyToTid)) 
                                    return null;
                            }

                            var post = GetPost(x.PostId);
                            if (post.Data == null) return null;

                            var rootAuthor = post.RootPostId.Author;
                            if (rootAuthor != postAuthor && rootAuthor != parentAuthor)
                            {
                                if (!plcToLatestFollowRkey.TryGetValue(rootAuthor, out rootTid))
                                    return null;
                                if (!IsFollowPossiblyStillValid(rootTid))
                                    return null;
                            }

                            requireFollowStillValid[post] = (author.ListItemRKey, inReplyToTid, rootTid);
                            return post;
                        })
                        .Where(x => x != null);
                });
            var usersRecentReposts =
                follows
                .Select(reposter =>
                {
                    BlueskyProfile? reposterProfile = null;
                    return this
                        .EnumerateRecentReposts(reposter.Member, thresholdDate, maxTid)
                        .Select(x =>
                        {
                            var post = GetPost(x.PostId);
                            post.RepostedBy = (reposterProfile ??= GetProfile(reposter.Member));
                            post.RepostDate = x.RepostRKey.Date;
                            requireFollowStillValid[post] = (reposter.ListItemRKey, default, default);
                            return post;
                        });
                });
            var result = 
                SimpleJoin.ConcatPresortedEnumerablesKeepOrdered(usersRecentPosts.Concat(usersRecentReposts).ToArray(), x => x.RepostDate != null ? Tid.FromDateTime(x.RepostDate.Value, 0) : x.PostId.PostRKey, new ReverseComparer<Tid>())
                .Where(x =>
                {
                    var triplet = requireFollowStillValid[x];
                    return IsFollowStillValid(triplet.A) && IsFollowStillValid(triplet.B) && IsFollowStillValid(triplet.C);
                })!;
            return result;
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
                yield return post;
            }
        }

        internal RepositoryImportEntry[] GetRepositoryImports(Plc plc)
        {
            return
                CarImports.GetInRangeUnsorted(new RepositoryImportKey(plc, default), new RepositoryImportKey(plc.GetNext(), default))
                .Select(x =>
                {
                    var p = DeserializeProto<RepositoryImportEntry>(x.Values.AsSmallSpan());
                    p.StartDate = x.Key.ImportDate;
                    p.Plc = plc;
                    return p;
                })
                .ToArray();

        }

        public AppViewLiteProfileProto? TryGetAppViewLiteProfile(Plc plc)
        {
            if (AppViewLiteProfiles.TryGetPreserveOrderSpanLatest(plc, out var appviewProfileBytes))
            {
                return BlueskyRelationships.DeserializeProto<AppViewLiteProfileProto>(appviewProfileBytes.AsSmallSpan());
            }
            return null;
        }

        public void StoreAppViewLiteProfile(Plc plc, AppViewLiteProfileProto profile)
        {
            AppViewLiteProfiles.AddRange(plc, SerializeProto(profile));
        }

        internal void MaybeGlobalFlush()
        {
            if (lastGlobalFlush.Elapsed.TotalMinutes >= 5)
            {
                Console.Error.WriteLine("====== START OF GLOBAL PERIODIC FLUSH ======");
                GlobalFlush();
                Console.Error.WriteLine("====== END OF GLOBAL PERIODIC FLUSH ======");
            }
        }

        private void GlobalFlush()
        {
            if (IsReadOnly) throw new InvalidOperationException();
            foreach (var table in disposables)
            {
                table.Flush(false);
            }
            CaptureCheckpoint();
            lastGlobalFlush.Restart();
        }

        public SubscriptionDictionary<PostId, LiveNotificationDelegate> PostLiveSubscribersThreadSafe = new();
        public SubscriptionDictionary<Plc, Action<long>> UserNotificationSubscribersThreadSafe = new();

        internal void NotifyPostStatsChange(PostId postId, Plc commitPlc)
        {
            PostLiveSubscribersThreadSafe.MaybeNotify(postId, (handler) => handler(new PostStatsNotification(postId, GetDid(postId.Author), postId.PostRKey.ToString(), Likes.GetActorCount(postId), Reposts.GetActorCount(postId), Quotes.GetValueCount(postId), DirectReplies.GetValueCount(postId)), commitPlc));
        }

        //private Dictionary<PostId, int> notifDebug = new();



        private Dictionary<string, (TimeSpan TotalTime, long Count)> recordTypeDurations = new();


        public BlueskyList GetList(Relationship listId, ListData? listData = null)
        {
            var did = GetDid(listId.Actor);
            return new BlueskyList
            {
                Did = did,
                ListId = listId,
                Data = listData ?? TryGetListData(listId),
                ListIdStr = new RelationshipStr(did, listId.RelationshipRKey.ToString()),
                Author = GetProfile(listId.Actor)
            };
        }

        public ListData? TryGetListData(Relationship listId)
        {
            if (this.Lists.TryGetPreserveOrderSpanLatest(listId, out var listMetadataBytes))
            {
                if (this.ListDeletions.ContainsKey(listId))
                {
                    return new ListData
                    {
                        Error = "This list was deleted.",
                        Deleted = true
                    };
                }
                return DeserializeProto<ListData>(listMetadataBytes.AsSmallSpan());
            }
            if (this.FailedListLookups.ContainsKey(listId))
                return new ListData { Error = "This list could not be retrieved." };
            return null;
        }


        public void IndexFeedGenerator(Plc plc, string rkey, Generator generator)
        {
            var key = new RelationshipHashedRKey(plc, rkey);

            var proto = new BlueskyFeedGeneratorData
            {
                DisplayName = generator.DisplayName,
                AvatarCid = generator.Avatar?.Ref?.Link?.ToArray(),
                Description = generator.Description,
                DescriptionFacets = GetFacetsAsProtos(generator.DescriptionFacets),
                RetrievalDate = generator.CreatedAt ?? DateTime.UtcNow,
                ImplementationDid = generator.Did!.Handler,
                //IsVideo = generator.ContentMode == "contentModeVideo",
                AcceptsInteractions = generator.AcceptsInteractions,
                RKey = rkey,
            };

            foreach (var wordHash in StringUtils.GetAllWords(proto.DisplayName).Concat(StringUtils.GetAllWords(proto.Description)).Select(x => HashWord(x)).Distinct())
            {
                FeedGeneratorSearch.AddIfMissing(wordHash, key);
            }
            FeedGenerators.AddRange(key, SerializeProto(proto));

        }

        public BlueskyFeedGeneratorData? TryGetFeedGeneratorData(RelationshipHashedRKey feedId)
        {
            if (FeedGenerators.TryGetPreserveOrderSpanLatest(feedId, out var bytes))
            {
                var proto = DeserializeProto<BlueskyFeedGeneratorData>(bytes.AsSmallSpan());
                MakeUtcAfterDeserialization(ref proto.RetrievalDate);
                if (FeedGeneratorDeletions.TryGetLatestValue(feedId, out var deletionDate) && deletionDate > proto.RetrievalDate)
                {
                    return new BlueskyFeedGeneratorData { RKey = proto.RKey, Deleted = true, Error = "This feed was deleted." };
                }
                return proto;
            }
            return null;
        }

        private static void MakeUtcAfterDeserialization(ref DateTime date)
        {
            date = new DateTime(date.Ticks, DateTimeKind.Utc);
        }

        public BlueskyFeedGenerator GetFeedGenerator(Plc plc, BlueskyFeedGeneratorData data)
        {
            return GetFeedGenerator(plc, data.RKey, data);
        }
        public BlueskyFeedGenerator GetFeedGenerator(Plc plc, string rkey, BlueskyFeedGeneratorData? data = null)
        {
            data ??= TryGetFeedGeneratorData(new RelationshipHashedRKey(plc, rkey));
            return new BlueskyFeedGenerator
            {
                 Data = data,
                 Did = GetDid(plc),
                 RKey = rkey,
                 Author = GetProfile(plc),
                 LikeCount = FeedGeneratorLikes.GetActorCount(new(plc, rkey))
            };
        }

        public BlueskyFeedGenerator? TryGetFeedGenerator(RelationshipHashedRKey feedId)
        {
            var data = TryGetFeedGeneratorData(feedId);
            if (data == null) return null;
            return GetFeedGenerator(feedId.Plc, data);
        }

        internal object? TryGetAtObject(string? aturi)
        {
            if (aturi == null) return null;
            var parsed = new ATUri(aturi);

            var plc = SerializeDid(parsed.Did!.Handler);
            if (parsed.Collection == Generator.RecordType)
            {
                return GetFeedGenerator(plc, parsed.Rkey);
            }

            if (parsed.Collection == FishyFlip.Lexicon.App.Bsky.Graph.List.RecordType)
            {
                return GetList(new Relationship(plc, Tid.Parse(parsed.Rkey)));
            }

            return null;

        }

        public BlueskyPostgate? TryGetPostgate(PostId postid)
        {
            if (Postgates.TryGetPreserveOrderSpanLatest(postid, out var postgateBytes))
            {
                return BlueskyRelationships.DeserializeProto<BlueskyPostgate>(postgateBytes.AsSmallSpan());
            }
            return null;
        }
        public BlueskyThreadgate? TryGetThreadgate(PostId postid)
        {
            if (Threadgates.TryGetPreserveOrderSpanLatest(postid, out var threadgateBytes))
            {
                return BlueskyRelationships.DeserializeProto<BlueskyThreadgate>(threadgateBytes.AsSmallSpan());
            }
            return null;
        }


        public bool ThreadgateAllowsUser(PostId rootPostId, BlueskyThreadgate threadgate, Plc replyAuthor)
        {
            if (replyAuthor == rootPostId.Author) return true;

            if (!threadgate.AllowlistedOnly) return true;

            if (threadgate.AllowFollowing)
            {
                if (Follows.HasActor(rootPostId.Author, replyAuthor, out _))
                    return true;
            }

            if (threadgate.AllowMentioned)
            {
                var op = TryGetPostData(rootPostId);
                if (op?.Facets != null)
                {
                    var rootDid = GetDid(replyAuthor);
                    if (op?.Facets?.Any(x => x.Did == rootDid) == true)
                        return true;
                }
            }

            if (threadgate.AllowLists != null)
            {
                if (threadgate.AllowLists.Any(x => IsMemberOfList(x.RelationshipId, replyAuthor)))
                    return true;
            }

            return false;
        }


        internal void CompressDidDoc(DidDocProto proto)
        {
            if (proto.Pds == null) return;
            var pds = proto.Pds;

            var pdsId = SerializePds(pds);
            proto.PdsId = pdsId.PdsId;
            proto.Pds = null;
        }

        internal void DecompressDidDoc(DidDocProto proto)
        {
            if (proto.PdsId == null) return;
            proto.Pds = DeserializePds(new Pds(proto.PdsId.Value));
            proto.PdsId = null;
        }
        public Pds SerializePds(string pds)
        {
            var pdsHash = StringUtils.HashUnicodeToUuid(pds);

            if (!PdsHashToPdsId.TryGetSingleValue(pdsHash, out var pdsId))
            {
                pdsId = new Pds(checked((PdsIdToString.MaximumKey?.PdsId ?? default) + 1));
                PdsIdToString.AddRange(pdsId, Encoding.UTF8.GetBytes(pds));
                PdsHashToPdsId.Add(pdsHash, pdsId);
            }

            var roundtrip = DeserializePds(pdsId);
            if (roundtrip != pds) throw new Exception("PDS serialization did not roundtrip: " + pds + "/" + roundtrip);

            return pdsId;
        }

        public string DeserializePds(Pds pds)
        {
            if (pds == default) throw new ArgumentException();
            if (PdsIdToString.TryGetPreserveOrderSpanAny(pds, out var utf8))
                return Encoding.UTF8.GetString(utf8.AsSmallSpan());
            throw new Exception("Unknown PDS id:" + pds);
        }

        internal void IndexHandleCore(string? handle, string did, Plc plc)
        {
            if (handle == null) return;

            HandleToPossibleDids.Add(BlueskyRelationships.HashWord(handle), plc);

            if (handle.EndsWith(".bsky.social", StringComparison.Ordinal))
            {
                handle = handle.Substring(0, handle.Length - ".bsky.social".Length);
            }
            foreach (var word in StringUtils.GetDistinctWords(handle))
            {
                IndexProfileWord(word, plc);
            }
        }

        internal void IndexHandle(string? handle, string did)
        {
            var plc = SerializeDid(did);
            IndexHandleCore(handle, did, plc);

            if (did.StartsWith("did:web:", StringComparison.Ordinal)) 
            {
                var domain = did.Substring(8);
                if (domain != handle)
                    IndexHandleCore(domain, did, plc);
            }
        }

        internal void AddHandleToDidVerification(string handle, Plc plc)
        {
            if (string.IsNullOrEmpty(handle) || handle.Contains(':') || plc == default) throw new ArgumentException();
            HandleToDidVerifications.Add(StringUtils.HashUnicodeToUuid(handle), new HandleVerificationResult((ApproximateDateTime32)DateTime.UtcNow, plc));
        }
    }

    public delegate void LiveNotificationDelegate(PostStatsNotification notification, Plc commitPlc);
}

