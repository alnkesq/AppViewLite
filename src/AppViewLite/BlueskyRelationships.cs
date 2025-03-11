using FishyFlip.Lexicon.App.Bsky.Actor;
using FishyFlip.Lexicon.App.Bsky.Embed;
using FishyFlip.Lexicon.App.Bsky.Feed;
using FishyFlip.Lexicon.App.Bsky.Richtext;
using FishyFlip.Lexicon.Com.Atproto.Repo;
using FishyFlip.Models;
using AppViewLite.Models;
using AppViewLite.Numerics;
using AppViewLite.Storage;
using DuckDbSharp.Types;
using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Hashing;
using System.Linq;
using System.Numerics;
using AppViewLite.PluggableProtocols;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace AppViewLite
{
    public class BlueskyRelationships : IDisposable, ICloneableAsReadOnly
    {

        public long Version = 1;
        public int ManagedThreadIdWithWriteLock;
        public int ForbidUpgrades;
        private Stopwatch lastGlobalFlush = Stopwatch.StartNew();
        public CombinedPersistentMultiDictionary<DuckDbUuid, Plc> DidHashToUserId;
        public RelationshipDictionary<PostIdTimeFirst> Likes;
        public RelationshipDictionary<PostIdTimeFirst> Reposts;
        public RelationshipDictionary<Plc> Follows;
        public RelationshipDictionary<Plc> Blocks;
        public CombinedPersistentMultiDictionary<Plc, BookmarkPostFirst> Bookmarks;
        public CombinedPersistentMultiDictionary<Plc, BookmarkDateFirst> RecentBookmarks;
        public CombinedPersistentMultiDictionary<Plc, Tid> BookmarkDeletions;
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
        public CombinedPersistentMultiDictionary<PostId, PostId> Quotes;
        public CombinedPersistentMultiDictionary<PostId, DateTime> PostDeletions;
        public CombinedPersistentMultiDictionary<Plc, byte> Profiles;
        public CombinedPersistentMultiDictionary<Plc, byte> PlcToDidOther;
        public CombinedPersistentMultiDictionary<Plc, UInt128> PlcToDidPlc;
        public CombinedPersistentMultiDictionary<PostIdTimeFirst, byte> PostData;
        public CombinedPersistentMultiDictionary<Plc, RecentPostLikeCount> RecentPluggablePostLikeCount;
        public CombinedPersistentMultiDictionary<HashedWord, ApproximateDateTime32> PostTextSearch;
        public CombinedPersistentMultiDictionary<Plc, DateTime> FailedProfileLookups;
        public CombinedPersistentMultiDictionary<Relationship, DateTime> FailedListLookups;
        public CombinedPersistentMultiDictionary<PostId, DateTime> FailedPostLookups;
        public CombinedPersistentMultiDictionary<Plc, Notification> LastSeenNotifications;
        public CombinedPersistentMultiDictionary<Plc, Notification> Notifications;
        public CombinedPersistentMultiDictionary<Plc, ListEntry> RegisteredUserToFollowees;
        public CombinedPersistentMultiDictionary<Plc, Plc> RssFeedToFollowers;
        public CombinedPersistentMultiDictionary<Plc, RecentPost> UserToRecentPosts;
        public CombinedPersistentMultiDictionary<Plc, Tid> UserToRecentMediaPosts;
        public CombinedPersistentMultiDictionary<Plc, RecentRepost> UserToRecentReposts;
        public CombinedPersistentMultiDictionary<RepositoryImportKey, byte> CarImports;
        public CombinedPersistentMultiDictionary<Plc, byte> AppViewLiteProfiles;
        public CombinedPersistentMultiDictionary<Plc, byte> DidDocs;
        public CombinedPersistentMultiDictionary<HashedWord, Plc> HandleToPossibleDids;
        public CombinedPersistentMultiDictionary<Pds, byte> PdsIdToString;
        public CombinedPersistentMultiDictionary<DuckDbUuid, Pds> PdsHashToPdsId;
        public CombinedPersistentMultiDictionary<DateTime, int> LastRetrievedPlcDirectoryEntry;
        public CombinedPersistentMultiDictionary<DuckDbUuid, HandleVerificationResult> HandleToDidVerifications;
        public CombinedPersistentMultiDictionary<PostId, LabelEntry> PostLabels;
        public CombinedPersistentMultiDictionary<Plc, LabelEntry> ProfileLabels;
        public CombinedPersistentMultiDictionary<ulong, byte> LabelNames;
        public CombinedPersistentMultiDictionary<LabelId, byte> LabelData;
        public CombinedPersistentMultiDictionary<DuckDbUuid, byte> CustomEmojis;
        public CombinedPersistentMultiDictionary<DuckDbUuid, byte> KnownMirrorsToIgnore;
        public CombinedPersistentMultiDictionary<DuckDbUuid, Tid> ExternalPostIdHashToSyntheticTid;
        public CombinedPersistentMultiDictionary<Plc, PostEngagement> SeenPosts;
        public CombinedPersistentMultiDictionary<Plc, TimePostSeen> SeenPostsByDate;
        public CombinedPersistentMultiDictionary<Plc, byte> RssRefreshInfos;
        public CombinedPersistentMultiDictionary<DuckDbUuid, byte> NostrSeenPubkeyHashes;

        public ConcurrentFullEvictionCache<DuckDbUuid, Plc> DidToPlcConcurrentCache;
        public ConcurrentFullEvictionCache<Plc, string> PlcToDidConcurrentCache;

        public DateTime PlcDirectorySyncDate;
        private Plc LastAssignedPlc;
        public TimeSpan PlcDirectoryStaleness => DateTime.UtcNow - PlcDirectorySyncDate;

        public Stopwatch? ReplicaAge;

        private HashSet<Plc> registerForNotificationsCache = new();
        private List<ICheckpointable> disposables = new();

        public IReadOnlyList<CombinedPersistentMultiDictionary> AllMultidictionaries => disposables.OfType<CombinedPersistentMultiDictionary>().Concat(disposables.OfType<RelationshipDictionary>().SelectMany(x => x.Multidictionaries)).ToArray();


        public static int TableWriteBufferSize = AppViewLiteConfiguration.GetInt32(AppViewLiteParameter.APPVIEWLITE_TABLE_WRITE_BUFFER_SIZE) ?? (10 * 1024 * 1024);
        public int AvoidFlushes;
        public ReaderWriterLockSlim Lock;

        public bool IsAtLeastVersion(long minVersion, TimeSpan maxStaleness, long latestKnownVersion)
        {
            return this.Version >= minVersion && (this.Version == latestKnownVersion || ReplicaAge!.Elapsed <= maxStaleness);
        }

        public BlueskyRelationships()
            : this(
                  AppViewLiteConfiguration.GetString(AppViewLiteParameter.APPVIEWLITE_DIRECTORY) ?? Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "BskyAppViewLiteData"), 
                  AppViewLiteConfiguration.GetBool(AppViewLiteParameter.APPVIEWLITE_READONLY) ?? false)
        {
        }

        public string BaseDirectory { get; }
        private IDisposable lockFile;
        private Dictionary<string, string[]>? checkpointToLoad;
        private GlobalCheckpoint loadedCheckpoint;
        internal Dictionary<string, Dictionary<nuint, bool>> AccessedMemoryPagesByTaskType = new();

        private T Register<T>(T r) where T : ICheckpointable
        {
            disposables.Add(r);
            return r;
        }
        private CombinedPersistentMultiDictionary<TKey, TValue> RegisterDictionary<TKey, TValue>(string name, PersistentDictionaryBehavior behavior = PersistentDictionaryBehavior.SortedValues, Func<IEnumerable<TValue>, IEnumerable<TValue>>? onCompactation = null) where TKey : unmanaged, IComparable<TKey> where TValue : unmanaged, IComparable<TValue>, IEquatable<TValue>
        {
            return Register(new CombinedPersistentMultiDictionary<TKey, TValue>(
                BaseDirectory + "/" + name,
                checkpointToLoad!.TryGetValue(name, out var slices) ? slices : [],
                behavior
            ) {
                WriteBufferSize = TableWriteBufferSize,
                OnCompactation = onCompactation 
            });
        }
        private RelationshipDictionary<TTarget> RegisterRelationshipDictionary<TTarget>(string name, Func<TTarget, bool, UInt24?>? targetToApproxTarget) where TTarget : unmanaged, IComparable<TTarget>
        {
            return Register(new RelationshipDictionary<TTarget>(BaseDirectory, name, checkpointToLoad!, targetToApproxTarget));
        }

        public bool IsReadOnly { get; private set; }

#nullable disable
        private BlueskyRelationships(bool isReadOnly)
        { 
            // set via reflection
        }
#nullable restore

        public BlueskyRelationships(string basedir, bool isReadOnly)
            : this(isReadOnly)
        {
            DidToPlcConcurrentCache = new(512 * 1024);
            PlcToDidConcurrentCache = new(128 * 1024);
            Lock = new ReaderWriterLockSlim();
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
            PlcToDidPlc = RegisterDictionary<Plc, UInt128>("plc-to-did-plc", PersistentDictionaryBehavior.SingleValue);
            PlcToDidOther = RegisterDictionary<Plc, byte>("plc-to-did-other", PersistentDictionaryBehavior.PreserveOrder);
            PdsIdToString = RegisterDictionary<Pds, byte>("pds-id-to-string", PersistentDictionaryBehavior.PreserveOrder);
            PdsHashToPdsId = RegisterDictionary<DuckDbUuid, Pds>("pds-hash-to-id", PersistentDictionaryBehavior.SingleValue);


            Likes = RegisterRelationshipDictionary<PostIdTimeFirst>("post-like-time-first", GetApproxTime24);
            Reposts = RegisterRelationshipDictionary<PostIdTimeFirst>("post-repost-time-first", GetApproxTime24);
            Follows = RegisterRelationshipDictionary<Plc>("follow", GetApproxPlc24);
            Blocks = RegisterRelationshipDictionary<Plc>("block", GetApproxPlc24);

            Bookmarks = RegisterDictionary<Plc, BookmarkPostFirst>("bookmark");
            RecentBookmarks = RegisterDictionary<Plc, BookmarkDateFirst>("bookmark-recent");
            BookmarkDeletions = RegisterDictionary<Plc, Tid>("bookmark-deletion");
            DirectReplies = RegisterDictionary<PostId, PostId>("post-reply-direct") ;
            Quotes = RegisterDictionary<PostId, PostId>("post-quote") ;
            PostDeletions = RegisterDictionary<PostId, DateTime>("post-deletion", PersistentDictionaryBehavior.SingleValue);
            Profiles = RegisterDictionary<Plc, byte>("profile-basic-2", PersistentDictionaryBehavior.PreserveOrder);
            ProfileSearchLong = RegisterDictionary<HashedWord, Plc>("profile-search-long");
            ProfileSearchDescriptionOnly = RegisterDictionary<HashedWord, Plc>("profile-search-description-only");
            ProfileSearchPrefix8 = RegisterDictionary<SizeLimitedWord8, Plc>("profile-search-prefix");
            ProfileSearchPrefix2 = RegisterDictionary<SizeLimitedWord2, Plc>("profile-search-prefix-2-letters");

            PostData = RegisterDictionary<PostIdTimeFirst, byte>("post-data-time-first-2", PersistentDictionaryBehavior.PreserveOrder) ;
            RecentPluggablePostLikeCount = RegisterDictionary<Plc, RecentPostLikeCount>("recent-post-like-count", onCompactation: x => x.DistinctByAssumingOrderedInputLatest(x => x.PostRKey));
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
            RssFeedToFollowers = RegisterDictionary<Plc, Plc>("registered-user-to-rss-feeds");

            UserToRecentPosts = RegisterDictionary<Plc, RecentPost>("user-to-recent-posts-2") ;
            UserToRecentReposts = RegisterDictionary<Plc, RecentRepost>("user-to-recent-reposts-2", onCompactation: x => { var threshold = DateTime.UtcNow.AddDays(-7); return x.Where((x, i) => i == 0 || x.RepostRKey.Date > threshold); });
            UserToRecentMediaPosts = RegisterDictionary<Plc, Tid>("user-to-recent-media-posts");

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

            PostLabels = RegisterDictionary<PostId, LabelEntry>("post-label");
            ProfileLabels = RegisterDictionary<Plc, LabelEntry>("profile-label");
            LabelNames = RegisterDictionary<ulong, byte>("label-name", PersistentDictionaryBehavior.PreserveOrder);
            LabelData = RegisterDictionary<LabelId, byte>("label-data", PersistentDictionaryBehavior.PreserveOrder);

            CustomEmojis = RegisterDictionary<DuckDbUuid, byte>("custom-emoji", PersistentDictionaryBehavior.PreserveOrder);
            KnownMirrorsToIgnore = RegisterDictionary<DuckDbUuid, byte>("known-mirror-ignore", PersistentDictionaryBehavior.SingleValue);
            ExternalPostIdHashToSyntheticTid = RegisterDictionary<DuckDbUuid, Tid>("external-post-id-to-synth-tid", PersistentDictionaryBehavior.SingleValue);
            SeenPosts = RegisterDictionary<Plc, PostEngagement>("seen-posts-2", onCompactation: CompactPostEngagements);
            SeenPostsByDate = RegisterDictionary<Plc, TimePostSeen>("seen-posts-by-date");
            RssRefreshInfos = RegisterDictionary<Plc, byte>("rss-refresh-info", PersistentDictionaryBehavior.PreserveOrder);
            NostrSeenPubkeyHashes = RegisterDictionary<DuckDbUuid, byte>("nostr-seen-pubkey-hashes", PersistentDictionaryBehavior.SingleValue);

            

            PlcDirectorySyncDate = LastRetrievedPlcDirectoryEntry.MaximumKey ?? new DateTime(2022, 11, 17, 00, 35, 16, DateTimeKind.Utc) /* first event on the PLC directory */;
            LastAssignedPlc = new Plc(Math.Max((PlcToDidOther.MaximumKey ?? default).PlcValue, (PlcToDidPlc.MaximumKey ?? default).PlcValue));

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
                    item.BeforeWrite += (_, _) => throw new InvalidOperationException("ReadOnly mode.");
                    item.AfterFlush += (_, _) => throw new InvalidOperationException("ReadOnly mode.");
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
                    item.BeforeWrite += (table, e) =>
                    {
                        if (!Lock.IsWriteLockHeld)
                            throw ThrowIncorrectLockUsageException("Cannot perform writes without holding the write lock.");
                    };
                }

            }
            checkpointToLoad = null;

            try 
            {
                Lock.EnterUpgradeableReadLock();

                var bskyModeration = SerializeDid("did:plc:ar7c4by46qjdydhdevvrndac", RequestContext.CreateForFirehose("DefaultLabelsInit"));
                // https://www.atproto-browser.dev/at/did:plc:ar7c4by46qjdydhdevvrndac/app.bsky.labeler.service/self
                this.DefaultLabelSubscriptions = new[]
                {

                    ("!takedown", ModerationBehavior.Badge),
                    ("!hide", ModerationBehavior.Badge),
                    ("!warn", ModerationBehavior.Badge),
                    //"porn",
                    //"sexual",
                    //"nudity",
                    //"sexual-figurative",
                    //"graphic-media",
                    //"self-harm",
                    //"sensitive",
                    //"extremist",
                    //"intolerant",
                    //"threat",
                    //"rude",
                    //"illicit",
                    ("security", ModerationBehavior.Badge),
                    ("unsafe-link", ModerationBehavior.Badge),
                    ("impersonation", ModerationBehavior.Badge),
                    ("misinformation", ModerationBehavior.Badge),
                    ("scam", ModerationBehavior.Badge),
                    ("engagement-farming", ModerationBehavior.Badge),
                    ("spam", ModerationBehavior.Mute),
                    ("rumor", ModerationBehavior.Badge),
                    ("misleading", ModerationBehavior.Badge),
                    ("inauthentic", ModerationBehavior.Badge),
                }
                .Select(x => new LabelerSubscription() { Behavior = x.Item2, LabelerPlc = bskyModeration.PlcValue, LabelerNameHash = HashLabelName(x.Item1) })
                .Where(x => x.Behavior != ModerationBehavior.None)
                .ToArray();
            }
            finally 
            {
                Lock.ExitUpgradeableReadLock();
            }

            

            GarbageCollectOldSlices(allowTempFileDeletion: true);
        }

        public IEnumerable<PostEngagement> CompactPostEngagements(IEnumerable<PostEngagement> enumerable)
        {
            return enumerable.GroupAssumingOrderedInput(x => x.PostId)
                .Select(x =>
                {
                    if (x.Values.Count == 1) return x.Values[0];
                    PostEngagementKind flags = default;
                    foreach (var item in x.Values)
                    {
                        flags |= item.Kind;
                    }
                    return new PostEngagement(x.Key, flags);
                });
        }

        public static ApproximateDateTime32 GetApproxTime32(Tid tid)
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
                return saturate ? ApproximateDateTime24.MinValue.Value : null;
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

        protected bool IsDisposing;
        public bool IsDisposed => _disposed;

        public StrongBox<(StackTrace StackTrace, int ManagedThreadId)>? LastStackTraceOnWriteLockEnter;

        private readonly static bool WriteStackTracesOnLockEnter = AppViewLiteConfiguration.GetBool(AppViewLiteParameter.APPVIEWLITE_WRITE_STACK_TRACES_ON_LOCK_ENTER) ?? false;
        internal void OnBeforeWriteLockEnter()
        {
            if (WriteStackTracesOnLockEnter)
                LastStackTraceOnWriteLockEnter = new((new StackTrace(true), Environment.CurrentManagedThreadId));

            //var managedThreadId = Environment.CurrentManagedThreadId;
            //var currentThread = Thread.CurrentThread;
            //if (!managedThreadIdToThread.TryGetValue(managedThreadId, out var wr) || !wr.TryGetTarget(out var existing) || existing != currentThread)
            //{
            //    managedThreadIdToThread[managedThreadId] = new WeakReference<Thread>(currentThread);
            //}
        }
        //internal ConcurrentDictionary<int, WeakReference<Thread>> managedThreadIdToThread = new();

        public void Dispose()
        {
            if (Lock == null) return;
            OnBeforeWriteLockEnter();
            Lock.EnterWriteLock();
            try
            {
                PrintMemoryPageAccesses();

                IsDisposing = true;
                if (!_disposed)
                {

                    foreach (var d in disposables)
                    {
                        if (IsReadOnly) d.DisposeNoFlush();
                        else d.Dispose();
                    }
                    if (IsReadOnly)
                    {
                        foreach (var d in AllMultidictionaries)
                        {
                            d.ReturnQueueForNextReplica();
                        }
                    }
                    else
                    {
                        CaptureCheckpoint();
                    }
                    lockFile?.Dispose();
                    _disposed = true;
                }

            }
            finally
            {
                
                Lock.ExitWriteLock();
                // Lock.Dispose(); // non-trivial to synchronize correctly
            }


            lock (recordTypeDurations)
            {
                foreach (var item in recordTypeDurations.OrderByDescending(x => x.Value.TotalTime))
                {
                    Console.Error.WriteLine(item.Value + " " + item.Key + " (" + item.Value.Count + ", avg. " + (long)(item.Value.TotalTime / item.Value.Count).TotalMicroseconds  + ")");
                }
            }
        }

        public void PrintMemoryPageAccesses()
        {
            foreach (var item in AccessedMemoryPagesByTaskType.Select(x => (Key: x.Key, Count: x.Value.Count)).OrderByDescending(x => x.Count))
            {
                Console.Error.WriteLine("Accessed pages for " + item.Key + ":\t\t" + item.Count);
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
            if (AppViewLiteConfiguration.GetBool(AppViewLiteParameter.APPVIEWLITE_DISABLE_SLICE_GC) == true) return;
            var allCheckpoints = new DirectoryInfo(BaseDirectory + "/checkpoints").EnumerateFiles("*.pb").ToArray();

            var checkpointsToKeep =
                allCheckpoints
                .OrderByDescending(x => x.LastWriteTimeUtc)
                .Take(AppViewLiteConfiguration.GetInt32(AppViewLiteParameter.APPVIEWLITE_RECENT_CHECKPOINTS_TO_KEEP) ?? 3)
                .ToArray();

            var keep = checkpointsToKeep
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

            var checkpointsToKeepHashset = checkpointsToKeep.Select(x => x.Name).ToHashSet();
            foreach (var checkpoint in allCheckpoints)
            {
                if (!checkpointsToKeepHashset.Contains(checkpoint.Name))
                    checkpoint.Delete();
            }
        }

        private bool _disposed;

        public string GetDid(Plc plc)
        {
            string did;

            if (PlcToDidConcurrentCache.TryGetValue(plc, out var cached))
            {
                return cached;
            }

            if (PlcToDidPlc.TryGetSingleValue(plc, out var plc128))
            {
                did = DeserializeDidPlcFromUInt128(plc128);
            }
            else if (PlcToDidOther.TryGetPreserveOrderSpanAny(plc, out var r))
            {
                did = Encoding.UTF8.GetString(r.AsSmallSpan());
            }
            else
            {  
                throw new Exception("Missing DID string for Plc(" + plc + ")");

            }
            //if (SerializeDid(did) != plc) CombinedPersistentMultiDictionary.Abort(new Exception("Did serialization did not roundtrip for " + plc + "/" + did));
            PlcToDidConcurrentCache[plc] = did;
            return did;
        }



        public Plc SerializeDid(string did, RequestContext ctx) // TODO: actually very important to pass ctx
        {

            var plc = TrySerializeDidMaybeReadOnly(did, ctx);
            if (plc == default)
                throw ThrowIncorrectLockUsageException("Cannot serialize new did, because a write or upgradable lock is not held.");
            return plc;
        }
        public Plc TrySerializeDidMaybeReadOnly(string did, RequestContext ctx)
        {
            BlueskyEnrichedApis.EnsureValidDid(did);
            var hash = StringUtils.HashUnicodeToUuid(did);


            if (DidToPlcConcurrentCache.TryGetValue(hash, out var cached))
            {
                return cached;
            }


            if (DidHashToUserId.TryGetSingleValue(hash, out var plc))
            {
                DidToPlcConcurrentCache[hash] = plc;
                return plc;
            }

            if (!CanUpgradeToWrite) return default;

            WithWriteUpgrade(() =>
            {
                plc = new Plc(checked(LastAssignedPlc.PlcValue + 1));
                LastAssignedPlc = plc;

                if (did.StartsWith("did:plc:", StringComparison.Ordinal))
                {
                    PlcToDidPlc.Add(plc, SerializeDidPlcToUInt128(did));
                }
                else
                {
                    PlcToDidOther.AddRange(plc, Encoding.UTF8.GetBytes(did));
                }

                DidHashToUserId.Add(hash, plc);
            }, ctx);


            DidToPlcConcurrentCache[hash] = plc;
            return plc;

        }


        public bool CanUpgradeToWrite => Lock.IsWriteLockHeld || (Lock.IsUpgradeableReadLockHeld && ForbidUpgrades == 0);

        public void WithWriteUpgrade(Action value, RequestContext ctx)
        {
            if (ctx == null) BlueskyRelationships.ThrowIncorrectLockUsageException("Missing ctx");
            if (ForbidUpgrades != 0) throw ThrowIncorrectLockUsageException("Cannot upgrade to write lock after the upgradable preamble.");
            if (Lock.IsWriteLockHeld)
            {
                value();
            }
            else
            {
                if (Lock.IsReadLockHeld) throw ThrowIncorrectLockUsageException("Lock should've been entered in upgradable mode, but read mode was used.");
                OnBeforeWriteLockEnter();
                using var _ = new ThreadPriorityScope(ThreadPriority.Normal);
                Lock.EnterWriteLock();
                try
                {
                    Version++;
                    ctx.BumpMinimumVersion(Version);

                    value();
                    BeforeExitingLockUpgrade?.Invoke(this, EventArgs.Empty);
                }
                finally
                {
                    Lock.ExitWriteLock();
                }
            }
        }

        public event EventHandler BeforeExitingLockUpgrade;

        public static PostId GetPostId(Plc Plc, string rkey)
        {
            return new PostId(Plc, Tid.Parse(rkey));
        }
        public PostId GetPostId(StrongRef subject, RequestContext ctx, bool ignoreIfNotPost = false)
        {
            var uri = subject.Uri;
            if (uri.Collection != Post.RecordType)
            {
                if (ignoreIfNotPost) return default;
                throw new UnexpectedFirehoseDataException("Unexpected URI type: " + uri.Collection);
            }
            return new PostId(SerializeDid(uri.Did!.Handler, ctx), Tid.Parse(uri.Rkey));
        }
        public static PostIdString GetPostIdStr(StrongRef uri)
        {
            return GetPostIdStr(uri.Uri);
        }
        public static PostIdString GetPostIdStr(ATUri uri)
        {
            if (uri.Collection != Post.RecordType)
            {
                throw new ArgumentException("Unexpected URI type: " + uri.Collection);
            }
            return new PostIdString(uri.Did!.Handler, uri.Rkey);
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
                return proto;

            }
            else if (FailedProfileLookups.ContainsKey(plc))
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
        public BlueskyPostData? StorePostInfoExceptData(Post p, PostId postId, RequestContext ctx)
        {
            if (postId == default) throw new Exception();
            if (PostData.ContainsKey(postId)) return null;
            var proto = new BlueskyPostData
            {
                Text = string.IsNullOrEmpty(p.Text) ? null : p.Text,
                PostId = postId,

                // We will change them later if necessary.
                RootPostPlc = postId.Author.PlcValue,
                RootPostRKey = postId.PostRKey.TidValue,
            };
            var lang = p.Langs?.FirstOrDefault();

            var langEnum = ParseLanguage(lang);

            proto.Language = langEnum;

            proto.Facets = GetFacetsAsProtos(p.Facets);

            IndexPost(proto);

            if (p.Facets != null)
            {
                foreach (var facet in p.Facets)
                {
                    foreach (var feature in facet.Features!.OfType<Tag>())
                    {
                        var tag = feature.TagValue;
                        if (!string.IsNullOrEmpty(tag))
                        {
                            AddToSearchIndex("#" + tag.ToLowerInvariant(), GetApproxTime32(proto.PostId.PostRKey));
                        }
                    }
                }
            }

            if (p.Reply?.Root is { } root)
            {
                var rootPost = this.GetPostId(root, ctx);
                proto.RootPostPlc = rootPost.Author.PlcValue;
                proto.RootPostRKey = rootPost.PostRKey.TidValue;
                AddNotification(rootPost.Author, NotificationKind.RepliedToYourThread, postId, ctx);
            }
            if (p.Reply?.Parent is { } parent)
            {
                var inReplyTo = this.GetPostId(parent, ctx);
                proto.InReplyToPlc = inReplyTo.Author.PlcValue;
                proto.InReplyToRKey = inReplyTo.PostRKey.TidValue;
                this.DirectReplies.Add(inReplyTo, postId);
                AddNotification(inReplyTo.Author, NotificationKind.RepliedToYourPost, postId, ctx);
            }


            var embed = p.Embed;
            if (embed is EmbedRecord { } er)
            {
                var quoted = this.GetPostId(er.Record!, ctx, ignoreIfNotPost: true);
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
                var quoted = this.GetPostId(rm.Record!.Record!, ctx, ignoreIfNotPost: true);
                if (quoted != default)
                {
                    proto.QuotedPlc = quoted.Author.PlcValue;
                    proto.QuotedRKey = quoted.PostRKey.TidValue;

                    embed = rm.Media;
                    this.Quotes.Add(quoted, postId);
                    AddNotification(quoted.Author, NotificationKind.QuotedYourPost, postId, ctx);
                }
            }


            if (embed is EmbedImages { } ei)
            {
                if (ei.Images.Any(x => x.ImageValue?.Ref?.Link == null)) throw new UnexpectedFirehoseDataException("Missing CID in EmbedImages");
                proto.Media = ei.Images!.Select(x => new BlueskyMediaData { AltText = string.IsNullOrEmpty(x.Alt) ? null : x.Alt, Cid = x.ImageValue.Ref!.Link!.ToArray() }).ToArray();
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

            if (proto.Facets != null)
            {
                foreach (var facet in proto.Facets)
                {
                    if (facet.Did != null)
                    {
                        var plc = SerializeDid(facet.Did, ctx);
                        AddNotification(plc, NotificationKind.MentionedYou, postId, ctx);
                    }
                }
            }

            if (proto.Media != null)
                UserToRecentMediaPosts.Add(postId.Author, postId.PostRKey);
            UserToRecentPosts.Add(postId.Author, new RecentPost(postId.PostRKey, proto.IsReplyToUnspecifiedPost == true ? Plc.MaxValue : new Plc(proto.InReplyToPlc.GetValueOrDefault())));

            if (proto.QuotedRKey != null)
                NotifyPostStatsChange(proto.QuotedPostId!.Value, postId.Author);
            return proto;
        }

        public void IndexPost(BlueskyPostData proto)
        {
            var postId = proto.PostId;
            if (postId.PostRKey.Date < ApproximateDateTime32.MinValueAsDateTime) return;
            var approxPostDate = GetApproxTime32(postId.PostRKey);

            if (proto.Language != LanguageEnum.Unknown)
                AddToSearchIndex("%lang-" + proto.Language!.Value.ToString(), approxPostDate);
            PostTextSearch.Add(HashPlcForTextSearch(postId.Author), approxPostDate);

            if (proto.Text != null)
            {
                var words = StringUtils.GetDistinctWords(proto.Text);
                foreach (var word in words)
                {
                    AddToSearchIndex(word, approxPostDate);
                }
            }

            if (proto.Media != null)
            {
                foreach (var media in proto.Media)
                {
                    var words = StringUtils.GetDistinctWords(media.AltText);
                    foreach (var word in words)
                    {
                        AddToSearchIndex(word, approxPostDate);
                    }
                }
            }
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
            }).WhereNonNull().ToArray();
            if (facetProtos.Length == 0) return null;
            return facetProtos!;
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

        public void AddToSearchIndex(ReadOnlySpan<char> word, ApproximateDateTime32 approxPostDate)
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



        public List<ManagedOrNativeArray<Plc>> SearchProfilesPrefixOnly(SizeLimitedWord8 prefix)
        {
            if (prefix.Length <= 2)
            {
                var prefix2 = SizeLimitedWord2.Create(prefix);
                var maxExclusive = prefix2.GetMaxExclusiveForPrefixRange();
                return ConsolidatePrefixSearch(ProfileSearchPrefix2.GetInRangeUnsorted(prefix2, maxExclusive).Select(x => x.Values));
            }
            else
            {
                var maxExclusive = prefix.GetMaxExclusiveForPrefixRange();
                return ConsolidatePrefixSearch(ProfileSearchPrefix8.GetInRangeUnsorted(prefix, maxExclusive).Select(x => x.Values));
            }
            
        }

        private static List<ManagedOrNativeArray<Plc>> ConsolidatePrefixSearch(IEnumerable<ManagedOrNativeArray<Plc>> slices)
        {
            var result = new List<ManagedOrNativeArray<Plc>>();
            var small = new List<Plc>();
            foreach (var slice in slices)
            {
                if (slice.Count <= 5)
                    small.AddRange(slice);
                else
                    result.Add(slice);
            }

            if (small.Count != 0)
            {
                small.Sort();
                result.Add(small.ToArray());
            }

            return result;
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
                var index = slice.AsSpan().BinarySearchRightBiased(maxPost);
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

        internal void StoreProfileBasicInfo(Plc plc, Profile pf, RequestContext ctx)
        {
            //PlcToBasicInfo.AddRange(plc, [.. Encoding.UTF8.GetBytes(pf.DisplayName ?? string.Empty), 0, .. (pf.Avatar?.Ref?.Link?.ToArray() ?? [])]);

            var pinnedPost = pf.PinnedPost?.Uri;
            if (pinnedPost != null && SerializeDid(pinnedPost.Did!.Handler, ctx) != plc)
                pinnedPost = null;

            var proto = new BlueskyProfileBasicInfo
            {
                Description = pf.Description,
                DisplayName = pf.DisplayName,
                AvatarCidBytes = pf.Avatar?.Ref?.Link?.ToArray(),
                BannerCidBytes = pf.Banner?.Ref?.Link?.ToArray(),
                PinnedPostTid = pinnedPost != null ? Tid.Parse(pinnedPost.Rkey).TidValue : null,
            };

            IndexProfile(plc, proto);
            StoreProfileBasicInfo(plc, proto);
        }

        public void StoreProfileBasicInfo(Plc plc, BlueskyProfileBasicInfo proto)
        {
            lock (textCompressorUnlocked)
            {
                textCompressorUnlocked.CompressInPlace(ref proto.Description, ref proto.DescriptionBpe);
                textCompressorUnlocked.CompressInPlace(ref proto.DisplayName, ref proto.DisplayNameBpe);
            }



            Profiles.AddRange(plc, SerializeProto(proto, x => x.Dummy = true));
        }

        internal void IndexProfile(Plc plc, BlueskyProfileBasicInfo proto)
        {
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
        }

        internal void IndexProfileWord(string word, Plc plc)
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

        internal void StorePostInfo(PostId postId, Post p, string did, RequestContext ctx)
        {
            // PERF: This method performs the slow brotli compression while holding the lock. Avoid if possible.
            var proto = StorePostInfoExceptData(p, postId, ctx);
            if (proto != null)
                this.PostData.AddRange(postId, SerializePostData(proto, did));
        }

        internal static byte[] SerializePostData(BlueskyPostData proto, string did)
        {
            // var (a, b, c) = (proto.PostId, proto.InReplyToPostId, proto.RootPostId);
            Compress(proto, did);

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

        public static AppViewLite.PluggableProtocols.PluggableProtocol? TryGetPluggableProtocolForDid(string did) => AppViewLite.PluggableProtocols.PluggableProtocol.TryGetPluggableProtocolForDid(did);

        internal static void Compress(BlueskyPostData proto, string did)
        {

            if (proto.PluggablePostId != null)
            {
                var pluggable = TryGetPluggableProtocolForDid(did)!;

                if (!pluggable.RequiresExplicitPostIdStorage(proto.PluggablePostId.Value))
                {
                    proto.PluggablePostId = null;
                }
                if (!pluggable.RequiresExplicitPostIdStorage(proto.PluggableInReplyToPostId))
                {
                    proto.PluggableInReplyToPostId = null;
                }
                if (proto.RootPostId == proto.InReplyToPostId ||
                    proto.RootPostId == proto.PostId ||
                    !pluggable.RequiresExplicitPostIdStorage(proto.PluggableRootPostId))
                {
                    proto.PluggableRootPostId = null;
                }
            }

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
                        textCompressorUnlocked.CompressInPlace(ref facet.InlineImageUrl, ref facet.InlineImageUrlBpe);
                        textCompressorUnlocked.CompressInPlace(ref facet.InlineImageAlt, ref facet.InlineImageAltBpe);
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
        private static void Decompress(BlueskyPostData proto, PostId postId, bool skipBpeDecompression = false)
        {

            if (!skipBpeDecompression)
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
                            textCompressorUnlocked.DecompressInPlace(ref facet.InlineImageUrl, ref facet.InlineImageUrlBpe);
                            textCompressorUnlocked.DecompressInPlace(ref facet.InlineImageAlt, ref facet.InlineImageAltBpe);
                        }
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


            //var pluggable = PluggableProtocol.TryGetPluggableProtocolForDid();

        }


        public static void VerifyNotEnumerable<T>()
        {
            if (typeof(T).IsGenericType && typeof(T).GetGenericTypeDefinition() == typeof(IEnumerable<>))
                throw new InvalidOperationException("Enumeration runs outside the lock.");
        }


        public BlueskyProfile[] GetPostLikers(Plc plc, string rkey, Relationship continuation, int limit)
        {
            return Likes.GetRelationshipsSorted(GetPostId(plc, rkey), continuation).Take(limit).Select(x => GetProfile(x.Actor, x.RelationshipRKey)).ToArray();
        }
        public BlueskyProfile[] GetPostReposts(Plc plc, string rkey, Relationship continuation, int limit)
        {
            return Reposts.GetRelationshipsSorted(GetPostId(plc, rkey), continuation).Take(limit).Select(x => GetProfile(x.Actor, x.RelationshipRKey)).ToArray();
        }
        public BlueskyPost[] GetPostQuotes(Plc plc, string rkey, PostId continuation, int limit)
        {
            return Quotes.GetValuesSorted(GetPostId(plc, rkey), continuation).Take(limit).Select(GetPost).ToArray();
        }

        public BlueskyPost GetPost(string did, string rkey, RequestContext ctx)
        {
            return GetPost(GetPostId(did, rkey, ctx));
        }
        public BlueskyPost GetPost(Plc plc, Tid rkey)
        {
            return GetPost(new PostId(plc, rkey));
        }
        public BlueskyPost GetPost(ATUri uri, RequestContext ctx)
        {
            return GetPost(GetPostId(uri, ctx));
        }

        public BlueskyPost GetPost(PostId id)
        {
            var post = GetPostWithoutData(id);
            (post.Data, post.InReplyToUser) = TryGetPostDataAndInReplyTo(id);
            if (post.Data != null)
            {
                if (post.Data.PluggableReplyCount != null)
                    post.ReplyCount = post.Data.PluggableReplyCount.Value;
                if (post.Data.PluggableLikeCount != null)
                    post.LikeCount = post.Data.PluggableLikeCount.Value;
            }
            DecompressPluggablePostData(id.PostRKey, post.Data, post.Author.Did);
            if (post.PluggableProtocol != null && post.Data == null)
            {
                post.Data = new BlueskyPostData
                {
                    Error = "This post could be found."
                };
            }
            MaybePropagateAdministrativeBlockToPost(post);
            return post;
        }

        private static void MaybePropagateAdministrativeBlockToPost(BlueskyPost post)
        {
            if (post.Author.IsBlockedByAdministrativeRule)
            {
                post.Data = new BlueskyPostData { Error = post.Author.BasicData!.Error };
            }
            if (post.Author.IsMediaBlockedByAdministrativeRule && post.Data != null)
            {
                RemoveCustomEmojiFacets(ref post.Data.Facets);
            }
        }

        public BlueskyPost GetPost(PostId id, BlueskyPostData? data)
        {
            var post = GetPostWithoutData(id);
            post.Data = data;
            DecompressPluggablePostData(id.PostRKey, data, post.Author.Did);
            MaybePropagateAdministrativeBlockToPost(post);
            return post;
        }
        

        private static void DecompressPluggablePostData(Tid tid, BlueskyPostData? data, string did)
        {
            if (data == null) return;
            var pluggable = TryGetPluggableProtocolForDid(did);
            if (pluggable == null) return;


            pluggable.DecompressPluggablePostId(ref data.PluggablePostId, tid, null);
            if (data.InReplyToRKey != null)
            {
                pluggable.DecompressPluggablePostId(ref data.PluggableInReplyToPostId, new Tid(data.InReplyToRKey.Value), null);
            }

            pluggable.DecompressPluggablePostId(ref data.PluggableRootPostId, data.RootPostId.PostRKey, data.PluggableInReplyToPostId ?? data.PluggablePostId);
            
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

        public BlueskyPostData? TryGetPostData(PostId id, bool skipBpeDecompression = false)
        {
            var isDeleted = PostDeletions.ContainsKey(id);
            
            BlueskyPostData? proto = null;
            ManagedOrNativeArray<byte> postDataCompressed;
            
            // latest instead of any (pluggable posts include their own like count)
            if (PostData.TryGetPreserveOrderSpanLatest(id, out postDataCompressed))
            {
                proto = DeserializePostData(postDataCompressed.AsSmallSpan(), id, skipBpeDecompression: skipBpeDecompression);
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

        internal static BlueskyPostData DeserializePostData(ReadOnlySpan<byte> postDataCompressed, PostId postId, bool skipBpeDecompression = false)
        {
            var encoding = (PostDataEncoding)postDataCompressed[0];
            postDataCompressed = postDataCompressed.Slice(1);

            using var ms = new MemoryStream(postDataCompressed.Length);
            ms.Write(postDataCompressed);
            ms.Seek(0, SeekOrigin.Begin);

            if (encoding == PostDataEncoding.Proto)
            {
                var proto = ProtoBuf.Serializer.Deserialize<BlueskyPostData>(ms);
                Decompress(proto, postId, skipBpeDecompression: skipBpeDecompression);
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

                Decompress(proto, postId, skipBpeDecompression: skipBpeDecompression);
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

            var pluggable = TryGetPluggableProtocolForDid(did);

            if (basic == null && pluggable != null && !pluggable.SupportsProfileMetadataLookup(did))
                basic = new BlueskyProfileBasicInfo();

            var didDoc = TryGetLatestDidDoc(plc);



            var isBlockedByAdministrativeRule = AdministrativeBlocklist.Instance.GetValue().ShouldBlockDisplay(did, didDoc);
            var isMediaBlockedByAdministrativeRule = AdministrativeBlocklist.Instance.GetValue().ShouldBlockOutboundConnection(did, didDoc);

            var possibleHandle = didDoc?.Handle;
            bool handleIsCertain = false;
            if (possibleHandle != null &&
                HandleToDidVerifications.TryGetLatestValue(StringUtils.HashUnicodeToUuid(StringUtils.NormalizeHandle(possibleHandle)), out var lastVerification) &&
                (DateTime.UtcNow - lastVerification.VerificationDate) < BlueskyEnrichedApis.HandleToDidMaxStale)
            {
                if (lastVerification.Plc == plc) handleIsCertain = true;
                else possibleHandle = null;
            }
            if (possibleHandle == null && didDoc != null)
                handleIsCertain = true;

            if (pluggable != null)
            {
                if (possibleHandle == null)
                {
                    possibleHandle = pluggable.TryGetHandleFromDid(did) ?? did;
                    handleIsCertain = true;
                }
            }

            possibleHandle = MaybeBridgyHandleToFediHandle(possibleHandle);

            if (isBlockedByAdministrativeRule)
            {
                basic = new BlueskyProfileBasicInfo
                {
                    Error = "This DID or domain is blocked by administrative rules."
                };
            }

            if (isMediaBlockedByAdministrativeRule && basic != null)
            {
                RemoveCustomEmojiFacets(ref basic.DisplayNameFacets);
                RemoveCustomEmojiFacets(ref basic.DescriptionFacets);
                basic.AvatarCidBytes = null;
                basic.BannerCidBytes = null;
            }

            return new BlueskyProfile()
            {
                PlcId = plc.PlcValue,
                Did = did,
                BasicData = basic,
                DidDoc = didDoc,
                RelationshipRKey = relationshipRKey,
                PossibleHandle = possibleHandle,
                Pds = didDoc?.Pds,
                HandleIsUncertain = !handleIsCertain,
                IsBlockedByAdministrativeRule = isBlockedByAdministrativeRule,
                IsMediaBlockedByAdministrativeRule = isMediaBlockedByAdministrativeRule,
                Badges = Badges.GetBadges(plc, did, possibleHandle),
                PluggableProtocol = pluggable,
            };
        }

        public static string? MaybeBridgyHandleToFediHandle(string? handle)
        {
            if (handle != null && handle.EndsWith(".ap.brid.gy", StringComparison.Ordinal))
            {
                var h = handle.Substring(0, handle.Length - ".ap.brid.gy".Length);
                var dot = h.IndexOf('.');
                if (dot != -1)
                {
                    return string.Concat(h.AsSpan(0, dot), "@", h.AsSpan(dot + 1));
                }
            }
            return handle;
        }

        private static void RemoveCustomEmojiFacets(ref FacetData[]? facets)
        {
            if (facets == null || facets.Length == 0) return;
            facets = facets.Where(x => x.CustomEmojiHash == null).ToArray();
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

        public BlockReason GetBlockReason(Plc plc, RequestContext ctx)
        {
            return ctx.IsLoggedIn == true ? UsersHaveBlockRelationship(ctx.LoggedInUser, plc) : default;
        }

        public PostId GetPostId(string did, string rkey, RequestContext ctx)
        {
            return new PostId(SerializeDid(did, ctx), Tid.Parse(rkey));
        }
        public PostId GetPostId(ATUri uri, RequestContext ctx)
        {
            if (uri.Collection != Post.RecordType) throw new ArgumentException();
            return GetPostId(uri.Did!.ToString(), uri.Rkey, ctx);
        }

        public BlueskyProfile[] GetFollowers(Plc plc, Relationship continuation, int limit)
        {
            return Follows.GetRelationshipsSorted(plc, continuation).Take(limit).Select(x => GetProfile(x.Actor, x.RelationshipRKey)).ToArray();
        }

        public BlueskyFullProfile GetFullProfile(Plc plc, RequestContext ctx, int followersYouFollowToLoad)
        {
            return new BlueskyFullProfile
            {
                Profile = GetProfile(plc),
                Followers = Follows.GetActorCount(plc),
                FollowedByPeopleYouFollow = ctx.IsLoggedIn ? GetFollowersYouFollow(plc, ctx.LoggedInUser)?.Select((x, i) => i < followersYouFollowToLoad ? GetProfile(x) : new BlueskyProfile { PlcId = x.PlcValue, Did = null! } ).ToList() : null,
                HasFeeds = FeedGenerators.GetInRangeUnsorted(new RelationshipHashedRKey(plc, 0), new RelationshipHashedRKey(plc.GetNext(), 0)).Any(),
                HasLists = Lists.GetInRangeUnsorted(new Relationship(plc, default), new Relationship(plc.GetNext(), default)).Any(),
            };
        }

        public ProfilesAndContinuation GetFollowersYouFollow(string did, string? continuation, int limit, RequestContext ctx)
        {
            if (!ctx.IsLoggedIn) return new ProfilesAndContinuation();
            var offset = continuation != null ? int.Parse(continuation) : 0;
            var plc = SerializeDid(did, ctx);
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
                .Where(x => x.Left != default && x.Right != default && !Follows.IsDeleted(new Relationship(loggedInUser, x.Left.ListItemRKey)))
                .Select(x => x.Key)
                .ToArray();
        }

        internal void EnsureNotDisposed()
        {
            if (_disposed)
            {
                ShutdownRequested.ThrowIfCancellationRequested();
                throw new ObjectDisposedException(nameof(BlueskyRelationships));
            }
        }


        public CancellationTokenSource ShutdownRequestedCts = new CancellationTokenSource();
        public CancellationToken ShutdownRequested => ShutdownRequestedCts.Token;
        public void NotifyShutdownRequested()
        {
            ShutdownRequestedCts.Cancel();
            Dispose();
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



        internal ReadOnlySpan<byte> SerializeThreadgateToBytes(Threadgate threadGate, RequestContext ctx)
        {
            var proto = new BlueskyThreadgate
            {
                HiddenReplies = threadGate.HiddenReplies?.Select(x => RelationshipProto.FromPostId(GetPostId(x, ctx))).ToArray(),
                AllowlistedOnly = threadGate.Allow != null,
                AllowFollowing = threadGate.Allow?.Any(x => x is FollowingRule) ?? false,
                AllowFollowers = threadGate.Allow?.Any(x => x is FollowerRule) ?? false,
                AllowMentioned = threadGate.Allow?.Any(x => x is MentionRule) ?? false,
                AllowLists = threadGate.Allow?.OfType<ListRule>().Select(x => {
                    return new RelationshipProto { Plc = SerializeDid(x.List.Did!.Handler, ctx).PlcValue, Tid = Tid.Parse(x.List.Rkey).TidValue };
                }).ToArray()
            };
            return SerializeProto(proto, x => x.Dummy = true);
        }
        internal ReadOnlySpan<byte> SerializePostgateToBytes(Postgate postgate, RequestContext ctx)
        {
            var proto = new BlueskyPostgate
            {
                 DetachedEmbeddings = postgate.DetachedEmbeddingUris?.Select(x => RelationshipProto.FromPostId(GetPostId(x, ctx))).ToArray(),
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
        public void AddNotification(PostId destination, NotificationKind kind, Plc actor, RequestContext ctx)
        {
            AddNotification(destination.Author, kind, actor, destination.PostRKey, ctx);
        }
        public void AddNotification(Plc destination, NotificationKind kind, Plc actor, RequestContext ctx)
        {
            AddNotification(destination, kind, actor, default, ctx);
        }
        public void AddNotification(Plc destination, NotificationKind kind, PostId replyId, RequestContext ctx)
        {
            AddNotification(destination, kind, replyId.Author, replyId.PostRKey, ctx);
        }
        public void AddNotification(Plc destination, NotificationKind kind, Plc actor, Tid rkey, RequestContext ctx)
        {
            if (SuppressNotificationGeneration != 0) return;
            if (destination == actor) return;
            if (!IsRegisteredForNotifications(destination)) return;
            if (UsersHaveBlockRelationship(destination, actor) != default) return;
            var notification = new Notification((ApproximateDateTime32)DateTime.UtcNow, actor, rkey, kind);
            Notifications.Add(destination, notification);
            UserNotificationSubscribersThreadSafe.MaybeFetchDataAndNotifyOutsideLock(destination, () => GetNotificationCount(destination), (data, handler) => handler(data));

            // Callback must stay inside the lock.
            NotificationGenerated?.Invoke(destination, notification, ctx);
        }

        public int SuppressNotificationGeneration;

        public event Action<Plc, Notification, RequestContext>? NotificationGenerated;



        public BlueskyNotification? RehydrateNotification(Notification notification, Plc destination, RequestContext ctx)
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
                feed = TryGetFeedGenerator(new RelationshipHashedRKey(destination, (ulong)notification.RKey.TidValue), ctx);
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

        public (BlueskyNotification[] NewNotifications, BlueskyNotification[] OldNotifications, Notification NewestNotification) GetNotificationsForUser(Plc user, RequestContext ctx)
        {
            if (!LastSeenNotifications.TryGetLatestValue(user, out var threshold)) return ([], [], default);
            var newNotificationsCore = Notifications.GetValuesSortedDescending(user, threshold, null).ToArray();

            Notification? newestNotification = newNotificationsCore.Length != 0 ? newNotificationsCore[0] : null;

            var newNotifications = 
                newNotificationsCore
                .Select(x => RehydrateNotification(x, user, ctx))
                .WhereNonNull()
                .ToArray();

            Notification? oldestNew = newNotificationsCore.Length != 0 ? newNotificationsCore[^1] : null;

            var distinctOldCoalesceKeys = new HashSet<NotificationCoalesceKey>();

            var oldNotifications =
                Notifications.GetValuesSortedDescending(user, null, oldestNew)
                .Select(x =>
                {
                    newestNotification ??= x;
                    return RehydrateNotification(x, user, ctx);
                })
                .WhereNonNull()
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
        internal IEnumerable<PostId> EnumerateRecentMediaPosts(Plc author, Tid minDate, Tid? maxDate)
        {
            return this.UserToRecentMediaPosts.GetValuesSortedDescending(author, minDate, maxDate).Select(x => new PostId(author, x));
        }
        internal IEnumerable<RecentRepost> EnumerateRecentReposts(Plc author, Tid minDate, Tid? maxDate)
        {
            return this.UserToRecentReposts.GetValuesSortedDescending(author, new RecentRepost(minDate, default), maxDate != null ? new RecentRepost(maxDate.Value, default) : null);
        }

        public IEnumerable<BlueskyPost> EnumerateFollowingFeed(RequestContext ctx, DateTime minDate, Tid? maxTid)
        {
            var loggedInUser = ctx.LoggedInUser;
            var thresholdDate = minDate != default ? Tid.FromDateTime(minDate) : default;


            var follows = GetFollowingFast(ctx);
            var plcToLatestFollowRkey = new Dictionary<Plc, Tid>();

            var requireFollowStillValid = new Dictionary<BlueskyPost, (Plc A, Plc B, Plc C)>();

            var usersRecentPosts =
                follows.PossibleFollows
                .Select(pair =>
                {
                    var author = pair.Plc;
                    return this
                        .EnumerateRecentPosts(author, thresholdDate, maxTid)
                        .Select(x =>
                        {
                            var postAuthor = author;
                            var parentAuthor = x.InReplyTo;

                            if (!follows.IsPossiblyStillFollowed(author))
                                return null;

                            bool ShouldConsiderPostByAuthor(Plc mustFollow)
                            {
                                if (mustFollow == loggedInUser) return true;

                                if (!follows.IsPossiblyStillFollowed(mustFollow))
                                    return false;
                                return true;
                            }

                            if (parentAuthor != default && parentAuthor != postAuthor && !ShouldConsiderPostByAuthor(parentAuthor))
                            {
                                return null;
                            }

                            var post = GetPost(x.PostId);
                            if (post.Data == null) return null;
                            if (post.Data.IsReplyToUnspecifiedPost == true) return null;

                            var rootAuthor = post.RootPostId.Author;
                            if (rootAuthor != postAuthor && rootAuthor != parentAuthor && !ShouldConsiderPostByAuthor(rootAuthor))
                            {
                                return null;
                            }

                            requireFollowStillValid[post] = (author, parentAuthor, rootAuthor);
                            return post;
                        })
                        .WhereNonNull();
                });
            var usersRecentReposts =
                follows.PossibleFollows
                .Select(pair =>
                {
                    var reposter = pair.Plc;
                    BlueskyProfile? reposterProfile = null;
                    return this
                        .EnumerateRecentReposts(reposter, thresholdDate, maxTid)
                        .Select(x =>
                        {
                            var post = GetPost(x.PostId);
                            post.RepostedBy = (reposterProfile ??= GetProfile(reposter));
                            post.RepostDate = x.RepostRKey.Date;
                            requireFollowStillValid[post] = (reposter, default, default);
                            return post;
                        });
                });
            var result = 
                SimpleJoin.ConcatPresortedEnumerablesKeepOrdered(usersRecentPosts.Concat(usersRecentReposts).ToArray(), x => x.RepostDate != null ? Tid.FromDateTime(x.RepostDate.Value) : x.PostId.PostRKey, new ReverseComparer<Tid>())
                .Where(x =>
                {
                    var triplet = requireFollowStillValid[x];
                    
                    if (!(follows.IsStillFollowed(triplet.A, this) && follows.IsStillFollowed(triplet.B, this) && follows.IsStillFollowed(triplet.C, this)))
                        return false;

                    return ShouldIncludeLeafPostInFollowingFeed(x, ctx) ?? true;
                })!;
            return result;
        }

        public void PopulateViewerFlags(BlueskyPost post, RequestContext ctx)
        {
            if (post.DidPopulateViewerFlags) return;

            PopulateViewerFlags(post.Author, ctx);
            if (post.RepostedBy != null)
                PopulateViewerFlags(post.RepostedBy, ctx);
            if (post.QuotedPost != null)
                PopulateViewerFlags(post.QuotedPost, ctx);
            post.Labels = GetPostLabels(post.PostId, ctx.NeedsLabels).Select(x => GetLabel(x)).ToArray();
            post.IsMuted = post.ShouldMuteCore(ctx);
            post.DidPopulateViewerFlags = true;
        }

        internal IEnumerable<BlueskyPost> EnumerateFeedWithNormalization(IEnumerable<BlueskyPost> posts, RequestContext ctx, HashSet<PostId>? alreadyReturned = null, bool onlyIfRequiresFullReplyChain = false, bool omitIfMuted = false)
        {
            alreadyReturned ??= [];
            foreach (var post in posts)
            {
                var postId = post.PostId;
                if (!alreadyReturned.Add(postId)) continue;
                if (post.Data?.Deleted == true) continue;

                if (post.InReplyToPostId != null && post.PluggableProtocol?.ShouldIncludeFullReplyChain(post) == true)
                {
                    var chain = MakeFullReplyChainExcludingLeaf(post);
                    if (omitIfMuted) 
                    {
                        foreach (var item in chain)
                        {
                            PopulateViewerFlags(item, ctx);
                        }

                        if (chain.Any(x => x.IsMuted)) continue;
                    }
                    foreach (var item in chain)
                    {
                        alreadyReturned.Add(item.PostId);
                        yield return item;
                    }
                }
                else
                {
                    if (!post.IsRepost && post.InReplyToPostId is { } parentId && !onlyIfRequiresFullReplyChain)
                    {
                        if (!alreadyReturned.Contains(parentId))
                        {
                            BlueskyPost? rootPost = null;
                            var rootId = post.RootPostId;
                            if (rootId != postId && rootId != parentId)
                            {
                                if (!alreadyReturned.Contains(rootId))
                                {
                                    
                                    rootPost = GetPost(rootId);
                                    if (omitIfMuted)
                                    {
                                        PopulateViewerFlags(rootPost, ctx);
                                        if (rootPost.IsMuted) continue;
                                    }
                                }
                            }

                            var parentPost = GetPost(parentId);
                            if (omitIfMuted)
                            {
                                PopulateViewerFlags(parentPost, ctx);
                                if (parentPost.IsMuted) continue;
                            }

                            if (rootPost != null)
                            {
                                alreadyReturned.Add(rootPost.PostId);
                                yield return rootPost;
                            }

                            alreadyReturned.Add(parentPost.PostId);
                            yield return parentPost;
                        }
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

        public void SaveAppViewLiteProfile(AppViewLiteUserContext userContext)
        {
            AppViewLiteProfiles.AddRange(userContext.LoggedInUser!.Value, SerializeProto(userContext.PrivateProfile));
        }


        private readonly int GlobalPeriodicFlushSeconds = AppViewLiteConfiguration.GetInt32(AppViewLiteParameter.APPVIEWLITE_GLOBAL_PERIODIC_FLUSH_SECONDS) ?? 600;
        internal void MaybeGlobalFlush()
        {
            if (lastGlobalFlush.Elapsed.TotalSeconds > GlobalPeriodicFlushSeconds)
            {
                if (IsReadOnly) return;
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
            var version = this.Version;
            PostLiveSubscribersThreadSafe.MaybeFetchDataAndNotifyOutsideLock(postId,
                () => new PostStatsNotification(postId, GetDid(postId.Author), postId.PostRKey.ToString()!, Likes.GetActorCount(postId), Reposts.GetActorCount(postId), Quotes.GetValueCount(postId), DirectReplies.GetValueCount(postId)),
                (data, handler) => handler(new Versioned<PostStatsNotification>(data, version), commitPlc));
        }

        //private Dictionary<PostId, int> notifDebug = new();



        private Dictionary<string, (TimeSpan TotalTime, long Count)> recordTypeDurations = new();


        public BlueskyList GetList(Relationship listId, ListData? listData = null)
        {
            var did = GetDid(listId.Actor);
            return new BlueskyList
            {
                ModeratorDid = did,
                ListId = listId,
                Data = listData ?? TryGetListData(listId),
                ListIdStr = new RelationshipStr(did, listId.RelationshipRKey.ToString()!),
                Moderator = GetProfile(listId.Actor)
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


        public void IndexFeedGenerator(Plc plc, string rkey, Generator generator, DateTime retrievalDate)
        {
            var key = new RelationshipHashedRKey(plc, rkey);

            var proto = new BlueskyFeedGeneratorData
            {
                DisplayName = generator.DisplayName,
                AvatarCid = generator.Avatar?.Ref?.Link?.ToArray(),
                Description = generator.Description,
                DescriptionFacets = GetFacetsAsProtos(generator.DescriptionFacets),
                RetrievalDate = retrievalDate,
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

        public BlueskyFeedGenerator GetFeedGenerator(Plc plc, BlueskyFeedGeneratorData data, RequestContext ctx)
        {
            return GetFeedGenerator(plc, data.RKey, ctx, data);
        }
        public BlueskyFeedGenerator GetFeedGenerator(Plc plc, string rkey, RequestContext ctx, BlueskyFeedGeneratorData? data = null)
        {
            data ??= TryGetFeedGeneratorData(new RelationshipHashedRKey(plc, rkey));
            return new BlueskyFeedGenerator
            {
                 Data = data,
                 Did = GetDid(plc),
                 RKey = rkey,
                 Author = GetProfile(plc),
                 LikeCount = FeedGeneratorLikes.GetActorCount(new(plc, rkey)),
                 IsPinned = ctx.IsLoggedIn && ctx.PrivateProfile.FeedSubscriptions.Any(x => new Plc(x.FeedPlc) == plc && x.FeedRKey == rkey),
            };
        }

        public BlueskyFeedGenerator? TryGetFeedGenerator(RelationshipHashedRKey feedId, RequestContext ctx)
        {
            var data = TryGetFeedGeneratorData(feedId);
            if (data == null) return null;
            return GetFeedGenerator(feedId.Plc, data, ctx);
        }

        internal object? TryGetAtObject(string? aturi, RequestContext ctx)
        {
            if (aturi == null) return null;
            var parsed = new ATUri(aturi);

            var plc = SerializeDid(parsed.Did!.Handler, ctx);
            if (parsed.Collection == Generator.RecordType)
            {
                return GetFeedGenerator(plc, parsed.Rkey, ctx);
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
                // rootPostId.Author must follow replyAuthor
                if (Follows.HasActor(replyAuthor, rootPostId.Author, out _))
                    return true;
            }
            if (threadgate.AllowFollowers)
            {
                // replyAuthor must follow rootPostId.Author
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

        internal void IndexHandleCore(string? handle, Plc plc)
        {
            if (handle == null) return;

            HandleToPossibleDids.Add(BlueskyRelationships.HashWord(StringUtils.NormalizeHandle(handle)), plc);

            foreach (var word in StringUtils.GetDistinctWords(handle))
            {
                IndexProfileWord(word, plc);
            }
        }

        internal void IndexHandle(string? handle, string did, RequestContext ctx)
        {
            var plc = SerializeDid(did, ctx);
            IndexHandleCore(handle, plc);

            if (did.StartsWith("did:web:", StringComparison.Ordinal)) 
            {
                var domain = did.Substring(8);
                if (domain != handle)
                    IndexHandleCore(domain, plc);
            }
        }

        internal void AddHandleToDidVerification(string handle, Plc plc)
        {
            if (string.IsNullOrEmpty(handle) || handle.Contains(':')) throw new ArgumentException();
            HandleToDidVerifications.Add(StringUtils.HashUnicodeToUuid(StringUtils.NormalizeHandle(handle)), new HandleVerificationResult((ApproximateDateTime32)DateTime.UtcNow, plc));
        }

        internal static string DeserializeDidPlcFromUInt128(UInt128 plcAsUInt128)
        {
            return "did:plc:" + AtProtoS32.EncodePadded(plcAsUInt128);
        }

        internal static UInt128 SerializeDidPlcToUInt128(string did)
        {
            if (!did.StartsWith("did:plc:", StringComparison.Ordinal)) throw new ArgumentException();
            if(did.Length != 32) throw new UnexpectedFirehoseDataException("Not a valid did:plc: " + did);
            var result = AtProtoS32.TryDecode128(did.Substring(8))!.Value;
            if (DeserializeDidPlcFromUInt128(result) != did) throw new UnexpectedFirehoseDataException("Not a valid did:plc: " + did);
            return result;
        }

        public bool IsLockHeld => Lock.IsReadLockHeld || Lock.IsWriteLockHeld || Lock.IsUpgradeableReadLockHeld;

        internal void EnsureLockNotAlreadyHeld()
        {
            if (IsLockHeld)
                ThrowIncorrectLockUsageException("Attempted to re-enter lock, perhaps due to callback reentrancy.");

        }

        [DoesNotReturn]
        public static Exception ThrowIncorrectLockUsageException(string message)
        {
            throw ThrowFatalError("ThrowIncorrectLockUsageException: " + message);
        }

        [DoesNotReturn]
        public static Exception ThrowFatalError(string message)
        {
            var stackTrace = new StackTrace(true);
            Console.Error.WriteLine(message);
            Console.Error.WriteLine(stackTrace.ToString());
            var directory = AppViewLiteConfiguration.GetString(AppViewLiteParameter.APPVIEWLITE_FATAL_ERROR_LOG_DIRECTORY);
            if (directory != null)
            {
                Directory.CreateDirectory(directory);
                File.WriteAllText(Path.Combine(directory, DateTime.UtcNow.ToString("yyyyMMddHHmmss") + ".log"), message + "\n" + stackTrace.ToString());
            }
            Environment.FailFast(message);
            throw new Exception(message);
        }
        public static ulong HashLabelName(string label) => System.IO.Hashing.XxHash64.HashToUInt64(MemoryMarshal.AsBytes<char>(label));
        public BlueskyLabel GetLabel(LabelId x)
        {
            return new BlueskyLabel
            {
                LabelId = x,
                Moderator = GetProfile(x.Labeler),
                ModeratorDid = GetDid(x.Labeler),
                Name = LabelNames.TryGetPreserveOrderSpanAny(x.NameHash, out var name) ? Encoding.UTF8.GetString(name.AsSmallSpan()) : throw new Exception("Don't have name for label name hash."),
                Data = TryGetLabelData(x)
            };
        }

        public BlueskyLabelData? TryGetLabelData(LabelId x)
        {
            var data = LabelData.TryGetPreserveOrderSpanLatest(x, out var bytes) ? DeserializeProto<BlueskyLabelData>(bytes.AsSmallSpan()) : null;
            if (data != null && !data.ReuseDefaultDefinition)
            {
                return data;
            }
            if (DefaultLabels.DefaultLabelData.TryGetValue(x.NameHash, out var defaults))
                return defaults;

            return data;
        }
        public LabelId[] GetPostLabels(PostId postId, HashSet<LabelId>? onlyLabels = null)
        {
            return PostLabels.GetValuesSorted(postId)
                .GroupAssumingOrderedInput(x => x.Labeler)
                .Select(x => x.Values[x.Values.Count - 1])
                .Where(x => !x.Neg)
                .Select(x => new LabelId(x.Labeler, x.KindHash))
                .Where(x => onlyLabels?.Contains(x) ?? true)
                .ToArray();
        }
        public LabelId[] GetProfileLabels(Plc plc, HashSet<LabelId>? onlyLabels = null)
        {
            return ProfileLabels.GetValuesSorted(plc)
                .GroupAssumingOrderedInput(x => x.Labeler)
                .Select(x => x.Values[x.Values.Count - 1])
                .Where(x => !x.Neg)
                .Select(x => new LabelId(x.Labeler, x.KindHash))
                .Where(x => onlyLabels?.Contains(x) ?? true)
                .ToArray();
        }



        public static byte[]? CompressBpe(string? text)
        {
            if (string.IsNullOrEmpty(text)) return null;
            lock (textCompressorUnlocked)
            {
                return textCompressorUnlocked.Compress(text);
            }
        }

        public static string? DecompressBpe(byte[]? bpe)
        {
            if (bpe == null || bpe.Length == 0) return null;
            lock (textCompressorUnlocked)
            {
                return textCompressorUnlocked.Decompress(bpe);
            }
        }


        public static bool IsNativeAtProtoDid(string did)
        {
            return
                did.StartsWith("did:plc:", StringComparison.Ordinal) ||
                did.StartsWith("did:web:", StringComparison.Ordinal);
        }

        public bool ProfileMatchesSearchTerms(BlueskyProfile x, bool alsoSearchDescriptions, string[] queryWords, string? wordPrefix)
        {
            if (x.IsBlockedByAdministrativeRule) return false;
            if (IsKnownMirror(x.Did)) return false;
            var words = StringUtils.GetAllWords(x.BasicData?.DisplayName).Concat(StringUtils.GetAllWords(x.PossibleHandle));
            if (alsoSearchDescriptions)
            {
                words = words.Concat(StringUtils.GetAllWords(x.BasicData?.Description));
            }
            var wordsHashset = words.ToHashSet();
            return
                queryWords.All(x => wordsHashset.Contains(x)) &&
                (wordPrefix == null || wordsHashset.Any(x => x.StartsWith(wordPrefix, StringComparison.Ordinal)));
        }

        public bool IsKnownMirror(string did)
        {
            if (!did.StartsWith("did:nostr:", StringComparison.Ordinal)) return false;
            var hash = StringUtils.HashUnicodeToUuid(did);
            return KnownMirrorsToIgnore.ContainsKey(hash);
        }

        public ((Plc Plc, bool IsPrivate)[] PossibleFollows, Func<Plc, BlueskyRelationships, bool> IsStillFollowed, Func<Plc, bool> IsPossiblyStillFollowed) GetFollowingFast(RequestContext ctx) // The lambda is SAFE to reuse across re-locks
        {
            var stillFollowedResult = new Dictionary<Plc, bool>();
            var possibleFollows = new Dictionary<Plc, Tid>();
            foreach (var item in RegisteredUserToFollowees.GetValuesUnsorted(ctx.LoggedInUser))
            {
                ref var rkey = ref CollectionsMarshal.GetValueRefOrAddDefault(possibleFollows, item.Member, out _);
                if (item.ListItemRKey.CompareTo(rkey) > 0)
                    rkey = item.ListItemRKey;
            }
            foreach (var item in ctx.UserContext.PrivateFollows)
            {
                if((item.Value.Flags & PrivateFollowFlags.PrivateFollow) != default)
                    possibleFollows[item.Key] = default;
            }

            return (possibleFollows.Select(x => (Plc: x.Key, IsPrivate: x.Value == default)).ToArray(), (plc, rels) =>
            {
                if (plc == default) return true;

                // Callers can assume that this lambda is SAFE to reuse across re-locks (must not capture ManagedOrNativeArrays)

                if (!possibleFollows.TryGetValue(plc, out var rkey)) return false;
                if (rkey == default) return true; // private follow

                ref var result = ref CollectionsMarshal.GetValueRefOrAddDefault(stillFollowedResult, plc, out var exists);
                if (!exists)
                {
                    result = !rels.Follows.IsDeleted(new Relationship(ctx.LoggedInUser, rkey));
                }
                return result;
            },
            plc => 
            {
                if (plc == default) return true;

                if (!possibleFollows.TryGetValue(plc, out var rkey)) return false;
                if (rkey == default) return true; // private follow

                if (stillFollowedResult.TryGetValue(plc, out var result))
                    return result;
                
                return true; // we don't know, assume yes
            }
            );
        }


        internal QualifiedPluggablePostId TryGetStoredSyntheticTidFromPluggablePostId(QualifiedPluggablePostId postId)
        {
            var hash = postId.GetExternalPostIdHash();
            if (ExternalPostIdHashToSyntheticTid.TryGetSingleValue(hash, out var tid))
            {
                return new QualifiedPluggablePostId(postId.Did, postId.PostId.WithTid(tid));
            }
            return postId;
        }

        internal BlueskyPost GetPostAndMaybeRepostedBy(PostId postId, Relationship repost)
        {
            var post = GetPost(postId);
            if (repost != default)
            {
                post.RepostDate = repost.RelationshipRKey.Date;
                post.RepostedBy = GetProfile(repost.Actor);
            }
            return post;
        }


        public Func<PostIdTimeFirst, bool> GetIsPostSeenFuncForUserRequiresLock(Plc loggedInUser)
        {
            var seenPosts = SeenPosts.GetValuesChunked(loggedInUser).ToArray();

            var cache = new Dictionary<PostIdTimeFirst, bool>();

            return postId =>
            {
                bool Core()
                {
                    foreach (var slice in seenPosts)
                    {
                        var span = slice.AsSpan();
                        var index = span.BinarySearch(new PostEngagement(postId, default));
                        if (index >= 0)
                        {
                            return true;
                        }
                        else
                        {
                            index = ~index;
                            if (index != span.Length && span[index].PostId == postId)
                                return true;
                        }
                    }
                    return false;
                }

                if (!cache.TryGetValue(postId, out var result))
                {
                    result = Core();
                }
                return result;
            };
        }


        internal static void EnsureNotExcessivelyFutureDate(Tid tid)
        {
            if ((tid.Date - DateTime.UtcNow).TotalMinutes > 15)
                throw new UnexpectedFirehoseDataException("Post date is too much into the future.");
        }



        public RssRefreshInfo? GetRssRefreshInfo(Plc plc)
        {
            if (RssRefreshInfos.TryGetPreserveOrderSpanLatest(plc, out var bytes))
            {
                var proto = DeserializeProto<RssRefreshInfo>(bytes.AsSmallSpan());
                proto.MakeUtc();
                return proto;
            }
            return null;
        }

        public void UpdatePrivateFollow(PrivateFollow info, RequestContext ctx)
        {
            lock (info)
            {
                ctx.UserContext.PrivateProfile!.PrivateFollows = ctx.UserContext.PrivateFollows.Values.Where(x => x.Plc != info.Plc).Append(info).ToArray();
                ctx.UserContext.PrivateFollows = ctx.UserContext.PrivateProfile!.PrivateFollows.ToDictionary(x => new Plc(x.Plc), x => x);
            }
            SaveAppViewLiteProfile(ctx);
        }

        public void SaveAppViewLiteProfile(RequestContext ctx)
        {
            SaveAppViewLiteProfile(ctx.UserContext);
        }

        public LabelerSubscription[] DefaultLabelSubscriptions;

        public void PopulateViewerFlags(BlueskyProfile profile, RequestContext ctx)
        {
            if (ctx.IsLoggedIn && Follows.HasActor(profile.Plc, ctx.LoggedInUser, out var followRel))
                profile.IsFollowedBySelf = followRel.RelationshipRKey;
            profile.IsYou = profile.Plc == ctx.Session?.LoggedInUser;
            profile.BlockReason = GetBlockReason(profile.Plc, ctx);
            profile.FollowsYou = ctx.IsLoggedIn && Follows.HasActor(ctx.LoggedInUser, profile.Plc, out _);
            profile.Labels = GetProfileLabels(profile.Plc, ctx.NeedsLabels).Select(x => (BlueskyModerationBase)GetLabel(x)).Concat(ctx.LabelSubscriptions.Where(x => x.ListRKey != 0).Select(x =>
            {
                var listId = new Models.Relationship(new Plc(x.LabelerPlc), new Tid(x.ListRKey));
                if (IsMemberOfList(listId, profile.Plc)) 
                {
                    return GetList(listId);
                }
                return null;
            }).WhereNonNull()).ToArray();
            if (profile.BlockReason != default && ctx.IsLoggedIn && Blocks.HasActor(profile.Plc, ctx.LoggedInUser, out var blockedBySelf))
            {
                profile.IsBlockedBySelf = blockedBySelf.RelationshipRKey;
            }
            // ctx.Session is null when logging in (ourselves)
            profile.PrivateFollow = ctx.UserContext?.GetPrivateFollow(profile.Plc) ?? new() { Plc = profile.Plc.PlcValue };
        }

        public void AssertCanRead()
        {
            if (!IsLockHeld)
            {
                ThrowIncorrectLockUsageException("Attempting to read without holding a read lock.");
            }
        }

        public ICloneableAsReadOnly CloneAsReadOnly()
        {
            AssertCanRead();
            var sw = Stopwatch.StartNew();
            var copy = new BlueskyRelationships(isReadOnly: true);
            copy.Version = this.Version;
            copy.Lock = new();
            copy.IsReadOnly = true;
            copy.DidToPlcConcurrentCache = this.DidToPlcConcurrentCache;
            copy.PlcToDidConcurrentCache = this.PlcToDidConcurrentCache;
            copy.ShutdownRequestedCts = this.ShutdownRequestedCts;
            copy.DefaultLabelSubscriptions = this.DefaultLabelSubscriptions;
            var fields = typeof(BlueskyRelationships).GetFields(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            foreach (var field in fields)
            {
                if (field.FieldType.IsAssignableTo(typeof(ICloneableAsReadOnly)))
                {
                    var copiedField = ((ICloneableAsReadOnly)field.GetValue(this)!).CloneAsReadOnly();
                    field.SetValue(copy, copiedField);
                    copy.disposables.Add((ICheckpointable)copiedField);
                }
                else
                {
                    // Console.Error.WriteLine("Extra field: " + field.Name);
                }
            }
            copy.ReplicaAge = Stopwatch.StartNew();

            // . Copied bytes: " + StringUtils.ToHumanBytes(copiedQueueBytes) + "
            Console.Error.WriteLine("Captured readonly replica, time: " + sw.Elapsed.TotalMilliseconds.ToString("0.00") + " ms");
            return copy;
        }


        public List<BlueskyPost> MakeFullReplyChainExcludingLeaf(BlueskyPost post)
        {
            PostId? current = post.InReplyToPostId!.Value;
            var ancestors = new List<BlueskyPost>();
            while (current != null)
            {
                var parent = GetPost(current.Value);
                ancestors.Add(parent);
                current = parent.InReplyToPostId;
            }
            ancestors.Reverse();
            ancestors[0].RepostedBy = post.RepostedBy;
            ancestors[0].RepostDate = post.RepostDate;
            post.RepostedBy = null;
            post.RepostDate = null;
            return ancestors;
        }


        public long GetApproximateLikeCount(PostIdTimeFirst postId, bool couldBePluggablePost, Dictionary<Plc, ManagedOrNativeArray<RecentPostLikeCount>[]?> plcToRecentPostLikes)
        {
            var likeCount = Likes.GetApproximateActorCount(postId);
            if (likeCount == 0 && couldBePluggablePost)
            {
                if (!plcToRecentPostLikes.TryGetValue(postId.Author, out var recentPostLikes))
                {
                    var did = GetDid(postId.Author);
                    if (TryGetPluggableProtocolForDid(did) is { } pluggable && pluggable.ProvidesLikeCount)
                    {
                        recentPostLikes = RecentPluggablePostLikeCount.GetValuesChunkedLatestFirst(postId.Author).ToArray();
                    }
                    else
                    {
                        recentPostLikes = null;
                    }
                    plcToRecentPostLikes[postId.Author] = recentPostLikes;
                }
                if (recentPostLikes != null)
                {
                    likeCount =
                        recentPostLikes
                        .Select(x => x.EnumerateFromReverseRightBiased(new RecentPostLikeCount(postId.PostRKey, int.MaxValue)).FirstOrDefault(x => x.PostRKey == postId.PostRKey))
                        .FirstOrDefault(x => x != default)
                        .LikeCount;
                }
            }
            return likeCount;
        }



        public Tid? TryGetLatestBookmarkForPost(PostId postId, Plc loggedInUser)
        {
            ManagedOrNativeArray<BookmarkPostFirst>[]? a = null;
            ManagedOrNativeArray<Tid>[]? b = null;
            return TryGetLatestBookmarkForPost(postId, loggedInUser, ref a, ref b);
        }
        public Tid? TryGetLatestBookmarkForPost(PostId postId, Plc loggedInUser, ref ManagedOrNativeArray<BookmarkPostFirst>[]? userBookmarks, ref ManagedOrNativeArray<Tid>[]? userDeletedBookmarks)
        {
            if (userBookmarks == null)
            {
                userBookmarks = this.Bookmarks.GetValuesChunkedLatestFirst(loggedInUser).ToArray();
            }
            foreach (var chunk in userBookmarks)
            {
                var span = chunk.AsSpan();
                var index = span.BinarySearch(new BookmarkPostFirst(postId, default));
                index = ~index;
                
                BookmarkPostFirst bookmark = default;
                if (index == span.Length) continue;

                while (true)
                {
                    bookmark = span[index];
                    if (index != span.Length - 1 && (PostId)span[index + 1].PostId == postId) index++;
                    else break;
                }

                if ((PostId)bookmark.PostId != postId) continue;

                if (userDeletedBookmarks == null)
                {
                    userDeletedBookmarks = this.BookmarkDeletions.GetValuesChunked(loggedInUser).ToArray();
                }
                if (userDeletedBookmarks.Any(x => x.AsSpan().BinarySearch(bookmark.BookmarkRKey) >= 0))
                    return null;

                return bookmark.BookmarkRKey;
            }
            return null;
        }

        public void PopulateQuotedPost(BlueskyPost post, RequestContext ctx)
        {
            if (post.QuotedPost != null || post.Data?.QuotedPostId == null)
                return;


            post.QuotedPost = GetPost(new PostId(new Plc(post.Data.QuotedPlc!.Value), new Tid(post.Data.QuotedRKey!.Value)));
            PopulateViewerFlags(post.QuotedPost, ctx);
        }


        public bool? ShouldIncludeLeafPostInFollowingFeed(BlueskyPost post, RequestContext ctx)
        {
            if (post.Data?.Deleted == true) return false;
            if (post.Data?.IsReplyToUnspecifiedPost == true) return false;

            var shouldInclude = ShouldIncludeLeafOrRootPostInFollowingFeed(post, ctx);
            if (shouldInclude != null) return shouldInclude;
            if (post.RepostedBy != null) return true;

            return null;
        }

        public bool? ShouldIncludeLeafOrRootPostInFollowingFeed(BlueskyPost post, RequestContext ctx)
        {
            PopulateViewerFlags(post, ctx);
            if (post.IsMuted) return false;

            if (post.Author.BlockReason != default) return false;

            PopulateQuotedPost(post, ctx);
            if (post.QuotedPost != null)
            {
                if (post.QuotedPost?.IsMuted == true) return false;
            }

            return null;
        }




        public string? GetOriginalPostUrl(PostIdTimeFirst postId, string did)
        {
            var pluggable = PluggableProtocol.TryGetPluggableProtocolForDid(did);
            if (pluggable == null) return null;

            var postData = TryGetPostData(postId, skipBpeDecompression: true);
            if (postData == null) return null;

            var post = new BlueskyPost() { Author = new BlueskyProfile { Did = did }, RKey = postId.PostRKey.ToString()!, Data = postData };
            DecompressPluggablePostData(postId.PostRKey, postData, did);
            if (postData.PluggablePostId == null) return null;
            return pluggable.TryGetOriginalPostUrl(new QualifiedPluggablePostId(did, postData.PluggablePostId.Value), post);
        }

    }


    public delegate void LiveNotificationDelegate(Versioned<PostStatsNotification> notification, Plc commitPlc);
}

