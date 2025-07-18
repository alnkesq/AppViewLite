using AppViewLite.Models;
using AppViewLite.Numerics;
using AppViewLite.PluggableProtocols;
using FishyFlip.Lexicon.App.Bsky.Actor;
using FishyFlip.Lexicon.App.Bsky.Embed;
using FishyFlip.Lexicon.App.Bsky.Feed;
using FishyFlip.Lexicon.App.Bsky.Richtext;
using FishyFlip.Lexicon.Com.Atproto.Repo;
using FishyFlip.Models;
using AppViewLite.Storage;
using DuckDbSharp.Types;
using AppViewLite.Numerics;
using AppViewLite.Storage;
using System;
using System.Collections.Concurrent;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.IO.Hashing;
using System.Linq;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;

namespace AppViewLite
{
    public class BlueskyRelationships : LoggableBase, IDisposable, ICloneableAsReadOnly
    {

        public long Version = 1;
        public int ManagedThreadIdWithWriteLock;
        public int ForbidUpgrades;
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
        public CombinedPersistentMultiDictionary<PostIdTimeFirst, byte> Threadgates;
        public CombinedPersistentMultiDictionary<PostIdTimeFirst, byte> Postgates;
        public CombinedPersistentMultiDictionary<Relationship, Relationship> ListBlocks;
        public CombinedPersistentMultiDictionary<Relationship, Relationship> ListSubscribers;
        public CombinedPersistentMultiDictionary<Relationship, DateTime> ListBlockDeletions;
        public CombinedPersistentMultiDictionary<PostIdTimeFirst, PostId> DirectReplies;
        public CombinedPersistentMultiDictionary<PostIdTimeFirst, PostId> Quotes;
        public CombinedPersistentMultiDictionary<PostIdTimeFirst, DateTime> PostDeletions;
        public CombinedPersistentMultiDictionary<Plc, byte> Profiles;
        public CombinedPersistentMultiDictionary<Plc, byte> PlcToDidOther;
        public CombinedPersistentMultiDictionary<Plc, UInt128> PlcToDidPlc;
        public CombinedPersistentMultiDictionary<PostIdTimeFirst, byte> PostData;
        public CombinedPersistentMultiDictionary<PostIdTimeFirst, int> RecentPluggablePostLikeCount;
        public CombinedPersistentMultiDictionary<HashedWord, ApproximateDateTime32> PostTextSearch;
        public CombinedPersistentMultiDictionary<Plc, DateTime> FailedProfileLookups;
        public CombinedPersistentMultiDictionary<Relationship, DateTime> FailedListLookups;
        public CombinedPersistentMultiDictionary<PostId, DateTime> FailedPostLookups;
        public CombinedPersistentMultiDictionary<Plc, Notification> LastSeenNotifications;
        public CombinedPersistentMultiDictionary<Plc, Notification> LastSeenDarkNotifications;
        public CombinedPersistentMultiDictionary<Plc, Notification> Notifications;
        public CombinedPersistentMultiDictionary<Plc, Notification> DarkNotifications;
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
        public CombinedPersistentMultiDictionary<DateTime, byte> LastRetrievedPlcDirectoryEntry;
        public CombinedPersistentMultiDictionary<DuckDbUuid, HandleVerificationResult> HandleToDidVerifications;
        public CombinedPersistentMultiDictionary<PostId, LabelEntry> PostLabels;
        public CombinedPersistentMultiDictionary<Plc, LabelEntry> ProfileLabels;
        public CombinedPersistentMultiDictionary<LabelId, PostIdTimeFirst> LabelToPosts;
        public CombinedPersistentMultiDictionary<LabelId, Plc> LabelToProfiles;
        public CombinedPersistentMultiDictionary<ulong, byte> LabelNames;
        public CombinedPersistentMultiDictionary<LabelId, byte> LabelData;
        public CombinedPersistentMultiDictionary<DuckDbUuid, byte> CustomEmojis;
        public CombinedPersistentMultiDictionary<DuckDbUuid, byte> KnownMirrorsToIgnore;
        public CombinedPersistentMultiDictionary<DuckDbUuid, Tid> ExternalPostIdHashToSyntheticTid;
        public CombinedPersistentMultiDictionary<Plc, PostEngagement> SeenPosts;
        public CombinedPersistentMultiDictionary<Plc, TimePostSeen> SeenPostsByDate;
        public CombinedPersistentMultiDictionary<Plc, byte> RssRefreshInfos;
        public CombinedPersistentMultiDictionary<DuckDbUuid, byte> NostrSeenPubkeyHashes;
        public CombinedPersistentMultiDictionary<Plc, byte> ReposterOnlyProfile;
        public CombinedPersistentMultiDictionary<DuckDbUuid, byte> OpenGraphData;
        public CombinedPersistentMultiDictionary<Plc, byte> AccountStates;

        public ConcurrentFullEvictionCache<DuckDbUuid, Plc> DidToPlcConcurrentCache;
        public ConcurrentFullEvictionCache<Plc, string> PlcToDidConcurrentCache;

        public DateTime PlcDirectorySyncDate;
        private Plc LastAssignedPlc;
        public TimeSpan PlcDirectoryStaleness => DateTime.UtcNow - PlcDirectorySyncDate;

        public Stopwatch? ReplicaAge;
        public bool IsReplica => ReplicaAge != null;
        public bool IsPrimary => !IsReplica;

        private HashSet<Plc> registerForNotificationsCache = new();
        private List<ICheckpointable> disposables = new();

        public IReadOnlyList<CombinedPersistentMultiDictionary> AllMultidictionaries => disposables.OfType<CombinedPersistentMultiDictionary>().Concat(disposables.OfType<RelationshipDictionary>().SelectMany(x => x.Multidictionaries)).ToArray();

        internal UserPairEngagementCache UserPairEngagementCache = new UserPairEngagementCache();

        public static int TableWriteBufferSize = AppViewLiteConfiguration.GetInt32(AppViewLiteParameter.APPVIEWLITE_TABLE_WRITE_BUFFER_SIZE) ?? (10 * 1024 * 1024);
        public int AvoidFlushes;
        public ReaderWriterLockSlim Lock;

        public bool IsAtLeastVersion(long minVersion, TimeSpan maxStaleness, long latestKnownVersion)
        {
            return this.Version >= minVersion && (this.Version == latestKnownVersion || ReplicaAge!.Elapsed <= maxStaleness);
        }


        public string BaseDirectory { get; }
        private FileStream? lockFile;
        private Dictionary<string, SliceName[]>? checkpointToLoad;
        private Dictionary<string, FirehoseCursor>? firehoseCursors;
        private GlobalCheckpoint? loadedCheckpoint;

        private T Register<T>(T r) where T : ICheckpointable
        {
            disposables.Add(r);
            return r;
        }
        private CombinedPersistentMultiDictionary<TKey, TValue> RegisterDictionary<TKey, TValue>(string name, PersistentDictionaryBehavior behavior = PersistentDictionaryBehavior.SortedValues, Func<IEnumerable<TValue>, IEnumerable<TValue>>? onCompactation = null, Func<PruningContext, TKey, bool>? shouldPreserveKey = null, Func<PruningContext, TKey, TValue, bool>? shouldPreserveValue = null, CombinedPersistentMultiDictionary<TKey, TValue>.CachedView[]? caches = null, Func<TKey, MultiDictionaryIoPreference>? getIoPreferenceForKey = null, bool cachesAreMandatory = false) where TKey : unmanaged, IComparable<TKey> where TValue : unmanaged, IComparable<TValue>, IEquatable<TValue>
        {
            if (!UseProbabilisticSets && caches != null && !cachesAreMandatory)
            {
                foreach (var item in caches)
                {
                    item.Dispose();
                }
                caches = null;
            }
            var table = new CombinedPersistentMultiDictionary<TKey, TValue>(
                BaseDirectory + "/" + name,
                checkpointToLoad!.TryGetValue(name, out var slices) ? slices : [],
                behavior,
                caches,
                getIoPreferenceForKey ?? GetIoPreferenceFunc<TKey>()
            )
            {
                WriteBufferSize = TableWriteBufferSize,
                OnCompactation = onCompactation,
                ShouldPreserveKey = shouldPreserveKey,
                ShouldPreserveValue = shouldPreserveValue,
            };
            return Register(table);
        }



        private const MultiDictionaryIoPreference SomewhatRecentEventIoPreference = MultiDictionaryIoPreference.KeysAndOffsetsMmap;
        private const MultiDictionaryIoPreference VeryRecentEventIoPreference = MultiDictionaryIoPreference.AllMmap;
        private readonly static TimeSpan SomewhatRecentEventAge = TimeSpan.FromHours(72);
        private readonly static TimeSpan VeryRecentEventAge = TimeSpan.FromHours(36);
        internal static Func<TKey, MultiDictionaryIoPreference>? GetIoPreferenceFunc<TKey>() => (Func<TKey, MultiDictionaryIoPreference>?)GetIoPreferenceFunc(typeof(TKey));
        internal static Delegate? GetIoPreferenceFunc(Type type)
        {
            if (type == typeof(PostIdTimeFirst)) return (PostIdTimeFirst r) => GetIoPreferenceForDate(r.PostRKey.Date);
            if (type == typeof(TimePostSeen)) return (TimePostSeen r) => GetIoPreferenceForDate(r.Date);
            if (type == typeof(PostEngagement)) return (PostEngagement r) => GetIoPreferenceForDate(r.PostId.PostRKey.Date);

            if (type == typeof(Plc)) return (Plc r) => MultiDictionaryIoPreference.KeysAndOffsetsMmap;
            return null;
        }

        private static bool IsSomewhatRecentDate(DateTime date) => DateTime.UtcNow - date < SomewhatRecentEventAge;
        private static bool IsVeryRecentDate(DateTime date) => DateTime.UtcNow - date < VeryRecentEventAge;

        private static MultiDictionaryIoPreference GetIoPreferenceForDate(DateTime date)
        {
            var ago = DateTime.UtcNow - date;
            if (ago < VeryRecentEventAge) return VeryRecentEventIoPreference;
            else if (ago < SomewhatRecentEventAge) return SomewhatRecentEventIoPreference;
            return default;
        }

        private RelationshipDictionary<TTarget> RegisterRelationshipDictionary<TTarget>(string name, Func<TTarget, bool, UInt24?>? targetToApproxTarget, RelationshipProbabilisticCache<TTarget>? relationshipCache = null, Func<TTarget, MultiDictionaryIoPreference>? getCreationsIoPreferenceForKey = null, KeyProbabilisticCache<Relationship, DateTime>? deletionProbabilisticCache = null, bool zeroApproxTargetsAreValid = false) where TTarget : unmanaged, IComparable<TTarget>
        {
            return Register(new RelationshipDictionary<TTarget>(BaseDirectory, name, checkpointToLoad!, targetToApproxTarget, relationshipCache, getCreationsIoPreferenceForKey, deletionProbabilisticCache: deletionProbabilisticCache, zeroApproxTargetsAreValid: zeroApproxTargetsAreValid)
            {
                GetVersion = () => this.Version
            });
        }

        public bool IsReadOnly { get; private set; }



        public static TimeSeries FirehoseProcessingLagBehindTimeSeries = null!;
        public static TimeSeries FirehoseEventReceivedTimeSeries = null!;
        public static TimeSeries FirehoseEventProcessedTimeSeries = null!;
        public readonly static TimeSpan TimeSeriesTotalTime = TimeSpan.FromDays(AppViewLiteConfiguration.GetInt32(AppViewLiteParameter.APPVIEWLITE_EVENT_CHART_HISTORY_DAYS) ?? 2);
        public static void CreateTimeSeries()
        {

            var timeSeriesTotalTime = TimeSeriesTotalTime;
            var timeSeriesInterval = TimeSpan.FromSeconds(1);
            var timeSeriesExtraTime = TimeSpan.FromSeconds(30);
            var clearTimeSkipFuture = TimeSpan.FromSeconds(10);
            var clearTimeLength = TimeSpan.FromSeconds(10);

            FirehoseEventReceivedTimeSeries = new(timeSeriesTotalTime, timeSeriesInterval, timeSeriesExtraTime);
            FirehoseEventProcessedTimeSeries = new(timeSeriesTotalTime, timeSeriesInterval, timeSeriesExtraTime);
            FirehoseProcessingLagBehindTimeSeries = new(timeSeriesTotalTime, timeSeriesInterval, timeSeriesExtraTime);
            TimeSeries.StartClearThread(timeSeriesInterval, clearTimeSkipFuture, clearTimeLength, [FirehoseEventReceivedTimeSeries, FirehoseEventProcessedTimeSeries, FirehoseProcessingLagBehindTimeSeries]).FireAndForget();

        }

#nullable disable
        private BlueskyRelationships(bool isReadOnly)
        {
            Interlocked.Increment(ref AliveBlueskyRelationshipsDatabases);

            // if this is a secondary, fields will be set via reflection
        }
#nullable restore

        public BlueskyRelationships(string basedir, bool isReadOnly, string[] rootDirectoriesToGcCollect)
            : this(isReadOnly)
        {
            this.RootDirectoriesToGcCollect = rootDirectoriesToGcCollect;
            ApproximateLikeCountCache = new(128 * 1024);
            ReplicaOnlyApproximateLikeCountCache = new(128 * 1024);
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

            lockFile = isReadOnly ? null : new FileStream(basedir + "/.lock", FileMode.Create, FileAccess.Write, FileShare.None, 1024, FileOptions.DeleteOnClose);

            var checkpointsDir = new DirectoryInfo(basedir + "/checkpoints");
            checkpointsDir.Create();
            var latestCheckpoint = checkpointsDir.EnumerateFiles("*.pb").MaxBy(x => (DateTime?)x.LastWriteTimeUtc);
            if (latestCheckpoint == null && !AppViewLiteConfiguration.GetBool(AppViewLiteParameter.APPVIEWLITE_ALLOW_NEW_DATABASE).GetValueOrDefault())
                throw new Exception("A checkpoint file to load was not found. Specify '--allow-new-database 1' to create a new database.");
            loadedCheckpoint = latestCheckpoint != null ? DeserializeProto<GlobalCheckpoint>(File.ReadAllBytes(latestCheckpoint.FullName)) : new GlobalCheckpoint();
            checkpointToLoad = (loadedCheckpoint.Tables ?? []).ToDictionary(x => x.Name, x => (x.Slices ?? []).Select(x => x.ToSliceName()).ToArray());

            var resetFirehoseCursors = AppViewLiteConfiguration.GetStringList(AppViewLiteParameter.APPVIEWLITE_RESET_FIREHOSE_CURSORS) ?? ["*"];
            firehoseCursors = loadedCheckpoint?.FirehoseCursors?
                .Select(x => { x.MakeUtc(); return x; })
                .Where(x => !(resetFirehoseCursors.Contains(x.FirehoseUrl) || resetFirehoseCursors.Contains("*")))
                .ToDictionary(x => x.FirehoseUrl, x => x) ?? new();

            LastRetrievedPlcDirectoryEntry = RegisterDictionary<DateTime, byte>("last-retrieved-plc-directory-6", PersistentDictionaryBehavior.KeySetOnly);
            PlcDirectorySyncDate = LastRetrievedPlcDirectoryEntry.MaximumKey ?? new DateTime(2022, 11, 17, 00, 35, 16, DateTimeKind.Utc) /* first event on the PLC directory */;
            var plcDirectoryIsReasonablyUpToDate = PlcDirectoryStaleness.TotalHours < 6;
            DidHashToUserId = RegisterDictionary<DuckDbUuid, Plc>("did-hash-to-user-id", PersistentDictionaryBehavior.SingleValue, getIoPreferenceForKey: _ => MultiDictionaryIoPreference.AllMmap, caches: plcDirectoryIsReasonablyUpToDate ? [] : [new KeyProbabilisticCache<DuckDbUuid, Plc>(128 * 1024 * 1024, 7)]);
            PlcToDidPlc = RegisterDictionary<Plc, UInt128>("plc-to-did-plc", PersistentDictionaryBehavior.SingleValue);
            PlcToDidOther = RegisterDictionary<Plc, byte>("plc-to-did-other", PersistentDictionaryBehavior.PreserveOrder);
            PdsIdToString = RegisterDictionary<Pds, byte>("pds-id-to-string", PersistentDictionaryBehavior.PreserveOrder);
            PdsHashToPdsId = RegisterDictionary<DuckDbUuid, Pds>("pds-hash-to-id", PersistentDictionaryBehavior.SingleValue);


            Likes = RegisterRelationshipDictionary<PostIdTimeFirst>("post-like-time-first", GetApproxTime24); //, new(1024 * 1024 * 1024, 6));
            Reposts = RegisterRelationshipDictionary<PostIdTimeFirst>("post-repost-time-first", GetApproxTime24); //, new(256 * 1024 * 1024, 10));
            Follows = RegisterRelationshipDictionary<Plc>("follow", GetApproxPlc24, new(256 * 1024 * 1024, 6), _ => MultiDictionaryIoPreference.KeysAndOffsetsMmap, deletionProbabilisticCache: new KeyProbabilisticCache<Relationship, DateTime>(128 * 1024 * 1024, 4), zeroApproxTargetsAreValid: true);
            Blocks = RegisterRelationshipDictionary<Plc>("block", GetApproxPlc24, new(64 * 1024 * 1024, 4), zeroApproxTargetsAreValid: true);

            Bookmarks = RegisterDictionary<Plc, BookmarkPostFirst>("bookmark");
            RecentBookmarks = RegisterDictionary<Plc, BookmarkDateFirst>("bookmark-recent");
            BookmarkDeletions = RegisterDictionary<Plc, Tid>("bookmark-deletion");
            DirectReplies = RegisterDictionary<PostIdTimeFirst, PostId>("post-reply-direct-2");
            Quotes = RegisterDictionary<PostIdTimeFirst, PostId>("post-quote-2");
            PostDeletions = RegisterDictionary<PostIdTimeFirst, DateTime>("post-deletion-2", PersistentDictionaryBehavior.SingleValue);

            Profiles = RegisterDictionary<Plc, byte>("profile-basic-2", PersistentDictionaryBehavior.PreserveOrder);
            ProfileSearchLong = RegisterDictionary<HashedWord, Plc>("profile-search-long");
            ProfileSearchDescriptionOnly = RegisterDictionary<HashedWord, Plc>("profile-search-description-only");
            ProfileSearchPrefix8 = RegisterDictionary<SizeLimitedWord8, Plc>("profile-search-prefix");
            ProfileSearchPrefix2 = RegisterDictionary<SizeLimitedWord2, Plc>("profile-search-prefix-2-letters");

            PostData = RegisterDictionary<PostIdTimeFirst, byte>("post-data-time-first-2", PersistentDictionaryBehavior.PreserveOrder, shouldPreserveKey: (ctx, postId) => ctx.ShouldPreservePost(postId), getIoPreferenceForKey: x => IsVeryRecentDate(x.PostRKey.Date) ? MultiDictionaryIoPreference.KeysAndOffsetsMmap : MultiDictionaryIoPreference.None);
            RecentPluggablePostLikeCount = RegisterDictionary<PostIdTimeFirst, int>("recent-post-like-count-2", PersistentDictionaryBehavior.SingleValue, getIoPreferenceForKey: x => MultiDictionaryIoPreference.AllMmap);
            PostTextSearch = RegisterDictionary<HashedWord, ApproximateDateTime32>("post-text-approx-time-32");
            FailedProfileLookups = RegisterDictionary<Plc, DateTime>("profile-basic-failed");
            FailedPostLookups = RegisterDictionary<PostId, DateTime>("post-data-failed");
            FailedListLookups = RegisterDictionary<Relationship, DateTime>("list-data-failed");

            ListItems = RegisterDictionary<Relationship, ListEntry>("list-item", caches: [new DelegateProbabilisticCache<Relationship, ListEntry, (Relationship, Plc)>("member", 32 * 1024 * 1024, 6, (k, v) => (k, v.Member))]);
            ListItemDeletions = RegisterDictionary<Relationship, DateTime>("list-item-deletion", PersistentDictionaryBehavior.SingleValue);
            ListMemberships = RegisterDictionary<Plc, ListMembership>("list-membership-2");

            Lists = RegisterDictionary<Relationship, byte>("list", PersistentDictionaryBehavior.PreserveOrder);
            ListDeletions = RegisterDictionary<Relationship, DateTime>("list-deletion", PersistentDictionaryBehavior.SingleValue, getIoPreferenceForKey: _ => MultiDictionaryIoPreference.AllMmap);

            Threadgates = RegisterDictionary<PostIdTimeFirst, byte>("threadgate-2", PersistentDictionaryBehavior.PreserveOrder);
            Postgates = RegisterDictionary<PostIdTimeFirst, byte>("postgate-2", PersistentDictionaryBehavior.PreserveOrder);
            ListBlocks = RegisterDictionary<Relationship, Relationship>("list-block", PersistentDictionaryBehavior.SingleValue, caches: [new DelegateProbabilisticCache<Relationship, Relationship, Plc>("blocklist-subscriber", 2 * 1024 * 1024, 6, (k, v) => k.Actor)], getIoPreferenceForKey: _ => MultiDictionaryIoPreference.AllMmap);
            ListBlockDeletions = RegisterDictionary<Relationship, DateTime>("list-block-deletion", PersistentDictionaryBehavior.SingleValue);
            ListSubscribers = RegisterDictionary<Relationship, Relationship>("list-subscribers", PersistentDictionaryBehavior.SingleValue);

            if (ListSubscribers.KeyCount == 0)
            {
                foreach (var item in ListBlocks.EnumerateUnsorted())
                {
                    ListSubscribers.Add(item.Value, item.Key);
                }
            }

            Notifications = RegisterDictionary<Plc, Notification>("notification-2");
            DarkNotifications = RegisterDictionary<Plc, Notification>("notification-dark");

            RegisteredUserToFollowees = RegisterDictionary<Plc, ListEntry>("registered-user-to-followees");
            RssFeedToFollowers = RegisterDictionary<Plc, Plc>("registered-user-to-rss-feeds");

            UserToRecentPosts = RegisterDictionary<Plc, RecentPost>("user-to-recent-posts-2");
            UserToRecentReposts = RegisterDictionary<Plc, RecentRepost>("user-to-recent-reposts-2", onCompactation: x => { var threshold = DateTime.UtcNow.AddDays(-7); return x.Where((x, i) => i == 0 || x.RepostRKey.Date > threshold); });
            UserToRecentMediaPosts = RegisterDictionary<Plc, Tid>("user-to-recent-media-posts");

            CarImports = RegisterDictionary<RepositoryImportKey, byte>("car-import-proto-2", PersistentDictionaryBehavior.PreserveOrder);

            LastSeenNotifications = RegisterDictionary<Plc, Notification>("last-seen-notification-3", PersistentDictionaryBehavior.SingleValue);
            LastSeenDarkNotifications = RegisterDictionary<Plc, Notification>("last-seen-notification-dark", PersistentDictionaryBehavior.SingleValue);

            AppViewLiteProfiles = RegisterDictionary<Plc, byte>("appviewlite-profile", PersistentDictionaryBehavior.PreserveOrder);
            FeedGenerators = RegisterDictionary<RelationshipHashedRKey, byte>("feed-generator", PersistentDictionaryBehavior.PreserveOrder);
            FeedGeneratorSearch = RegisterDictionary<HashedWord, RelationshipHashedRKey>("feed-generator-search");
            FeedGeneratorLikes = RegisterRelationshipDictionary<RelationshipHashedRKey>("feed-generator-like-2", GetApproxRkeyHash24, zeroApproxTargetsAreValid: true);
            FeedGeneratorDeletions = RegisterDictionary<RelationshipHashedRKey, DateTime>("feed-deletion");
            DidDocs = RegisterDictionary<Plc, byte>("did-doc-6", PersistentDictionaryBehavior.PreserveOrder, caches: [new WhereSelectCache<Plc, byte, Plc, byte>("labeler", PersistentDictionaryBehavior.PreserveOrder, (plc, diddocBytes) =>
            {
                var diddoc = DidDocProto.DeserializeFromBytes(diddocBytes.AsSmallSpan(), onlyIfProtobufEncoding: true /* labeler can only exist in protobuf encoding */);
                if (diddoc == null) return default;
                if (diddoc.AtProtoLabeler == null) return default;
                return (plc, Encoding.UTF8.GetBytes(diddoc.AtProtoLabeler));
            })], cachesAreMandatory: true);
            HandleToPossibleDids = RegisterDictionary<HashedWord, Plc>("handle-to-possible-dids");
            HandleToDidVerifications = RegisterDictionary<DuckDbUuid, HandleVerificationResult>("handle-verifications", getIoPreferenceForKey: x => MultiDictionaryIoPreference.AllMmap);

            PostLabels = RegisterDictionary<PostId, LabelEntry>("post-label", caches: [new KeyProbabilisticCache<PostId, LabelEntry>(32 * 1024 * 1024, 9)]);
            ProfileLabels = RegisterDictionary<Plc, LabelEntry>("profile-label");
            LabelToPosts = RegisterDictionary<LabelId, PostIdTimeFirst>("label-to-posts");
            LabelToProfiles = RegisterDictionary<LabelId, Plc>("label-to-profiles");
            LabelNames = RegisterDictionary<ulong, byte>("label-name", PersistentDictionaryBehavior.PreserveOrder);
            LabelData = RegisterDictionary<LabelId, byte>("label-data", PersistentDictionaryBehavior.PreserveOrder);

            CustomEmojis = RegisterDictionary<DuckDbUuid, byte>("custom-emoji", PersistentDictionaryBehavior.PreserveOrder);
            KnownMirrorsToIgnore = RegisterDictionary<DuckDbUuid, byte>("known-mirror-ignore", PersistentDictionaryBehavior.KeySetOnly);
            ExternalPostIdHashToSyntheticTid = RegisterDictionary<DuckDbUuid, Tid>("external-post-id-to-synth-tid", PersistentDictionaryBehavior.SingleValue, caches: [new KeyProbabilisticCache<DuckDbUuid, Tid>(128 * 1024 * 1024, 3)]);
            SeenPosts = RegisterDictionary<Plc, PostEngagement>("seen-posts-2", onCompactation: CompactPostEngagements, caches: [UserPairEngagementCache], cachesAreMandatory: true);
            SeenPostsByDate = RegisterDictionary<Plc, TimePostSeen>("seen-posts-by-date");
            RssRefreshInfos = RegisterDictionary<Plc, byte>("rss-refresh-info", PersistentDictionaryBehavior.PreserveOrder);
            NostrSeenPubkeyHashes = RegisterDictionary<DuckDbUuid, byte>("nostr-seen-pubkey-hashes", PersistentDictionaryBehavior.KeySetOnly);
            ReposterOnlyProfile = RegisterDictionary<Plc, byte>("reposter-only-profile", PersistentDictionaryBehavior.KeySetOnly);
            OpenGraphData = RegisterDictionary<DuckDbUuid, byte>("opengraph", PersistentDictionaryBehavior.PreserveOrder);
            AccountStates = RegisterDictionary<Plc, byte>("account-state", PersistentDictionaryBehavior.SingleValue); // Enums don't implement IEquatable, so store the underlying type



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
                    item.AfterFlush += (_, _) => ClearLockLocalCache();
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

            if (AppViewLiteConfiguration.GetBool(AppViewLiteParameter.APPVIEWLITE_CHECK_NUL_FILES) ?? true)
            {
                var potentiallyCorrupt = AllMultidictionaries.SelectMany(x => x.GetPotentiallyCorruptFiles()).ToArray();
                if (potentiallyCorrupt.Length != 0)
                {
                    Log("The following slices begin or end with many NUL bytes and are possibly corrupt. This can happen after an abrupt system power-off. Consider manually reverting to a previous checkpoint file. This might also be a false positive (https://github.com/alnkesq/AppViewLite/issues/219). In such case, set APPVIEWLITE_CHECK_NUL_FILES to 0.");
                    foreach (var item in potentiallyCorrupt)
                    {
                        Log(item);
                    }
                    ThrowFatalError("Potentially corrupt slices were detected. See log for details.");
                }
            }

            GarbageCollectOldSlices(allowTempFileDeletion: true);
            UpdateAvailableDiskSpace();
            Log("Database loaded.");
        }

        private static void Migrate<TValue>(CombinedPersistentMultiDictionary<PostIdTimeFirst, TValue> newTable, CombinedPersistentMultiDictionary<PostId, TValue> oldTable) where TValue : unmanaged, IComparable<TValue>, IEquatable<TValue>
        {
            if (newTable.KeyCount != 0) return;
            Assert(newTable.Behavior == oldTable.Behavior);
            if (oldTable.KeyCount == 0) return; // new database, avoid log noise
            Log("Migrating: " + newTable.Name);
            if (newTable.Behavior == PersistentDictionaryBehavior.PreserveOrder)
            {
                foreach (var item in oldTable.EnumerateUnsortedGrouped())
                {
                    newTable.AddRange(item.Key, item.Values.AsSmallSpan());
                }
            }
            else
            {
                foreach (var item in oldTable.EnumerateUnsorted())
                {
                    newTable.Add(item.Key, item.Value);
                }
            }
            Log("  Migrated.");
        }

        public static IEnumerable<PostEngagement> CompactPostEngagements(IEnumerable<PostEngagement> enumerable)
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

        public bool IsDisposing { get; private set; }
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

                IsDisposing = true;
                if (!_disposed)
                {
                    foreach (var d in disposables)
                    {
                        if (IsReadOnly) d.DisposeNoFlush();
                        else d.Dispose();
                    }
                    CarRecordInsertionSemaphore?.Dispose();
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
                    _lockLocalCaches.Dispose();
                    _disposed = true;

                    if (IsPrimary)
                        Log("Disposed primary BlueskyRelationships.");

                    Interlocked.Decrement(ref AliveBlueskyRelationshipsDatabases);
                }

            }
            finally
            {

                Lock.ExitWriteLock();
                // Lock.Dispose(); // non-trivial to synchronize correctly
            }

        }

        public static int AliveBlueskyRelationshipsDatabases;

        public readonly static long WarnOnLowDiskSpace = (AppViewLiteConfiguration.GetInt64(AppViewLiteParameter.APPVIEWLITE_LOW_DISK_SPACE_WARNING_MB) ?? 4 * 1024) * 1024 * 1024;

        public event Action? BeforeCaptureCheckpoint;


        private void CaptureCheckpoint()
        {
            if (IsReadOnly) return;
            try
            {
                Log("Capturing checkpoint");
                BeforeCaptureCheckpoint?.Invoke();
                loadedCheckpoint!.Tables ??= new();
                loadedCheckpoint.FirehoseCursors = firehoseCursors!.Values.ToList();
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
                            return new GlobalCheckpointSlice()
                            {
                                StartTime = x.StartTime.Ticks,
                                EndTime = x.EndTime.Ticks,
                                PruneId = x.PruneId,
                            };
                        }).ToArray();
                    }
                }
                if (OperatingSystem.IsLinux())
                {
                    var handle = lockFile!.SafeFileHandle.DangerousGetHandle();
                    // First we flush all file data to physical disk, then we save the .pb checkpoint
                    if (syncfs((int)handle) < 0)
                        throw new Win32Exception("syncfs failed with errno " + Marshal.GetLastWin32Error());

                    // On Windows, FlushFileBuffers() can work:
                    // - on individual files, but could many individual writes increase write amplification?
                    //     - would this be mitigated by writing everything, then flushing everything?
                    // - on a whole volume, but it requires administrative privileges.
                }

                var now = DateTime.UtcNow;
                while (true)
                {

                    var checkpointFile = Path.Combine(BaseDirectory, "checkpoints", now.ToString("yyyyMMdd-HHmmss") + ".pb");
                    if (File.Exists(checkpointFile))
                    {
                        now = now.AddSeconds(1);
                        continue;
                    }
                    File.WriteAllBytes(checkpointFile + ".tmp", SerializeProto(loadedCheckpoint, x => x.Dummy = true));
                    File.Move(checkpointFile + ".tmp", checkpointFile);
                    break;
                }



                GarbageCollectOldSlices();
            }
            catch (Exception ex)
            {
                CombinedPersistentMultiDictionary.Abort(ex);
            }
        }

        [DllImport("libc.so.6", SetLastError = true)]
        public static extern int syncfs(int fd);

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
                .Select(x => (TableName: x.Key, SlicesToKeep: x.SelectMany(x => x.Slices ?? []).Select(x => x.ToSliceName()).ToHashSet()));
            foreach (var rootOrAdditionalDirectory in RootDirectoriesToGcCollect)
            {
                foreach (var table in keep)
                {
                    var directory = new DirectoryInfo(Path.Combine(rootOrAdditionalDirectory, table.TableName));
                    if (!directory.Exists) continue;
                    foreach (var file in directory.EnumerateFiles())
                    {
                        var name = file.Name;
                        if (name.EndsWith(".tmp", StringComparison.Ordinal))
                        {
                            if (allowTempFileDeletion) file.Delete();
                            else continue; // might be a parallel compactation
                        }

                        var dot = name.IndexOf('.');
                        if (dot != -1) name = name.Substring(0, dot);

                        var sliceName = SliceName.ParseBaseName(name);
                        if (!table.SlicesToKeep.Contains(sliceName))
                        {
                            LogInfo("Deleting obsolete slice: " + file.FullName);
                            try
                            {
                                file.Delete();
                            }
                            catch (Exception ex)
                            {
                                LogNonCriticalException($"Deletion of old slice {file.FullName} failed", ex);
                            }
                        }
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
            return TryGetDid(plc, allowMissingIfReplica: false)!;
        }
        public string? TryGetDid(Plc plc, bool allowMissingIfReplica = false)
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
                if (allowMissingIfReplica && IsReplica) return null;
                throw new Exception("Missing DID string for " + plc);

            }
            //if (SerializeDid(did) != plc) CombinedPersistentMultiDictionary.Abort(new Exception("Did serialization did not roundtrip for " + plc + "/" + did));
            PlcToDidConcurrentCache[plc] = did;
            return did;
        }


        public Plc SerializeDidWithHint(string did, RequestContext ctx, Plc optionalHint)
        {
            if (optionalHint != default) return optionalHint;
            var result = SerializeDid(did, ctx);
            // Assert(optionalHint == default || optionalHint == result);
            return result;
        }

        public Plc SerializeDid(string did, RequestContext ctx)
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
                plc = AddDidPlcMappingCore(did, hash);
            }, ctx);


            DidToPlcConcurrentCache[hash] = plc;
            return plc;

        }

        internal Plc AddDidPlcMappingCore(string did, DuckDbUuid hash)
        {
            Plc plc = new Plc(checked(LastAssignedPlc.PlcValue + 1));
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
                using var _ = BlueskyRelationshipsClientBase.CreateNormalPriorityScope();
                Lock.EnterWriteLock();
                try
                {
                    Version++;
                    ctx.BumpMinimumVersion(Version);
                    value();
                }
                finally
                {
                    Lock.ExitWriteLock();
                }
            }
        }


        public static PostId GetPostId(Plc Plc, string rkey)
        {
            return new PostId(Plc, Tid.Parse(rkey));
        }
        public PostId GetPostId(StrongRef subject, RequestContext ctx, bool ignoreIfNotPost = false, Plc hint = default)
        {
            var uri = subject.Uri;
            if (uri.Collection != Post.RecordType)
            {
                if (ignoreIfNotPost) return default;
                throw new UnexpectedFirehoseDataException("Unexpected URI type: " + uri.Collection);
            }
            return new PostId(SerializeDidWithHint(uri.Did!.Handler, ctx, hint), Tid.Parse(uri.Rkey));
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

        public BlueskyProfileBasicInfo? GetProfileBasicInfo(Plc plc, bool canOmitDescription = false)
        {
            if (Profiles.TryGetPreserveOrderSpanLatest(plc, out var arr))
            {
                var span = arr.AsSmallSpan();
                var proto = DeserializeProto<BlueskyProfileBasicInfo>(arr.AsSmallSpan());

                EfficientTextCompressor.DecompressInPlace(ref proto.DisplayName, ref proto.DisplayNameBpe);
                if (!canOmitDescription)
                    EfficientTextCompressor.DecompressInPlace(ref proto.Description, ref proto.DescriptionBpe);

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


        public BlueskyPostData? StorePostInfoExceptData(Post p, PostId postId, RequestContext ctx)
        {
            if (postId == default) throw AssertionLiteException.Throw("StorePostInfoExceptData postId is default");
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
            }
            if (p.Reply?.Parent is { } parent)
            {
                var inReplyTo = this.GetPostId(parent, ctx);
                proto.InReplyToPlc = inReplyTo.Author.PlcValue;
                proto.InReplyToRKey = inReplyTo.PostRKey.TidValue;
                this.DirectReplies.Add(inReplyTo, postId);

                var notifiedAncestors = new HashSet<Plc>();

                var rootPostId = proto.RootPostId;
                if (rootPostId != postId && rootPostId != inReplyTo)
                {
                    // Reply to a non-root post.

                    AddNotification(inReplyTo.Author, NotificationKind.RepliedToYourPost, postId, ctx, postId.PostRKey.Date);
                    notifiedAncestors.Add(inReplyTo.Author);

                    var ancestor = inReplyTo;
                    var iterations = 0;
                    while (true)
                    {
                        if (iterations++ == 30) break;
                        var a = TryGetPostData(ancestor, skipBpeDecompression: true, ignoreDeletions: true);
                        if (a == null)
                        {
                            // We don't have all the data for all the intermediate posts.
                            // At least let's notify the root post.
                            if (notifiedAncestors.Add(rootPostId.Author))
                                AddNotification(rootPostId.Author, NotificationKind.RepliedToYourThread, postId, ctx, postId.PostRKey.Date);
                            break;
                        }

                        if (a.InReplyToPostId is { } nextAncestor)
                        {
                            ancestor = nextAncestor;
                            var hasMoreAncestors = ancestor != rootPostId;
                            if (notifiedAncestors.Add(ancestor.Author))
                                AddNotification(ancestor.Author, hasMoreAncestors ? NotificationKind.RepliedToADescendant : NotificationKind.RepliedToYourThread, postId, ctx, postId.PostRKey.Date);
                            if (!hasMoreAncestors) break;
                        }
                        else
                        {
                            Log("Retrieved ancestor is a root post, why didn't we stop earlier? Leaf: " + postId);
                            break;
                        }
                    }
                }
                else
                {
                    // Direct reply to a root post, no lookups needed.
                    AddNotification(inReplyTo.Author, NotificationKind.RepliedToYourPost, postId, ctx, postId.PostRKey.Date);
                }

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
                    AddNotification(quoted.Author, NotificationKind.QuotedYourPost, postId, ctx, postId.PostRKey.Date);
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
                    AddNotification(quoted.Author, NotificationKind.QuotedYourPost, postId, ctx, postId.PostRKey.Date);
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
                        AddNotification(plc, NotificationKind.MentionedYou, postId, ctx, postId.PostRKey.Date);
                    }
                }
            }

            if (proto.Media != null)
                UserToRecentMediaPosts.Add(postId.Author, postId.PostRKey);
            UserToRecentPosts.Add(postId.Author, new RecentPost(postId.PostRKey, proto.IsReplyToUnspecifiedPost == true ? Plc.MaxValue : new Plc(proto.InReplyToPlc.GetValueOrDefault())));

            if (proto.QuotedRKey != null)
                NotifyPostStatsChange(proto.QuotedPostId!.Value, postId.Author);

            AddPostToRecentPostCache(postId.Author, new UserRecentPostWithScore(postId.PostRKey, proto.InReplyToPostId?.Author ?? default, 0));

            return proto;
        }

        private readonly static HashedWord[] LanguageEnumToHashedSearchIndex = Enumerable.Range(0, (short)Enum.GetValues<LanguageEnum>().Max() + 1).Select(x => HashWord("%lang-" + ((LanguageEnum)x).ToString())).ToArray();

        public void IndexPost(BlueskyPostData proto)
        {
            var postId = proto.PostId;
            if (postId.PostRKey.Date < ApproximateDateTime32.MinValueAsDateTime) return;
            var approxPostDate = GetApproxTime32(postId.PostRKey);

            if (proto.Language != LanguageEnum.Unknown)
            {
                PostTextSearch.Add(LanguageEnumToHashedSearchIndex[(int)proto.Language!.Value], approxPostDate);
            }
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



        public List<DangerousHugeReadOnlyMemory<Plc>> SearchProfilesPrefixOnly(SizeLimitedWord8 prefix)
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

        private static List<DangerousHugeReadOnlyMemory<Plc>> ConsolidatePrefixSearch(IEnumerable<DangerousHugeReadOnlyMemory<Plc>> slices)
        {
            var result = new List<DangerousHugeReadOnlyMemory<Plc>>();
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
                result.Add(CombinedPersistentMultiDictionary.ToNativeArray(small));
            }

            return result;
        }

        public IEnumerable<Plc> SearchProfiles(string[] searchTerms, SizeLimitedWord8 searchTermsLastPrefix, Plc maxExclusive, bool alsoSearchDescriptions)
        {
            var searchTermsArray = searchTerms.Select(x => HashWord(x)).Distinct().ToArray();

            var toIntersect = new List<List<DangerousHugeReadOnlyMemory<Plc>>>();

            foreach (var word in searchTerms)
            {
                var slices = new List<DangerousHugeReadOnlyMemory<Plc>>();
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

            if (words.Length == 0) throw AssertionLiteException.Throw("words.Length is zero");

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
                searchTermsArray = [.. searchTermsArray, HashPlcForTextSearch(author)];
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

        private static bool RemoveEmptySearchSlices<T>((long TotalCount, List<DangerousHugeReadOnlyMemory<T>> Slices)[] words, T approxDate) where T : unmanaged, IEquatable<T>
        {
            var firstWord = words[0].Slices;
            var sliceIndex = firstWord.FindIndex(x => x[x.Count - 1].Equals(approxDate));
            if (sliceIndex == -1) AssertionLiteException.Throw("RemoveEmptySearchSlices already empty");
            firstWord[sliceIndex] = firstWord[sliceIndex].Slice(0, firstWord[sliceIndex].Count - 1);
            firstWord.RemoveAll(x => x.Count == 0);
            return firstWord.Count != 0;
        }

        private static void PeelUntilNextCommonPost<T>((long TotalCount, List<DangerousHugeReadOnlyMemory<T>> Slices)[] words, ref T mostRecentCommonPost) where T : unmanaged, IComparable<T>
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

        private static void TrimAwayPostsAboveThreshold<T>(List<DangerousHugeReadOnlyMemory<T>> slices, T maxPost) where T : unmanaged, IComparable<T>
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

            EfficientTextCompressor.CompressInPlace(ref proto.Description, ref proto.DescriptionBpe);
            EfficientTextCompressor.CompressInPlace(ref proto.DisplayName, ref proto.DisplayNameBpe);

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
                ProfileSearchDescriptionOnly.Add(hash, plc);
            }
        }

        private readonly SizeLimitedWord8 SizeLimitedWord8_bsky = SizeLimitedWord8.Create("bsky", out _);
        private readonly SizeLimitedWord2 SizeLimitedWord2_bsky = SizeLimitedWord2.Create("bsky");
        private readonly SizeLimitedWord8 SizeLimitedWord8_social = SizeLimitedWord8.Create("social", out _);
        private readonly SizeLimitedWord2 SizeLimitedWord2_social = SizeLimitedWord2.Create("social");


        internal void IndexProfileWord(string word, Plc plc)
        {

            if (word == "bsky")
            {
                ProfileSearchPrefix8.Add(SizeLimitedWord8_bsky, plc);
                ProfileSearchPrefix2.Add(SizeLimitedWord2_bsky, plc);
                return;
            }
            if (word == "social")
            {
                ProfileSearchPrefix8.Add(SizeLimitedWord8_social, plc);
                ProfileSearchPrefix2.Add(SizeLimitedWord2_social, plc);
                return;
            }


            Assert(word.Length != 0);
            var hash = HashWord(word);

            var wordUtf8 = Encoding.UTF8.GetBytes(word);

            ProfileSearchPrefix8.Add(SizeLimitedWord8.Create(wordUtf8, out var truncated), plc);
            if (truncated)
            {
                ProfileSearchLong.Add(hash, plc);
            }
            ProfileSearchPrefix2.Add(SizeLimitedWord2.Create(wordUtf8), plc);
        }

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
                //    throw new Exception("Bad roundtrip for SerializePostData");
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

            EfficientTextCompressor.CompressInPlace(ref proto.Text, ref proto.TextBpe);
            EfficientTextCompressor.CompressInPlace(ref proto.ExternalTitle, ref proto.ExternalTitleBpe);
            EfficientTextCompressor.CompressInPlace(ref proto.ExternalDescription, ref proto.ExternalDescriptionBpe);
            EfficientTextCompressor.CompressInPlace(ref proto.ExternalUrl, ref proto.ExternalUrlBpe);
            if (proto.Media != null)
            {
                foreach (var media in proto.Media)
                {
                    EfficientTextCompressor.CompressInPlace(ref media.AltText, ref media.AltTextBpe);
                }
            }
            if (proto.Facets != null)
            {
                foreach (var facet in proto.Facets)
                {
                    EfficientTextCompressor.CompressInPlace(ref facet.Link, ref facet.LinkBpe);
                    EfficientTextCompressor.CompressInPlace(ref facet.InlineImageUrl, ref facet.InlineImageUrlBpe);
                    EfficientTextCompressor.CompressInPlace(ref facet.InlineImageAlt, ref facet.InlineImageAltBpe);
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

                EfficientTextCompressor.DecompressInPlace(ref proto.Text, ref proto.TextBpe);
                EfficientTextCompressor.DecompressInPlace(ref proto.ExternalTitle, ref proto.ExternalTitleBpe);
                EfficientTextCompressor.DecompressInPlace(ref proto.ExternalDescription, ref proto.ExternalDescriptionBpe);
                EfficientTextCompressor.DecompressInPlace(ref proto.ExternalUrl, ref proto.ExternalUrlBpe);
                if (proto.Media != null)
                {
                    foreach (var media in proto.Media)
                    {
                        EfficientTextCompressor.DecompressInPlace(ref media.AltText, ref media.AltTextBpe);
                    }
                }
                if (proto.Facets != null)
                {
                    foreach (var facet in proto.Facets)
                    {
                        EfficientTextCompressor.DecompressInPlace(ref facet.Link, ref facet.LinkBpe);
                        EfficientTextCompressor.DecompressInPlace(ref facet.InlineImageUrl, ref facet.InlineImageUrlBpe);
                        EfficientTextCompressor.DecompressInPlace(ref facet.InlineImageAlt, ref facet.InlineImageAltBpe);
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
        public BlueskyPost[] GetPostQuotes(Plc plc, string rkey, PostId continuation, int limit, RequestContext? ctx = null)
        {
            return Quotes.GetValuesSorted(GetPostId(plc, rkey), continuation).Take(limit).Select(x => GetPost(x, ctx)).ToArray();
        }

        public BlueskyPost GetPost(string did, string rkey, RequestContext ctx)
        {
            return GetPost(GetPostId(did, rkey, ctx));
        }
        public BlueskyPost GetPost(Plc plc, Tid rkey, RequestContext? ctx = null)
        {
            return GetPost(new PostId(plc, rkey), ctx);
        }
        public BlueskyPost GetPost(ATUri uri, RequestContext ctx)
        {
            return GetPost(GetPostId(uri, ctx), ctx);
        }

        public bool IsThreadReplyFullyVisible(BlueskyPost post, BlueskyThreadgate? threadgate, RequestContext ctx)
        {
            if (post.Data is { Deleted: true }) return false;
            if (threadgate != null)
            {
                if (!ThreadgateAllowsUser(post.RootPostId, threadgate, post.AuthorId)) return false;
                if (threadgate.IsHiddenReply(post.PostId)) return false;
            }
            return true;
        }

        public BlueskyPost GetPost(PostId id, RequestContext? ctx = null)
        {
            var post = GetPostWithoutData(id, ctx);
            (post.Data, post.InReplyToUser, post.RootPostDid) = TryGetPostDataAndInReplyTo(id, ctx);
            MaybePropagateAdministrativeBlockToPost(post);

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
            if (post.PluggableProtocol?.RequiresLateOpenGraphData(post) == true)
            {
                post.ApplyLateOpenGraphData(GetOpenGraphData(post.ExternalLinkOrFirstLinkFacet!));
            }
            MaybePropagateAdministrativeBlockToPost(post);
            return post;
        }

        public static void MaybePropagateAdministrativeBlockToPost(BlueskyPost post)
        {
            if (!post.Author.IsActive)
            {
                post.Data = CloneWithError(post.Data, post.Author.BasicData!.Error!);
            }
            if (post.Author.IsMediaBlockedByAdministrativeRule && post.Data != null)
            {
                RemoveCustomEmojiFacets(ref post.Data.Facets);
            }
        }

        public BlueskyPost GetPost(PostId id, BlueskyPostData? data, RequestContext? ctx = null)
        {
            var post = GetPostWithoutData(id, ctx);
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

        public BlueskyPost GetPostWithoutData(PostId id, RequestContext? ctx = null)
        {
            return new BlueskyPost
            {
                Author = GetProfile(id.Author, ctx, canOmitDescription: true),
                RKey = id.PostRKey.ToString()!,
                LikeCount = Likes.GetActorCount(id),
                QuoteCount = Quotes.GetValueCount(id),
                ReplyCount = DirectReplies.GetValueCount(id),
                RepostCount = Reposts.GetActorCount(id),
                Date = DateTime.UnixEpoch.AddMicroseconds(id.PostRKey.Timestamp),
                PostId = id,
            };
        }

        internal (BlueskyPostData? Data, BlueskyProfile? InReplyTo, string? RootPostDid) TryGetPostDataAndInReplyTo(PostId id, RequestContext? ctx = null)
        {
            var d = TryGetPostData(id);
            if (d == null) return default;
            if (d.InReplyToPlc == null) return (d, null, null);
            var inReplyTo = GetProfile(new Plc(d.InReplyToPlc.Value), ctx: ctx, canOmitDescription: true);
            return (d, inReplyTo, GetDid(d.RootPostId.Author));

        }

        public BlueskyPostData? TryGetPostData(PostId id, bool skipBpeDecompression = false, bool ignoreDeletions = false)
        {
            var isDeleted = ignoreDeletions ? false : PostDeletions.ContainsKey(id);

            BlueskyPostData? proto = null;

            // latest instead of any (pluggable posts include their own like count)
            if (PostData.TryGetPreserveOrderSpanLatest(id, out var postDataCompressed))
            {
                proto = DeserializePostData(postDataCompressed.AsSmallSpan(), id, skipBpeDecompression: skipBpeDecompression);
            }
            else if (!isDeleted && FailedPostLookups.ContainsKey(id))
            {
                proto = new BlueskyPostData { Error = "This post could not be retrieved." };
            }

            if (isDeleted)
            {
                return CloneWithError(proto, "This post was deleted.");
            }

            return proto;
        }

        public static BlueskyPostData CloneWithError(BlueskyPostData? proto, string error)
        {
            return new BlueskyPostData
            {
                Deleted = true,
                Error = error,
                RootPostPlc = proto?.RootPostPlc,
                RootPostRKey = proto?.RootPostRKey,
                InReplyToPlc = proto?.InReplyToPlc,
                InReplyToRKey = proto?.InReplyToRKey,
            };
        }

        public static BlueskyPostData DeserializePostData(ReadOnlySpan<byte> postDataCompressed, PostId postId, bool skipBpeDecompression = false)
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
        public BlueskyProfile GetProfile(Plc plc, RequestContext? ctx, bool canOmitDescription = false)
        {
            return GetProfile(plc, relationshipRKey: null, ctx: ctx, canOmitDescription: canOmitDescription);
        }


        public BlueskyProfile GetProfile(Plc plc, Tid? relationshipRKey = null, RequestContext? ctx = null, bool canOmitDescription = false)
        {
            if (relationshipRKey != null) ctx = null; // we cannot cache
            if (ctx?.ProfileCache?.TryGetValue(plc, out var cached) == true && (canOmitDescription || !cached.DidOmitDescription)) return cached;
            var basic = GetProfileBasicInfo(plc, canOmitDescription: canOmitDescription);
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

            var accountState = GetAccountState(plc);
            if (isBlockedByAdministrativeRule)
            {
                accountState = AccountState.DisabledByAppViewLiteAdministrativeRules;
            }

            if (!IsAccountActive(accountState))
            {
                basic = new BlueskyProfileBasicInfo
                {
                    Error = DefaultLabels.GetErrorForAccountState(accountState, didDoc?.Pds)
                };
            }

            if (isMediaBlockedByAdministrativeRule && basic != null)
            {
                RemoveCustomEmojiFacets(ref basic.DisplayNameFacets);
                RemoveCustomEmojiFacets(ref basic.DescriptionFacets);
                basic.AvatarCidBytes = null;
                basic.BannerCidBytes = null;
            }

            var result = new BlueskyProfile()
            {
                AccountState = accountState,
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
                DidOmitDescription = canOmitDescription,
            };
            if (ctx?.ProfileCache is { } dict)
                dict[plc] = result;

            return result;
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
                var proto = DidDocProto.DeserializeFromBytes(bytes.AsSmallSpan())!;
                DecompressDidDoc(proto);
                return proto;
            }
            return null;
        }

        public BlockReason GetBlockReason(Plc plc, RequestContext ctx)
        {
            return ctx.IsLoggedIn == true ? UsersHaveBlockRelationship(ctx.LoggedInUser, plc, ctx) : default;
        }

        public PostId GetPostId(string did, string rkey, RequestContext ctx)
        {
            return new PostId(SerializeDid(did, ctx), Tid.Parse(rkey));
        }
        public PostId GetPostId(ATUri uri, RequestContext ctx)
        {
            if (uri.Collection != Post.RecordType) throw new ArgumentException("GetPostId: not an app.bsky.feed.post URI");
            return GetPostId(uri.Did!.ToString(), uri.Rkey, ctx);
        }

        public BlueskyProfile[] GetFollowers(Plc plc, Relationship continuation, int limit)
        {
            return Follows.GetRelationshipsSorted(plc, continuation).Take(limit).Select(x => GetProfile(x.Actor, x.RelationshipRKey)).ToArray();
        }

        public BlueskyProfile[] GetBlockedBy(Plc plc, Relationship continuation, int limit)
        {
            return Blocks.GetRelationshipsSorted(plc, continuation).Take(limit).Select(x => GetProfile(x.Actor, x.RelationshipRKey)).ToArray();
        }

        public BlueskyFullProfile GetFullProfile(Plc plc, RequestContext ctx, int followersYouFollowToLoad)
        {
            return new BlueskyFullProfile
            {
                Profile = GetProfile(plc, ctx),
                Followers = Follows.GetActorCount(plc),
                FollowedByPeopleYouFollow = ctx.IsLoggedIn && followersYouFollowToLoad != 0 ? GetFollowersYouFollow(plc, ctx.LoggedInUser)?.Select((x, i) => i < followersYouFollowToLoad ? GetProfile(x, ctx, canOmitDescription: true) : new BlueskyProfile { PlcId = x.PlcValue, Did = null! }).ToList() : null,
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
            var page = r.Select(x => GetProfile(x, ctx)).ToArray();
            var end = offset + page.Length;
            return new ProfilesAndContinuation(page, end < everything.Length ? end.ToString() : null);
        }

        private Plc[] GetFollowersYouFollow(Plc plc, Plc loggedInUser)
        {
            var myFolloweesChunked = RegisteredUserToFollowees.GetValuesChunked(loggedInUser).ToArray();
            var followerChunks = Follows.creations.GetValuesChunked(plc).ToArray();

            var totalMyFollowees = myFolloweesChunked.Sum(x => x.Length);
            var totalFollowers = followerChunks.Sum(x => x.Length);

            var popularityRatio = (double)totalFollowers / totalMyFollowees;

            var myFollowees = SimpleJoin.ConcatPresortedEnumerablesKeepOrdered(myFolloweesChunked.Select(x => x.AsEnumerable()).ToArray(), x => x.Member)
                .DistinctByAssumingOrderedInputLatest(x => x.Member);


            if (
                (popularityRatio > 100 || totalMyFollowees < 10) /* vibes only, not checked empirically */ &&
                Follows.RelationshipCache != null /* too slow otherwise */)
            {

                var result = myFollowees
                    .Where(x => Follows.HasActor(plc, x.Member, out _))
                    .Where(x => !Follows.IsDeleted(new Relationship(x.Member, x.ListItemRKey)))
                    .Select(x => x.Member)
                    .ToArray();
                return result;
            }




            var followers =
                SimpleJoin.ConcatPresortedEnumerablesKeepOrdered(followerChunks.Select(x => x.AsEnumerable()).ToArray(), x => x)
                .DistinctByAssumingOrderedInputLatest(x => x.Actor);

            var joined = SimpleJoin.JoinPresortedAndUnique(myFollowees, x => x.Member, followers, x => x.Actor);

            return joined
                .Where(x => x.Left != default && x.Right != default && !Follows.IsDeleted(new Relationship(loggedInUser, x.Left.ListItemRKey)) && !Follows.IsDeleted(x.Right))
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


        internal IEnumerable<BlueskyPost> GetFirehosePosts(CombinedPersistentMultiDictionary<PostIdTimeFirst, byte>.SliceInfo slice, PostIdTimeFirst? maxPostIdExlusive, DateTime now, RequestContext? ctx = null)
        {
            var index = maxPostIdExlusive != null ? slice.Reader.BinarySearch(maxPostIdExlusive.Value) : (slice.Reader.KeyCount + 1);
            if (index >= 0) index--;
            else index = ~index;
            for (long i = index - 1; i >= 0; i--)
            {
                var postId = slice.Reader.Keys[i];
                if (postId.PostRKey.Date > now) continue;
                if (PostDeletions.ContainsKey(postId)) continue;
                var postData = DeserializePostData(slice.Reader.GetValues(i, PostData.GetIoPreferenceForKey(postId)).Span.AsSmallSpan(), postId);
                yield return GetPost(postId, postData, ctx);
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



        internal ReadOnlySpan<byte> SerializeThreadgateToBytes(Threadgate threadGate, RequestContext ctx, out BlueskyThreadgate proto)
        {
            proto = new BlueskyThreadgate
            {
                HiddenReplies = threadGate.HiddenReplies?.Select(x => RelationshipProto.FromPostId(GetPostId(x, ctx))).ToArray(),
                AllowlistedOnly = threadGate.Allow != null,
                AllowFollowing = threadGate.Allow?.Any(x => x is FollowingRule) ?? false,
                AllowFollowers = threadGate.Allow?.Any(x => x is FollowerRule) ?? false,
                AllowMentioned = threadGate.Allow?.Any(x => x is MentionRule) ?? false,
                AllowLists = threadGate.Allow?.OfType<ListRule>().Select(x =>
                {
                    return new RelationshipProto { Plc = SerializeDid(x.List.Did!.Handler, ctx).PlcValue, Tid = Tid.Parse(x.List.Rkey).TidValue };
                }).ToArray()
            };
            return SerializeProto(proto, x => x.Dummy = true);
        }
        internal ReadOnlySpan<byte> SerializePostgateToBytes(Postgate postgate, RequestContext ctx, out BlueskyPostgate proto)
        {
            proto = new BlueskyPostgate
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
                if (setDummyValue == null) throw AssertionLiteException.Throw("Cannot serialize zero-length-serializing protos unless setDummyValue is provided.");
                setDummyValue(proto);
                ProtoBuf.Serializer.Serialize(protoMs, proto);
                if (protoMs.Length == 0) AssertionLiteException.Throw("Proto is still zero bytes after setDummyValue");
            }
            return protoMs.ToArray();
        }

        public bool UserDirectlyBlocksUser(Plc blocker, Plc blockee)
        {
            return Blocks.HasActor(blockee, blocker, out _);
        }

        public BlockReason UserBlocksUser(Plc blocker, Plc blockee, RequestContext ctx)
        {
            if (UserDirectlyBlocksUser(blocker, blockee)) return new BlockReason(BlockReasonKind.Blocks, default);

            foreach (var subscription in GetSubscribedBlockLists(blocker, ctx))
            {
                if (IsMemberOfList(subscription, blockee)) return new BlockReason(BlockReasonKind.Blocks, subscription);
            }
            return default;
        }

        public BlockReason UsersHaveBlockRelationship(Plc a, Plc b, RequestContext ctx)
        {
            if (!ctx.BlockReasonCache.TryGetValue((a, b), out var result))
            {
                result = UsersHaveBlockRelationshipCore(a, b, ctx);
                ctx.BlockReasonCache[(a, b)] = result;
            }
            return result;
        }
        public BlockReason UsersHaveBlockRelationshipCore(Plc a, Plc b, RequestContext ctx)
        {
            if (a == b) return default;
            var direct = UserBlocksUser(a, b, ctx);
            var inverse = UserBlocksUser(b, a, ctx);
            var directKind = direct.Kind;
            var inverseKind = inverse.Kind;

            if (directKind == default && inverseKind == default) return default;

            if (directKind != default && inverseKind != default)
            {
                return new BlockReason(BlockReasonKind.MutualBlock, direct.List);
            }

            if (directKind != default) return direct;
            if (inverseKind != default) return new BlockReason(BlockReasonKind.BlockedBy, inverse.List);

            throw AssertionLiteException.Throw("UsersHaveBlockRelationship impossible case");
        }

        internal void ClearLockLocalCache()
        {
            try
            {
                _lockLocalCaches.Value = new();
            }
            catch (ObjectDisposedException)
            {
                // Already disposed, and our callees (WithRelationshipsXxxxLock) must not fail (they must release the locks).
                // Subsequent reads from _lockLocalCaches.Value will fail anyway, no need to worry about stale data in them
            }
        }

        private readonly ThreadLocal<LockLocalCaches> _lockLocalCaches = new();

        public LockLocalCaches LockLocalCaches
        {
            get
            {
                var value = _lockLocalCaches.Value;
                if (value == null)
                {
                    value = new();
                    _lockLocalCaches.Value = value;
                }
                return value;
            }
        }

        public bool IsMemberOfList(Relationship list, Plc member)
        {
            if (ListItems.GetDelegateProbabilisticCache<(Relationship, Plc)>()?.PossiblyContains((list, member)) == false)
            {
                return false;
            }



            foreach (var memberChunk in LockLocalCaches.ListMembers.GetOrFetch(list, () => ListItems.GetValuesChunked(list).ToArray()))
            {
                var members = memberChunk.AsSpan();
                var index = members.BinarySearch(new ListEntry(member, default));
                if (index >= 0) AssertionLiteException.Throw("Approximate item should not have been found.");

                index = ~index;

                for (long i = index; i < members.Length; i++)
                {
                    var entry = members[i];
                    if (entry.Member != member) break;

                    var listItem = new Relationship(list.Actor, entry.ListItemRKey);
                    if (ListItemDeletions.ContainsKey(listItem))
                        continue;


                    if (ListDeletions.ContainsKey(list))
                        return false;
                    return true;
                }
            }

            return false;
        }

        public List<Relationship> GetSubscribedBlockLists(Plc subscriber, RequestContext ctx)
        {
            if (!ctx.SubscribedBlocklistsCache.TryGetValue(subscriber, out var result))
            {
                result = GetSubscribedBlockListsCore(subscriber);
                ctx.SubscribedBlocklistsCache[subscriber] = result;
            }
            return result;
        }
        private List<Relationship> GetSubscribedBlockListsCore(Plc subscriber)
        {
            if (ListBlocks.GetDelegateProbabilisticCache<Plc>()?.PossiblyContains(subscriber) == false) return [];
            var lists = new List<Relationship>();

            foreach (var (subscriptionId, singleList) in ListBlocks.GetInRangeUnsorted(new Relationship(subscriber, default), new Relationship(subscriber.GetNext(), default)))
            {
                if (ListBlockDeletions.ContainsKey(subscriptionId))
                    continue;

                if (singleList.Count != 1) AssertionLiteException.Throw("GetSubscribedBlockLists deletion should've been SingleValue.");

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

        public bool HasRecentNotification(Plc destination, NotificationKind kind, Plc actor, Tid rkey, ApproximateDateTime32 minDate)
        {
            foreach (var chunk in GetNotificationTable(IsDarkNotification(kind)).GetValuesChunked(destination, new Notification(minDate.AddTicks(-1), default, default, default)))
            {
                foreach (var notif in chunk)
                {
                    if (notif.Actor == actor && notif.Kind == kind && notif.RKey == rkey)
                        return true;
                }
            }
            return false;
        }

        public bool IsRegisteredForNotifications(Plc user)
        {
            return registerForNotificationsCache.Contains(user);
        }
        public void AddNotificationDateInvariant(Plc destination, NotificationKind kind, Plc actor, Tid rkey, RequestContext ctx, DateTime date, DateTime minDate)
        {
            if (!IsRegisteredForNotifications(destination)) return;

            if (date < minDate) minDate = date;
            if (HasRecentNotification(destination, kind, actor, rkey, (ApproximateDateTime32)minDate)) return;
            AddNotification(destination, kind, actor, rkey, ctx, date);
        }
        public void AddNotification(PostId destination, NotificationKind kind, Plc actor, RequestContext ctx, DateTime date)
        {
            AddNotification(destination.Author, kind, actor, destination.PostRKey, ctx, date);
        }
        public void AddNotification(Plc destination, NotificationKind kind, Plc actor, RequestContext ctx, DateTime date)
        {
            AddNotification(destination, kind, actor, default, ctx, date);
        }
        public void AddNotification(Plc destination, NotificationKind kind, PostId replyId, RequestContext ctx, DateTime date)
        {
            AddNotification(destination, kind, replyId.Author, replyId.PostRKey, ctx, date);
        }
        public void AddNotification(Plc destination, NotificationKind kind, Plc actor, Tid rkey, RequestContext ctx, DateTime date)
        {
            if (destination == actor) return;
            if (!IsRegisteredForNotifications(destination)) return;
            var notification = new Notification((ApproximateDateTime32)date, actor, rkey, kind);
            var dark = IsDarkNotification(kind);
            var table = GetNotificationTable(dark);
            if (table.Contains(destination, notification)) return;

            if (!dark && UsersHaveBlockRelationship(destination, actor, ctx) != default) return;
            table.Add(destination, notification);
            if (!dark)
                UserNotificationSubscribersThreadSafe.MaybeFetchDataAndNotifyOutsideLock(destination, () => GetNotificationCount(destination, dark: false), (data, handler) => handler(data));

            // Callback must stay inside the lock.
            NotificationGenerated?.Invoke(destination, notification, ctx);
        }


        public CombinedPersistentMultiDictionary<Plc, Notification> GetNotificationTable(bool dark) => dark ? DarkNotifications : Notifications;
        public CombinedPersistentMultiDictionary<Plc, Notification> GetLastSeenNotificationTable(bool dark) => dark ? LastSeenDarkNotifications : LastSeenNotifications;

        public static bool IsDarkNotification(NotificationKind kind)
        {
            return kind >= NotificationKind.DarkNotificationBase;
        }

        public event Action<Plc, Notification, RequestContext>? NotificationGenerated;



        public BlueskyNotification? RehydrateNotification(Notification notification, Plc destination, RequestContext ctx, Dictionary<PostId, BlueskyPost> postCache)
        {
            (PostId postId, Plc actor) = notification.Kind switch
            {
                NotificationKind.FollowedYou or NotificationKind.FollowedYouBack or NotificationKind.UnfollowedYou or NotificationKind.BlockedYou or NotificationKind.LabeledYourProfile => (default, notification.Actor),
                NotificationKind.LikedYourPost or NotificationKind.RepostedYourPost or NotificationKind.DetachedYourQuotePost or NotificationKind.HidYourReply or NotificationKind.LabeledYourPost => (new PostId(destination, notification.RKey), notification.Actor),
                NotificationKind.RepliedToYourPost or NotificationKind.RepliedToYourThread or NotificationKind.QuotedYourPost or NotificationKind.RepliedToADescendant => (new PostId(notification.Actor, notification.RKey), notification.Actor),
                _ => default
            };

            if (notification.Kind is NotificationKind.LikedYourRepost or NotificationKind.RepostedYourRepost)
            {
                var repostRkey = notification.RKey;
                postId = this.UserToRecentReposts.GetValuesUnsorted(ctx.LoggedInUser, new RecentRepost(repostRkey, default), new RecentRepost(repostRkey.GetNext(), default))
                    .FirstOrDefault(x => x.RepostRKey == repostRkey)
                    .PostId;
                if (postId == default) return null;
                actor = notification.Actor;
            }

            BlueskyFeedGenerator? feed = null;
            BlueskyList? list = null;
            if (notification.Kind == NotificationKind.LikedYourFeed)
            {
                feed = TryGetFeedGenerator(new RelationshipHashedRKey(destination, (ulong)notification.RKey.TidValue), ctx);
                actor = notification.Actor;
                postId = default;
            }
            if (notification.Kind == NotificationKind.AddedYouToAList)
            {
                list = GetList(new Relationship(notification.Actor, notification.RKey), ctx: ctx);
                actor = notification.Actor;
                postId = default;
            }

            if (postId == default && actor == default && feed == null && list == null) return null;

            BlueskyPost? post = null;
            if (postId != default)
            {
                if (!postCache.TryGetValue(postId, out post))
                {
                    post = GetPost(postId, ctx);
                    postCache.Add(postId, post);
                }
            }
            return new BlueskyNotification
            {
                EventDate = notification.EventDate,
                Kind = notification.Kind,
                Post = post,
                Profile = actor != default ? GetProfile(actor, ctx, canOmitDescription: true) : default,
                Hidden = !IsDarkNotification(notification.Kind) && actor != default && UsersHaveBlockRelationship(destination, actor, ctx) != default,
                NotificationCore = notification,
                Feed = feed,
                List = list,
            };



        }

        public long GetNotificationCount(Plc user, bool dark)
        {
            if (!GetLastSeenNotificationTable(dark).TryGetLatestValue(user, out var threshold)) return 0;

            long count = 0;
            foreach (var chunk in GetNotificationTable(dark).GetValuesChunked(user, threshold))
            {
                count += chunk.Count;
            }
            return count;
        }

        public (BlueskyNotification[] NewNotifications, BlueskyNotification[] OldNotifications, Notification NewestNotification) GetNotificationsForUser(Plc user, RequestContext ctx, bool dark)
        {
            if (!LastSeenNotifications.TryGetLatestValue(user, out var threshold)) return ([], [], default); // on user registration, only last seen main notification table is initialized.
            if (dark)
                LastSeenDarkNotifications.TryGetLatestValue(user, out threshold);

            var postCache = new Dictionary<PostId, BlueskyPost>();

            var table = GetNotificationTable(dark);
            var newNotificationsCore = table.GetValuesSortedDescending(user, threshold, null).ToArray();

            Notification? newestNotification = newNotificationsCore.Length != 0 ? newNotificationsCore[0] : null;

            var newNotifications =
                newNotificationsCore
                .Select(x => RehydrateNotification(x, user, ctx, postCache))
                .WhereNonNull()
                .ToArray();

            Notification? oldestNew = newNotificationsCore.Length != 0 ? newNotificationsCore[^1] : null;

            var distinctOldCoalesceKeys = new HashSet<NotificationCoalesceKey>();

            var oldNotifications =
                table.GetValuesSortedDescending(user, null, oldestNew)
                .Select(x =>
                {
                    newestNotification ??= x;
                    return RehydrateNotification(x, user, ctx, postCache);
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

        internal bool HaveCollectionForUser(Plc plc, RepositoryImportKind kind)
        {
            return GetRepositoryImports(plc).Any(x => BlueskyEnrichedApis.RepositoryImportKindIncludesCollection(x.Kind, kind));
        }

        internal IEnumerable<(PostId PostId, Plc InReplyTo)> EnumerateRecentPosts(Plc author, Tid minDateExclusive, Tid? maxDateExclusive)
        {
            return this.UserToRecentPosts.GetValuesSortedDescending(author, new RecentPost(minDateExclusive, default), maxDateExclusive != null ? new RecentPost(maxDateExclusive.Value, default) : null).Select(x => (new PostId(author, x.RKey), x.InReplyTo));
        }
        internal IEnumerable<PostId> EnumerateRecentMediaPosts(Plc author, Tid minDateExclusive, Tid? maxDateExclusive)
        {
            return this.UserToRecentMediaPosts.GetValuesSortedDescending(author, minDateExclusive, maxDateExclusive).Select(x => new PostId(author, x));
        }
        internal IEnumerable<RecentRepost> EnumerateRecentReposts(Plc author, Tid minDateExclusive, Tid? maxDateExclusive)
        {
            return this.UserToRecentReposts.GetValuesSortedDescending(author, new RecentRepost(minDateExclusive, default), maxDateExclusive != null ? new RecentRepost(maxDateExclusive.Value, default) : null);
        }

        public ConcurrentDictionary<Plc, UserRecentPostWithScore[]> UserToRecentPopularPosts = new(); // within each user, posts are sorted by rkey
        public ConcurrentDictionary<Plc, RecentRepost[]> UserToRecentRepostsCache = new(); // within each user, posts are sorted by repost rkey


        public static Func<bool> CreateTimeoutFunc(TimeSpan timeout)
        {
            var sw = Stopwatch.StartNew();
            return () => sw.Elapsed > timeout;
        }

        public readonly static TimeSpan FollowingFeedTimeout = TimeSpan.FromMilliseconds(750);

        public IEnumerable<BlueskyPost> EnumerateFollowingFeed(RequestContext ctx, DateTime minDate, Tid? maxTidExclusive, bool atProtoOnlyPosts)
        {
            var loggedInUser = ctx.LoggedInUser;
            var minDateAsTid = minDate != default ? Tid.FromDateTime(minDate) : default;


            var follows = GetFollowingFast(ctx);
            var plcToLatestFollowRkey = new Dictionary<Plc, Tid>();

            var requireFollowStillValid = new Dictionary<BlueskyPost, (Plc A, Plc B, Plc C)>();

            var timedOut = CreateTimeoutFunc(FollowingFeedTimeout);
            var usersRecentPosts =
                follows.PossibleFollows
                .Select(pair =>
                {
                    var author = pair.Plc;
                    return
                        this
                        //.EnumerateRecentPosts(author, thresholdDate, maxTid)
                        .GetRecentPopularPosts(author, pair.IsPrivate, onlyIfAlreadyInRam: timedOut())?
                        .Reverse()
                        .SkipWhile(x => maxTidExclusive != null && x.RKey.CompareTo(maxTidExclusive.Value) >= 0)
                        .TakeWhile(x => minDateAsTid.CompareTo(x.RKey) <= 0)
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

                            var post = GetPost(new PostId(postAuthor, x.RKey), ctx);
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
                })
                .WhereNonNull();
            var usersRecentReposts =
                follows.PossibleFollows
                .Select(pair =>
                {
                    var reposter = pair.Plc;
                    BlueskyProfile? reposterProfile = null;
                    return this
                        .GetRecentReposts(reposter, pair.IsPrivate, onlyIfAlreadyInRam: timedOut())?
                        .Select(x =>
                        {
                            var post = GetPost(x.PostId, ctx);
                            post.RepostedBy = (reposterProfile ??= GetProfile(reposter, ctx));
                            post.RepostedByOrLikeRKey = x.RepostRKey;
                            requireFollowStillValid[post] = (reposter, default, default);
                            return post;
                        });
                })
                .WhereNonNull();
            var result =
                SimpleJoin.ConcatPresortedEnumerablesKeepOrdered(usersRecentPosts.Concat(usersRecentReposts).ToArray(), x => x.RepostedByOrLikeRKey != default ? x.RepostedByOrLikeRKey : x.PostId.PostRKey, new ReverseComparer<Tid>())
                .Where(x =>
                {
                    var triplet = requireFollowStillValid[x];

                    if (!(follows.IsStillFollowed(triplet.A, this) && follows.IsStillFollowed(triplet.B, this) && follows.IsStillFollowed(triplet.C, this)))
                    {
                        DiscardPost(x.PostId, ctx);
                        return false;
                    }

                    if (atProtoOnlyPosts && x.PluggableProtocol != null) return false;
                    var shouldInclude = ShouldIncludeLeafPostInFollowingFeed(x, ctx) ?? true;
                    if (!shouldInclude)
                        DiscardPost(x.PostId, ctx);
                    return shouldInclude;
                })!;
            return result;
        }

        public static void DiscardPost(PostId postId, RequestContext ctx)
        {
            ctx.UserContext.RecentlySeenOrAlreadyDiscardedFromFollowingFeedPosts?.TryAdd(postId);
        }

        public void PopulateViewerFlags(BlueskyPost post, RequestContext ctx)
        {
            if (post.DidPopulateViewerFlags) return;

            PopulateViewerFlags(post.Author, ctx);
            if (post.RepostedBy != null)
                PopulateViewerFlags(post.RepostedBy, ctx);
            if (post.QuotedPost != null)
                PopulateViewerFlags(post.QuotedPost, ctx);
            post.Labels = GetPostLabels(post.PostId, ctx.NeedsLabels).Select(x => GetLabel(x, ctx)).ToArray();
            post.IsMuted = post.ShouldMuteCore(ctx);
            post.DidPopulateViewerFlags = true;
        }


        internal IEnumerable<BlueskyPost> EnumerateFeedWithNormalization(IEnumerable<BlueskyPost> posts, RequestContext ctx, HashSet<PostId>? alreadyReturned = null, bool onlyIfRequiresFullReplyChain = false, bool omitIfMuted = false, bool forGrid = false)
        {
            alreadyReturned ??= [];
            foreach (var post in posts)
            {
                var postId = post.PostId;
                if (post.Data?.Deleted == true) continue;

                if (alreadyReturned.Contains(postId)) continue;

                if (!forGrid)
                {
                    if (post.InReplyToPostId != null && post.PluggableProtocol?.ShouldIncludeFullReplyChain(post) == true)
                    {
                        var chain = MakeFullReplyChainExcludingLeaf(post, ctx);
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

                                        rootPost = GetPost(rootId, ctx);
                                        if (omitIfMuted)
                                        {
                                            PopulateViewerFlags(rootPost, ctx);
                                            if (rootPost.IsMuted) continue;
                                        }
                                    }
                                }

                                var parentPost = GetPost(parentId, ctx);
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
                }
                alreadyReturned.Add(post.PostId);
                yield return post;
            }
        }

        public RepositoryImportEntry[] GetRepositoryImports(Plc plc)
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

        public bool GlobalFlushWithoutFirehoseCursorCapture(bool onlyIfNoPendingCompactations = false)
        {
            if (IsReadOnly) throw new InvalidOperationException("Cannot GlobalFlush when IsReadOnly.");
            var ok = true;
            foreach (var table in disposables)
            {
                if (onlyIfNoPendingCompactations)
                {
                    if (!table.FlushIfNoPendingCompactations())
                        ok = false;
                }
                else
                {
                    table.Flush(false);
                }
            }

            if (ok)
            {
                CaptureCheckpoint();
            }
            UpdateAvailableDiskSpace();
            return ok;
        }


        public long AvailableDiskSpace;
        private void UpdateAvailableDiskSpace()
        {
            AvailableDiskSpace = new DriveInfo(this.BaseDirectory).AvailableFreeSpace;
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



        public BlueskyList GetList(Relationship listId, RequestContext ctx)
        {
            return GetList(listId, null, ctx: ctx);
        }

        public BlueskyList GetList(Relationship listId, ListData? listData, RequestContext ctx)
        {
            if (!ctx.ListCache.TryGetValue(listId, out var result))
            {
                result = GetListCore(listId, listData, ctx);
                if (ctx != null)
                    ctx.ListCache[listId] = result;
            }
            return result;
        }
        private BlueskyList GetListCore(Relationship listId, ListData? listData, RequestContext ctx)
        {

            var did = GetDid(listId.Actor);
            var list = new BlueskyList
            {
                ModeratorDid = did,
                ListId = listId,
                Data = listData ?? TryGetListData(listId),
                ListIdStr = new RelationshipStr(did, listId.RelationshipRKey.ToString()!),
                Moderator = GetProfile(listId.Actor, ctx, canOmitDescription: true)
            };

            if (ctx.IsLoggedIn)
            {
                var subscription = ctx.PrivateProfile.LabelerSubscriptions.FirstOrDefault(x => new Plc(x.LabelerPlc) == list.Moderator!.Plc && new Tid(x.ListRKey) == list.ListId.RelationshipRKey);
                list.Mode = subscription?.Behavior ?? ModerationBehavior.None;
                list.PrivateNickname = subscription?.OverrideDisplayName;
            }
            return list;
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
                Author = GetProfile(plc, ctx, canOmitDescription: true),
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
                return GetList(new Relationship(plc, Tid.Parse(parsed.Rkey)), ctx: ctx);
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

                if (!HaveCollectionForUser(rootPostId.Author, RepositoryImportKind.Follows))
                    return true; // Optimistic
            }
            if (threadgate.AllowFollowers)
            {
                // replyAuthor must follow rootPostId.Author
                if (Follows.HasActor(rootPostId.Author, replyAuthor, out _))
                    return true;

                if (!HaveCollectionForUser(replyAuthor, RepositoryImportKind.Follows))
                    return true; // Optimistic
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


        internal void CompressDidDoc(DidDocProto proto, Dictionary<string, Pds>? pdsStringToIdCache = null)
        {
            if (proto.Pds == null) return;
            var pds = proto.Pds;

            Pds pdsId;
            if (pdsStringToIdCache != null)
            {
                if (!pdsStringToIdCache.TryGetValue(proto.Pds, out pdsId))
                {
                    pdsId = SerializePds(proto.Pds);
                    pdsStringToIdCache.Add(proto.Pds, pdsId);
                }
            }
            else
            {
                pdsId = SerializePds(pds);
            }
            proto.PdsId = pdsId.PdsId;
            proto.Pds = null;
        }

        public void DecompressDidDoc(DidDocProto proto)
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
            if (pds == default) AssertionLiteException.Throw("DeserializePds: pds is default(Pds)");
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

        internal void IndexHandle(string? handle, string did, RequestContext ctx, Plc plcHint = default)
        {
            var plc = SerializeDidWithHint(did, ctx, plcHint);
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
            if (string.IsNullOrEmpty(handle) || handle.Contains(':')) throw new ArgumentException("Handle is empty or contains colon.");
            HandleToDidVerifications.Add(StringUtils.HashUnicodeToUuid(StringUtils.NormalizeHandle(handle)), new HandleVerificationResult((ApproximateDateTime32)DateTime.UtcNow, plc));
        }

        internal static string DeserializeDidPlcFromUInt128(UInt128 plcAsUInt128)
        {
            return "did:plc:" + AtProtoS32.EncodePadded(plcAsUInt128);
        }

        internal static UInt128 SerializeDidPlcToUInt128(string did)
        {
            if (!did.StartsWith("did:plc:", StringComparison.Ordinal)) throw AssertionLiteException.Throw("Cannot serialize non-PLC DIDs as UInt128: " + did);
            if (did.Length != 32) throw new UnexpectedFirehoseDataException("Not a valid did:plc: " + did);
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
            Log(message);
            Log(stackTrace.ToString());
            LoggableBase.FlushLog();
            Environment.FailFast(message);
            throw new Exception(message);
        }
        public static ulong HashLabelName(string label) => System.IO.Hashing.XxHash64.HashToUInt64(MemoryMarshal.AsBytes<char>(label));
        public BlueskyLabel GetLabel(LabelId x, RequestContext ctx)
        {
            if (ctx.LabelCache?.TryGetValue(x, out var cached) == true) return cached;
            var label = new BlueskyLabel
            {
                LabelId = x,
                Moderator = GetProfile(x.Labeler, ctx, canOmitDescription: true),
                ModeratorDid = GetDid(x.Labeler),
                Name = LabelNames.TryGetPreserveOrderSpanAny(x.NameHash, out var name) ? Encoding.UTF8.GetString(name.AsSmallSpan()) : throw new Exception("Don't have name for label name hash."),
                Data = TryGetLabelData(x)
            };

            if (ctx.IsLoggedIn)
            {
                var subscription = ctx.PrivateProfile.LabelerSubscriptions.FirstOrDefault(x => new Plc(x.LabelerPlc) == label.Moderator!.Plc && x.LabelerNameHash == label.LabelId.NameHash);
                label.Mode = subscription?.Behavior ?? ModerationBehavior.None;
                label.PrivateNickname = subscription?.OverrideDisplayName;
            }
            ctx.LabelCache?.TryAdd(x, label);
            return label;
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
            if (PostLabels.GetKeyProbabilisticCache()?.PossiblyContainsKey(postId) == false) return [];
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
            return EfficientTextCompressor.Compress(text);
        }

        public static string? DecompressBpe(byte[]? bpe)
        {
            if (bpe == null || bpe.Length == 0) return null;

            return EfficientTextCompressor.Decompress(bpe);

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

        public FollowingFastResults GetFollowingFast(RequestContext ctx) // The lambda is SAFE to reuse across re-locks
        {
            var StillPrivateFollowed = Tid.MaxValue;

            var stillFollowedResult = new Dictionary<Plc, Tid>();
            var possibleFollows = new Dictionary<Plc, Tid>();
            foreach (var item in RegisteredUserToFollowees.GetValuesUnsorted(ctx.LoggedInUser))
            {
                ref var rkey = ref CollectionsMarshal.GetValueRefOrAddDefault(possibleFollows, item.Member, out _);
                if (item.ListItemRKey.CompareTo(rkey) > 0)
                    rkey = item.ListItemRKey;
            }
            foreach (var item in ctx.UserContext.PrivateFollows)
            {
                if ((item.Value.Flags & PrivateFollowFlags.PrivateFollow) != default)
                    possibleFollows[item.Key] = default;
            }

            Tid IsStillFollowedGetRkey(Plc plc, BlueskyRelationships rels)
            {
                var perContextCache = ctx.IsStillFollowedCached;
                if (perContextCache != null)
                {
                    return perContextCache.GetOrAdd(plc, plc => IsStillFollowedGetRkeyCore(plc, rels));
                }
                else
                {
                    return IsStillFollowedGetRkeyCore(plc, rels);
                }
            }
            Tid IsStillFollowedGetRkeyCore(Plc plc, BlueskyRelationships rels)
            {

                if (plc == default) return StillPrivateFollowed; // Simply means nothing to filter (e.g. root post, no parent to check for followship)

                // Callers can assume that this lambda is SAFE to reuse across re-locks (must not capture DangerousHugeReadOnlyMemorys)

                if (!possibleFollows.TryGetValue(plc, out var rkey)) return default;
                if (rkey == default) return StillPrivateFollowed; // private follow

#if true
                ref var result = ref CollectionsMarshal.GetValueRefOrAddDefault(stillFollowedResult, plc, out var exists);
                if (!exists)
                {
                    result = !rels.Follows.IsDeleted(new Relationship(ctx.LoggedInUser, rkey)) && rels.UsersHaveBlockRelationship(ctx.LoggedInUser, plc, ctx) == default ? rkey : default;
                }
#else
                var result = stillFollowedResult.GetOrAdd(plc, plc => 
                { 
                    return !rels.Follows.IsDeleted(new Relationship(ctx.LoggedInUser, rkey)) && rels.UsersHaveBlockRelationship(ctx.LoggedInUser, plc, ctx) == default ? rkey : default; 
                });
#endif
                return result;

            }

            var possibleFollowsArray = possibleFollows.Select(x => (Plc: x.Key, IsPrivate: x.Value == default)).ToArray();
            
            // Following feed gives up on cold reads after a timeout, so be fair.
            new Random((DateTime.UtcNow.Ticks / TimeSpan.TicksPerMinute).GetHashCode()).Shuffle(possibleFollowsArray);

            return new FollowingFastResults(possibleFollowsArray, (plc, rels) => IsStillFollowedGetRkey(plc, rels) != default, IsStillFollowedGetRkey,
            plc =>
            {
                if (plc == default) return true;

                if (!possibleFollows.TryGetValue(plc, out var rkey)) return false;
                if (rkey == default) return true; // private follow

                if (stillFollowedResult.TryGetValue(plc, out var result))
                    return result != default;

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

        internal BlueskyPost GetPostAndMaybeRepostedBy(PostId postId, Relationship repost, FeedSubscription? fromFeed, RequestContext ctx)
        {
            var post = GetPost(postId, ctx);
            if (repost != default)
            {
                post.RepostedByOrLikeRKey = repost.RelationshipRKey;
                post.RepostedBy = GetProfile(repost.Actor, ctx, canOmitDescription: true);
            }
            post.FromFeed = fromFeed != null ? GetFeedGenerator(new Plc(fromFeed.FeedPlc), fromFeed.FeedRKey, ctx) : null;
            return post;
        }


        public Func<PostIdTimeFirst, bool> GetIsPostSeenFuncForUserRequiresLock(RequestContext ctx)
        {
            // The returned lambda can be used in parallel as long as the same read lock is always held.

            MaybeUpdateRecentlyAlreadySeenOrDiscardedPosts(ctx);
            var seenPostsInMemory = ctx.UserContext.RecentlySeenOrAlreadyDiscardedFromFollowingFeedPosts!;
            var seenPostsInMemoryThreshold = (ctx.UserContext.RecentlySeenOrAlreadyDiscardedFromFollowingFeedPostsLastReset - BalancedFeedMaximumAge).AddSeconds(60);
            var seenPosts = SeenPosts.GetValuesChunked(ctx.LoggedInUser).ToArray();

            var cache = new ConcurrentDictionary<PostIdTimeFirst, bool>();

            return postId =>
            {
                if (seenPostsInMemory.Contains(postId)) return true;
                if (postId.PostRKey.Date > seenPostsInMemoryThreshold) return false;

                if (!cache.TryGetValue(postId, out var result))
                {
                    result = IsPostSeen(postId, seenPosts);
                    cache[postId] = result;

                    if (result)
                        DiscardPost(postId, ctx);
                }
                return result;
            };
        }

        internal static bool IsPostSeen(PostIdTimeFirst postId, DangerousHugeReadOnlyMemory<PostEngagement>[] seenPostsSlices)
        {
            foreach (var slice in seenPostsSlices)
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

        private void MaybeUpdateRecentlyAlreadySeenOrDiscardedPosts(RequestContext ctx)
        {
            var now = DateTime.UtcNow;
            var userContext = ctx.UserContext;
            if (userContext.RecentlySeenOrAlreadyDiscardedFromFollowingFeedPosts == null || (now - userContext.RecentlySeenOrAlreadyDiscardedFromFollowingFeedPostsLastReset).TotalHours >= 36)
            {
                var postSet = new ConcurrentSet<PostId>();
                foreach (var item in this.SeenPostsByDate.GetValuesUnsorted(ctx.LoggedInUser, new TimePostSeen(now - BalancedFeedMaximumAge, default)))
                {
                    postSet.TryAdd(item.PostId);
                }
                userContext.RecentlySeenOrAlreadyDiscardedFromFollowingFeedPosts = postSet;
                userContext.RecentlySeenOrAlreadyDiscardedFromFollowingFeedPostsLastReset = now;
            }
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
            if (profile.DidPopulateViewerFlags) return;
            if (ctx.IsLoggedIn)
            {
                profile.UserContext = ctx.UserContext;
                if (ctx.IsStillFollowedCached?.TryGetValue(profile.Plc, out var followRkey) == true)
                {
                    if (followRkey != Tid.MaxValue && followRkey != default) // MaxValue means private follow, default means not followed.
                        profile.IsFollowedViaAtProto = followRkey;
                }
                else
                {
                    if (Follows.HasActor(profile.Plc, ctx.LoggedInUser, out var followRel))
                        profile.IsFollowedViaAtProto = followRel.RelationshipRKey;
                }

            }
            profile.IsYou = profile.Plc == ctx.Session?.LoggedInUser;
            profile.BlockReason = GetBlockReason(profile.Plc, ctx);
            profile.FollowsYou = ctx.IsLoggedIn && profile.IsActive && Follows.HasActor(ctx.LoggedInUser, profile.Plc, out _);
            profile.Labels = GetProfileLabels(profile.Plc, ctx.NeedsLabels).Select(x => (BlueskyModerationBase)GetLabel(x, ctx)).Concat(ctx.LabelSubscriptions.Where(x => x.ListRKey != 0).Select(x =>
            {
                var listId = new Models.Relationship(new Plc(x.LabelerPlc), new Tid(x.ListRKey));
                if (IsMemberOfList(listId, profile.Plc))
                {
                    return GetList(listId, ctx: ctx);
                }
                return null;
            }).WhereNonNull()).ToArray();
            if (profile.BlockReason != default && ctx.IsLoggedIn && Blocks.HasActor(profile.Plc, ctx.LoggedInUser, out var blockedBySelf))
            {
                profile.IsBlockedBySelf = blockedBySelf.RelationshipRKey;
            }
            // ctx.Session is null when logging in (ourselves)
            profile.PrivateFollow = (ctx.IsLoggedIn ? ctx.UserContext.GetPrivateFollow(profile.Plc) : null) ?? new() { Plc = profile.Plc.PlcValue };
            profile.DidPopulateViewerFlags = true;
        }

        public void AssertCanRead()
        {
            if (!IsLockHeld)
            {
                ThrowIncorrectLockUsageException("Attempting to read without holding a read lock.");
            }
        }
        public void AssertHasWriteLock()
        {
            if (!Lock.IsWriteLockHeld)
            {
                ThrowIncorrectLockUsageException("Attempting to write without holding a write lock.");
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
            copy.UserToRecentPopularPosts = this.UserToRecentPopularPosts;
            copy.UserToRecentRepostsCache = this.UserToRecentRepostsCache;
            copy.DefaultLabelSubscriptions = this.DefaultLabelSubscriptions;
            copy.PostAuthorsSinceLastReplicaSnapshot = this.PostAuthorsSinceLastReplicaSnapshot;
            copy.RepostersSinceLastReplicaSnapshot = this.RepostersSinceLastReplicaSnapshot;
            copy.ApproximateLikeCountCache = this.ApproximateLikeCountCache;
            copy.ReplicaOnlyApproximateLikeCountCache = this.ReplicaOnlyApproximateLikeCountCache;
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
                    // LogInfo("Extra field: " + field.Name);
                }
            }
            this.PostAuthorsSinceLastReplicaSnapshot.Clear();
            this.RepostersSinceLastReplicaSnapshot.Clear();
            copy.ReplicaAge = Stopwatch.StartNew();

            // . Copied bytes: " + StringUtils.ToHumanBytes(copiedQueueBytes) + "
            LogInfo("Captured readonly replica, time: " + sw.Elapsed.TotalMilliseconds.ToString("0.00") + " ms");
            return copy;
        }


        public List<BlueskyPost> MakeFullReplyChainExcludingLeaf(BlueskyPost post, RequestContext? ctx = null)
        {
            PostId? current = post.InReplyToPostId!.Value;
            var ancestors = new List<BlueskyPost>();
            while (current != null)
            {
                var parent = GetPost(current.Value, ctx);
                ancestors.Add(parent);
                current = parent.InReplyToPostId;
            }
            ancestors.Reverse();
            ancestors[0].RepostedBy = post.RepostedBy;
            ancestors[0].RepostedByOrLikeRKey = post.RepostedByOrLikeRKey;
            var count = ancestors.Count + 1;
            post.ReplyChainLength = count;
            foreach (var item in ancestors)
            {
                item.ReplyChainLength = count;
            }
            post.RepostedBy = null;
            post.RepostedByOrLikeRKey = default;
            return ancestors;
        }

        public ConcurrentFullEvictionCache<PostIdTimeFirst, int> ApproximateLikeCountCache; // is always up to date
        public ConcurrentFullEvictionCache<PostIdTimeFirst, int> ReplicaOnlyApproximateLikeCountCache; // might be out of date, and can be racy/imprecise



        public long GetApproximateLikeCount(PostIdTimeFirst postId, bool couldBePluggablePost, bool allowImprecise = false)
        {
            if (!couldBePluggablePost)
            {
                if (ApproximateLikeCountCache.TryGetValue(postId, out var val))
                {
                    return val;
                }
                if (allowImprecise && ReplicaOnlyApproximateLikeCountCache.TryGetValue(postId, out val))
                {
                    return val;
                }
            }

            var likeCount = Likes.GetApproximateActorCount(postId);


            if (likeCount == 0 && couldBePluggablePost)
            {
                var did = GetDid(postId.Author);
                if (TryGetPluggableProtocolForDid(did) is { } pluggable && pluggable.ProvidesLikeCount(did))
                {
                    if (RecentPluggablePostLikeCount.TryGetLatestValue(postId, out var pluggableLikeCount))
                        likeCount = pluggableLikeCount;
                }
            }

            if (allowImprecise && likeCount <= int.MaxValue)
            {
                ReplicaOnlyApproximateLikeCountCache.Add(postId, (int)likeCount);
            }

            return likeCount;
        }



        public Tid? TryGetLatestBookmarkForPost(PostId postId, Plc loggedInUser)
        {
            DangerousHugeReadOnlyMemory<BookmarkPostFirst>[]? a = null;
            DangerousHugeReadOnlyMemory<Tid>[]? b = null;
            return TryGetLatestBookmarkForPost(postId, loggedInUser, ref a, ref b);
        }
        public Tid? TryGetLatestBookmarkForPost(PostId postId, Plc loggedInUser, ref DangerousHugeReadOnlyMemory<BookmarkPostFirst>[]? userBookmarks, ref DangerousHugeReadOnlyMemory<Tid>[]? userDeletedBookmarks)
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


            post.QuotedPost = GetPost(new PostId(new Plc(post.Data.QuotedPlc!.Value), new Tid(post.Data.QuotedRKey!.Value)), ctx);
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
            if (post.AllLabels.Any(x => x.Mode is ModerationBehavior.Mute or ModerationBehavior.Block)) return false;

            PopulateQuotedPost(post, ctx);
            if (post.QuotedPost != null)
            {
                if (post.QuotedPost.IsMuted == true) return false;
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


        public readonly static TimeSpan BalancedFeedMaximumAge = TimeSpan.FromDays(2);


        public IEnumerable<UserRecentPostWithScore>? GetRecentPopularPosts(Plc plc, bool couldBePluggablePost, bool onlyIfAlreadyInRam)
        {
            var result = GetRecentPopularPostsEvenVeryRecent(plc, couldBePluggablePost, onlyIfAlreadyInRam);
            if (result == null)
                return null;
            if (couldBePluggablePost)
            {
                var threshold = Tid.FromDateTime(DateTime.UtcNow - PrimarySecondaryPair.ReadOnlyReplicaMaxStalenessOnExplicitRead - TimeSpan.FromSeconds(10));
                return result.Where(x => x.RKey.CompareTo(threshold) < 0);
            }
            return result;
        }
        public IEnumerable<RecentRepost>? GetRecentReposts(Plc plc, bool couldBePluggablePost, bool onlyIfAlreadyInRam)
        {
            var result = GetRecentRepostsEvenVeryRecent(plc, onlyIfAlreadyInRam);
            if (result == null) return null;
            if (couldBePluggablePost)
            {
                var threshold = Tid.FromDateTime(DateTime.UtcNow - PrimarySecondaryPair.ReadOnlyReplicaMaxStalenessOnExplicitRead - TimeSpan.FromSeconds(10));
                return result.Where(x => x.RepostRKey.CompareTo(threshold) < 0);
            }
            return result;
        }

        private IEnumerable<UserRecentPostWithScore>? GetRecentPopularPostsEvenVeryRecent(Plc plc, bool couldBePluggablePost, bool onlyIfAlreadyInRam)
        {
            while (true)
            {
                if (UserToRecentPopularPosts.TryGetValue(plc, out var result))
                    return result;

                if (onlyIfAlreadyInRam)
                    return null;

                // This code usually runs on the readonly replica, which can slightly lag behind the primary.
                // UserToRecentPopularPosts is shared across primary and replica.
                // This means 1-2 seconds of likes might be missing from the stats of UserToRecentPopularPosts.
                var results = GetRecentPopularPostsCore(plc, couldBePluggablePost);


                // If this user posted just a few seconds ago and we're in replica, we might not have complete data. Return without caching the result.
                // It's ok to miss some likes for stats, but not ok to miss posts.
                if (IsReplica && PostAuthorsSinceLastReplicaSnapshot.Contains(plc))
                    return results;


                if (UserToRecentPopularPosts.TryAdd(plc, results))
                    return results;
            }
        }


        private IEnumerable<RecentRepost>? GetRecentRepostsEvenVeryRecent(Plc plc, bool onlyIfAlreadyInRam)
        {
            while (true)
            {
                if (UserToRecentRepostsCache.TryGetValue(plc, out var result))
                    return result;

                if (onlyIfAlreadyInRam) return null;

                // This code usually runs on the readonly replica, which can slightly lag behind the primary.
                // UserToRecentRepostsCache is shared across primary and replica.
                // This means 1-2 seconds of likes might be missing from the stats of UserToRecentRepostsCache.
                var results = GetRecentRepostsCore(plc);


                // If this user reposted just a few seconds ago and we're in replica, we might not have complete data. Return without caching the result.
                if (IsReplica && RepostersSinceLastReplicaSnapshot.Contains(plc))
                    return results;

                if (UserToRecentRepostsCache.TryAdd(plc, results))
                    return results;
            }
        }

        private UserRecentPostWithScore[] GetRecentPopularPostsCore(Plc plc, bool couldBePluggablePost)
        {
            var now = DateTime.UtcNow;
            return UserToRecentPosts.GetValuesUnsorted(plc, new RecentPost(Tid.FromDateTime(now - BalancedFeedMaximumAge), default))
                .Select(x =>
                {
                    var likeCount = GetApproximateLikeCount(new PostIdTimeFirst(x.RKey, plc), couldBePluggablePost: couldBePluggablePost);
                    return new UserRecentPostWithScore(x.RKey, x.InReplyTo, (int)Math.Min(likeCount, int.MaxValue));
                })
                .OrderBy(x => x.RKey)
                .ToArray();
        }
        private RecentRepost[] GetRecentRepostsCore(Plc plc)
        {
            var now = DateTime.UtcNow;
            return UserToRecentReposts.GetValuesUnsorted(plc, new RecentRepost(Tid.FromDateTime(now - BalancedFeedMaximumAge), default))
                .OrderBy(x => x.RepostRKey)
                .ToArray();
        }

        public ConcurrentSet<Plc> PostAuthorsSinceLastReplicaSnapshot = new();
        public ConcurrentSet<Plc> RepostersSinceLastReplicaSnapshot = new();

        public void IncrementRecentPopularPostLikeCount(PostId postId, int? setTo)
        {
            if (UserToRecentPopularPosts.TryGetValue(postId.Author, out var recentPosts))
            {
                var index = recentPosts.AsSpan().IndexOfUsingBinarySearch(new UserRecentPostWithScore(postId.PostRKey, default, default), x => x.RKey == postId.PostRKey);

                if (index != -1)
                {
                    if (recentPosts[index].ApproximateLikeCount < int.MaxValue)
                    {
                        if (setTo != null)
                            recentPosts[index].ApproximateLikeCount = setTo.Value;
                        else
                            recentPosts[index].ApproximateLikeCount++;
                    }
                }

            }
        }

        public void AddPostToRecentPostCache(Plc author, UserRecentPostWithScore post)
        {
            PostAuthorsSinceLastReplicaSnapshot.TryAdd(author);

            if (UserToRecentPopularPosts.TryGetValue(author, out var recentPopularPosts))
            {
                if (recentPopularPosts.AsSpan().IndexOfUsingBinarySearch(new UserRecentPostWithScore(post.RKey, default, default), x => x.RKey == post.RKey) != -1)
                    return;
                var threshold = DateTime.UtcNow - BlueskyRelationships.BalancedFeedMaximumAge;

                var updated = new List<UserRecentPostWithScore>();
                foreach (var other in recentPopularPosts)
                {
                    if (other.RKey.Date < threshold) continue;
                    if (post.RKey != default && post.RKey.CompareTo(other.RKey) < 0)
                    {
                        updated.Add(post);
                        post = default;
                    }
                    updated.Add(other);
                }
                if (post.RKey != default)
                    updated.Add(post);
#if DEBUG
                //_ = updated.AssertOrderedAndUnique(x => x.RKey).Count();
#endif
                UserToRecentPopularPosts[author] = updated.ToArray();
            }
        }

        public void AddRepostToRecentRepostCache(Plc reposter, RecentRepost repost)
        {
            RepostersSinceLastReplicaSnapshot.TryAdd(reposter);

            if (UserToRecentRepostsCache.TryGetValue(reposter, out var recentReposts))
            {
                if (recentReposts.AsSpan().BinarySearch(repost) >= 0)
                    return;
                var threshold = DateTime.UtcNow - BlueskyRelationships.BalancedFeedMaximumAge;

                var updated = new List<RecentRepost>();
                foreach (var other in recentReposts)
                {
                    if (other.RepostRKey.Date < threshold) continue;
                    if (repost.RepostRKey != default && repost.RepostRKey.CompareTo(other.RepostRKey) < 0)
                    {
                        updated.Add(repost);
                        repost = default;
                    }
                    updated.Add(other);
                }
                if (repost.RepostRKey != default)
                    updated.Add(repost);
                UserToRecentRepostsCache[reposter] = updated.ToArray();
            }
        }

        private readonly static long MinSizeForPruning = AppViewLiteConfiguration.GetInt64(AppViewLiteParameter.APPVIEWLITE_PRUNE_MIN_SIZE) ?? (1024 * 1024 * 1024);
        private readonly static TimeSpan PruningInterval = TimeSpan.FromDays(AppViewLiteConfiguration.GetDouble(AppViewLiteParameter.APPVIEWLITE_PRUNE_INTERVAL_DAYS) ?? 10);
        public void Prune()
        {
            if (IsReadOnly) throw new InvalidOperationException("Cannot Prune when IsReadOnly.");
            AppViewLitePruningContext? pruningContext = null;

            AssertHasWriteLock();
            var anythingPruned = false;
            foreach (var table in this.AllMultidictionaries)
            {
                if (table.MaybePrune(() => pruningContext ??= CreatePruningContext(), MinSizeForPruning, PruningInterval))
                    anythingPruned = true;
            }

            if (anythingPruned)
                GlobalFlushWithoutFirehoseCursorCapture();
            else
                Log("Nothing to prune.");
        }

        private AppViewLitePruningContext CreatePruningContext()
        {
            Log("Pruning...");
            Log("  Creating pruning context...");
            var preserveUsers = new HashSet<Plc>();
            var preservePosts = new HashSet<PostId>();
            var neighborhoodScore = new Dictionary<Plc, int>();
            var appViewLiteUsers = new HashSet<Plc>();

            Log("    Collecting pluggable protocol profiles...");
            foreach (var (plc, didBytes) in this.PlcToDidOther.EnumerateUnsortedGrouped())
            {
                var did = Encoding.UTF8.GetString(didBytes.AsSmallSpan());
                if (!BlueskyRelationships.IsNativeAtProtoDid(did))
                    preserveUsers.Add(plc);
            }
            var allowlistedPluggableProfiles = preserveUsers.Count;


            Log("    Collecting AppViewLite users and their private follows...");
            foreach (var (appviewLiteUser, appviewLiteUserBytes) in this.AppViewLiteProfiles.EnumerateUnsortedGrouped())
            {
                preserveUsers.Add(appviewLiteUser);
                appViewLiteUsers.Add(appviewLiteUser);
                var profile = BlueskyRelationships.DeserializeProto<AppViewLiteProfileProto>(appviewLiteUserBytes.AsSmallSpan());
                foreach (var privateFollow in profile.PrivateFollows ?? [])
                {
                    if ((privateFollow.Flags & PrivateFollowFlags.PrivateFollow) != 0)
                        preserveUsers.Add(new Plc(privateFollow.Plc));
                }
            }

            Log("    Collecting AppViewLite users and their public follows...");
            foreach (var (appviewLiteUser, followee) in this.RegisteredUserToFollowees.EnumerateUnsorted())
            {
                preserveUsers.Add(followee.Member);
            }

            Log("    Collecting AppViewLite users' private bookmarks...");
            foreach (var (bookmarker, bookmark) in this.Bookmarks.EnumerateUnsorted())
            {
                preservePosts.Add(bookmark.PostId);
                var score = PruneScore_ReceivesLike * PruneScore_LikeMultiplierIfAppViewLiteUser;
                IncrementSaturated(ref CollectionsMarshal.GetValueRefOrAddDefault(neighborhoodScore, bookmark.PostId.Author, out _), score);
            }

            Log("    Collecting AppViewLite users' seen posts...");
            foreach (var (viewer, engagement) in this.SeenPosts.EnumerateUnsorted())
            {
                preservePosts.Add(engagement.PostId);
            }


            static void IncrementSaturated(ref int value, int increment)
            {
                var updated = (long)value + increment;
                value = (int)(Math.Min(updated, int.MaxValue));
            }


            Log("    Collecting frequent followers/followees of AppViewLite users and their followees...");
            Stopwatch lastProgressPrint = Stopwatch.StartNew();
            void PrintProgress(long processed, long total)
            {
                if (processed == total || lastProgressPrint.ElapsedMilliseconds > 5000)
                {
                    LogInfo($"      {((double)processed / total * 100):0.0}%");
                    lastProgressPrint.Restart();
                }
            }
            long totalFollows = this.Follows.creations.ValueCount;
            long processedFollows = 0;
            foreach (var (followee, followerRelationship) in this.Follows.creations.EnumerateUnsorted())
            {
                processedFollows++;
                PrintProgress(processedFollows, totalFollows);
                var follower = followerRelationship.Actor;

                if (preserveUsers.Contains(follower))
                {
                    // Users that core users frequently follow
                    var score = PruneScore_ReceivesFollow;
                    if (appViewLiteUsers.Contains(follower)) score = int.MaxValue;
                    IncrementSaturated(ref CollectionsMarshal.GetValueRefOrAddDefault(neighborhoodScore, followee, out _), score);
                }
                if (preserveUsers.Contains(followee))
                {
                    // Frequent followers of core users
                    var score = PruneScore_SendsFollow;
                    if (appViewLiteUsers.Contains(followee)) score = int.MaxValue;
                    IncrementSaturated(ref CollectionsMarshal.GetValueRefOrAddDefault(neighborhoodScore, follower, out _), score);
                }
            }
            lastProgressPrint.Restart();
            Log("    Collecting frequent likers/likees of AppViewLite users and their followees...");

            long totalLikes = this.Likes.creations.ValueCount;
            long processedLikes = 0;
            foreach (var (postId, like) in this.Likes.creations.EnumerateUnsorted())
            {
                processedLikes++;
                PrintProgress(processedLikes, totalLikes);

                var from = like.Actor;
                var to = postId.Author;

                if (preserveUsers.Contains(from))
                {
                    // Users that core users frequently like
                    var score = PruneScore_ReceivesLike;
                    if (appViewLiteUsers.Contains(from))
                    {
                        score *= PruneScore_LikeMultiplierIfAppViewLiteUser;
                        preservePosts.Add(postId); // Preserve posts liked by AppViewLite users.
                    }
                    IncrementSaturated(ref CollectionsMarshal.GetValueRefOrAddDefault(neighborhoodScore, to, out _), score);
                }
                if (preserveUsers.Contains(to))
                {
                    // Frequent likers of core users
                    var score = PruneScore_SendsLike;
                    if (appViewLiteUsers.Contains(to))
                    {
                        score *= PruneScore_LikeMultiplierIfAppViewLiteUser;
                        // AppViewLite user is the author of this liked post. Such posts are always preserved by ShouldPreservePost.
                    }
                    IncrementSaturated(ref CollectionsMarshal.GetValueRefOrAddDefault(neighborhoodScore, from, out _), score);
                }
            }
            Log("    Neighborhood scores computed, selecting top scores...");

            var priorityQueue = new PriorityQueue<Plc, int>();
            foreach (var (plc, score) in neighborhoodScore)
            {
                priorityQueue.Enqueue(plc, -score);
            }

            var neighborhoodSize = (AppViewLiteConfiguration.GetInt32(AppViewLiteParameter.APPVIEWLITE_PRUNE_NEIGHBORHOOD_SIZE) ?? 1_000_000) + allowlistedPluggableProfiles;
            while (true)
            {
                if (!priorityQueue.TryDequeue(out var plc, out var priority)) break;
                var score = -priority;

                if (preserveUsers.Count >= neighborhoodSize && score < int.MaxValue) break;
                preserveUsers.Add(plc);
            }
            Log($"    Pruning neighborhood computed. Will preserve content from {preserveUsers.Count} users, and {preservePosts.Count} isolated posts.");
            Log("    Proceeding with actual pruning...");

            return new AppViewLitePruningContext
            {
                PreservePosts = preservePosts,
                PreserveUsers = preserveUsers,
                OldPostThreshold = Tid.FromDateTime(DateTime.UtcNow.AddDays(AppViewLiteConfiguration.GetDouble(AppViewLiteParameter.APPVIEWLITE_PRUNE_OLD_DAYS) ?? 30)),
            };
        }

        public const int PruneScore_SendsLike = 1;
        public const int PruneScore_ReceivesLike = 3;

        public const int PruneScore_SendsFollow = 3;
        public const int PruneScore_ReceivesFollow = 9;

        public const int PruneScore_LikeMultiplierIfAppViewLiteUser = 4;

        public void MaybeEnterWriteLockAndPrune()
        {
            if (AppViewLiteConfiguration.GetBool(AppViewLiteParameter.APPVIEWLITE_RUN_PRUNING).GetValueOrDefault())
            {
                Lock.EnterWriteLock();
                try
                {
                    Prune();
                }
                finally
                {
                    Lock.ExitWriteLock();
                }
            }
        }



        public object GetCountersThreadSafe()
        {

            return new
            {
                this.AvoidFlushes,
                checkpointToLoad = this.checkpointToLoad?.Count,
                DefaultLabelSubscriptions = this.DefaultLabelSubscriptions.Length,
                this.PlcDirectoryStaleness,
                this.PlcDirectorySyncDate,
                DirectIoReadCache = CombinedPersistentMultiDictionary.DirectIoReadCache?.GetCounters(),
                PostLiveSubscribersThreadSafe = this.PostLiveSubscribersThreadSafe.Count,
                registerForNotificationsCache = this.registerForNotificationsCache.Count,
                ReplicaAge = this.ReplicaAge?.Elapsed,
                ShutdownRequested = this.ShutdownRequested.IsCancellationRequested,
                UserNotificationSubscribersThreadSafe = this.UserNotificationSubscribersThreadSafe.Count,
                this.Version,

            };
        }

        public object GetCountersThreadSafePrimaryOnly(bool cursors)
        {
            FirehoseCursor[]? firehoseCursors = null;
            if (cursors && IsPrimary)
            {
                lock (this.firehoseCursors!)
                {
                    firehoseCursors = this.firehoseCursors.Values.ToArray();
                }
            }
            return new
            {
                PostAuthorsSinceLastReplicaSnapshot = this.PostAuthorsSinceLastReplicaSnapshot.Count,
                RepostersSinceLastReplicaSnapshot = this.RepostersSinceLastReplicaSnapshot.Count,
                UserToRecentPopularPosts = this.UserToRecentPopularPosts.Count,
                AvailableDiskSpaceMb = AvailableDiskSpace / (float)(1024 * 1024),
                DidToPlcConcurrentCache = this.DidToPlcConcurrentCache.GetCounters(),
                ApproximateLikeCountCache = this.ApproximateLikeCountCache.GetCounters(),
                ReplicaOnlyApproximateLikeCountCache = this.ReplicaOnlyApproximateLikeCountCache.GetCounters(),
                PlcToDidConcurrentCache = this.PlcToDidConcurrentCache.GetCounters(),
                Caches = this.AllMultidictionaries.SelectMany(x => x.GetCounters()).ToDictionary(x => x.Name, x => x.Value),
                Firehoses = firehoseCursors,
            };
        }

        public static void Assert(bool ensure, string text)
        {
            if (!ensure)
                ThrowFatalError(text);
        }
        public static void Assert(bool ensure)
        {
            if (!ensure)
                ThrowFatalError("Failed assertion.");
        }

        public IEnumerable<UserEngagementStats> GetUserEngagementScoresForUser(Plc loggedInUser)
        {
            BlueskyRelationships.Assert(SeenPosts.slices.Count == UserPairEngagementCache.cacheSlices.Count, "Slice count mismatch between SeenPosts and UserPairEngagementCache.");
            var newdict = new Dictionary<Plc, float>();

            var concatenated = SimpleJoin.ConcatPresortedEnumerablesKeepOrdered(UserPairEngagementCache.cacheSlices.Select(x => x.Cache.GetValues(loggedInUser).AsEnumerable()).ToArray(), x => x.Target);
            foreach (var user in SimpleJoin.GroupAssumingOrderedInput(concatenated, x => x.Target))
            {

                UserEngagementStats cumulative = default;
                cumulative.Target = user.Key;
                foreach (var slice in user.Values)
                {
                    cumulative.EngagedPosts += slice.EngagedPosts;
                    cumulative.FollowingEngagedPosts += slice.FollowingEngagedPosts;
                    cumulative.FollowingSeenPosts += slice.FollowingSeenPosts;
                }
                yield return cumulative;

            }
        }
        public static float GetUserEngagementScore(UserEngagementStats stats, double averageEngagementRatio)
        {

            double otherEngagements = stats.EngagedPosts - stats.FollowingEngagedPosts;
            BlueskyRelationships.Assert(otherEngagements >= 0);
            otherEngagements = Math.Pow(otherEngagements, 0.3);

            var otherTotalEstimation = otherEngagements / averageEngagementRatio;

            const double nonFeedWeight = 0.1;

            var positive = stats.FollowingEngagedPosts + otherEngagements * nonFeedWeight;
            var total = stats.FollowingSeenPosts + otherTotalEstimation * nonFeedWeight;

            return (float)WeightedMeanWithPrior(positive, total, averageEngagementRatio, 50);

        }

        public static double WeightedMeanWithPrior(double positive, double total, double prior, int priorWeight)
        {
            return (priorWeight * prior + positive) / (priorWeight + total);
        }

        public SemaphoreSlim CarRecordInsertionSemaphore = new SemaphoreSlim(AppViewLiteConfiguration.GetInt32(AppViewLiteParameter.APPVIEWLITE_CAR_INSERTION_SEMAPHORE_SIZE) ?? 2);

        public string CarSpillDirectory => Path.Combine(BaseDirectory, "car-disk-spill");



        public OpenGraphData? GetOpenGraphData(string externalUrl)
        {
            var urlHash = StringUtils.HashUnicodeToUuid(externalUrl);
            if (!OpenGraphData.TryGetPreserveOrderSpanLatest(urlHash, out var span)) return null;
            var proto = DeserializeProto<OpenGraphData>(span.AsSmallSpan());

            EfficientTextCompressor.DecompressInPlace(ref proto.ExternalTitle, ref proto.ExternalTitleBpe);
            EfficientTextCompressor.DecompressInPlace(ref proto.ExternalDescription, ref proto.ExternalDescriptionBpe);
            EfficientTextCompressor.DecompressInPlace(ref proto.ExternalThumbnailUrl, ref proto.ExternalThumbnailUrlBpe);

            // not needed
            // EfficientTextCompressor.DecompressInPlace(ref proto.ExternalUrl, ref proto.ExternalUrlBpe);

            proto.ExternalUrl = externalUrl;
            return proto;
        }

        public readonly static bool UseProbabilisticSets = AppViewLiteConfiguration.GetBool(AppViewLiteParameter.APPVIEWLITE_USE_PROBABILISTIC_SETS) ?? true;


        public AccountState GetAccountState(Plc plc) => AccountStates.TryGetLatestValue(plc, out var b) ? (AccountState)b : AccountState.Unknown;
        public bool IsAccountActive(Plc plc) => IsAccountActive(GetAccountState(plc));
        public static bool IsAccountActive(AccountState state) => state is AccountState.Unknown or AccountState.Active;


        public Versioned<T> AsVersioned<T>(T value) => new(value, Version);


        internal readonly string[] RootDirectoriesToGcCollect;



        internal FirehoseCursor GetOrCreateFirehoseCursorThreadSafe(string firehoseUrl)
        {
            lock (firehoseCursors!)
            {
                if (!firehoseCursors.TryGetValue(firehoseUrl, out var cursor))
                {
                    cursor = new FirehoseCursor { FirehoseUrl = firehoseUrl };
                    firehoseCursors.Add(firehoseUrl, cursor);
                }
                return cursor;
            }
        }

        public static unsafe NativeMemoryRange AllocUnaligned(int length)
        {
            var ptr = CombinedPersistentMultiDictionary.UnalignedArenaForCurrentThread!.Allocate(length);
            return new NativeMemoryRange((nuint)ptr, (nuint)length);
        }
    }


    public delegate void LiveNotificationDelegate(Versioned<PostStatsNotification> notification, Plc commitPlc);

    public record struct FollowingFastResults((Plc Plc, bool IsPrivate)[] PossibleFollows, Func<Plc, BlueskyRelationships, bool> IsStillFollowed, Func<Plc, BlueskyRelationships, Tid> IsStillFollowedGetRkey, Func<Plc, bool> IsPossiblyStillFollowed);
}


