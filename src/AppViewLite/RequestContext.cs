using AppViewLite.Models;
using AppViewLite.Numerics;
using System;
using System.Collections.Concurrent;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace AppViewLite
{
    public class RequestContext
    {
        private TimeSpan? shortTimeout;
        public Task? ShortDeadline { get; private set; }
        public required AppViewLiteSession Session { get; set; }
        public AppViewLiteUserContext UserContext => Session.UserContext;

        public AppViewLiteProfileProto PrivateProfile => UserContext.PrivateProfile!;
        public string? SignalrConnectionId { get; set; }
        public bool IsUrgent { get; init; }

        public string? RequestUrl { get; init; }
        public string Reason { get; init; }
        public string ReasonOrUrlForBucketing => _urlForBucketing ?? Reason;
        private string? _urlForBucketing;
        public bool AllowStale { get; set; }
        public Stopwatch TimeSpentWaitingForLocks = new Stopwatch();

        public int WriteOrUpgradeLockEnterCount;
        public int ReadsFromPrimaryStolen;
        public int ReadsFromPrimaryLate;
        public int ReadsFromPrimaryNonUrgent;
        public int ReadsFromSecondary;
        public int WritesToPrimaryStolen;
        public int WritesToPrimaryLate;

        public long StopwatchTicksSpentInsideSecondaryLock;
        public long StopwatchTicksSpentInsidePrimaryLock;
        public int MaxOccurredGarbageCollectionGenerationInsideLock = -1;
        public readonly Guid Guid = Guid.NewGuid();

        public AccentColor AccentColor => IsLoggedIn ? PrivateProfile.AccentColor : AccentColor.Blue;

        public static Func<string, string, object[], Task>? SendSignalrImpl;

        public void SendSignalrAsync(string method, params object[] arguments)
        {
            if (SignalrConnectionId == null || SendSignalrImpl == null) return;
            SendSignalrImpl.Invoke(SignalrConnectionId, method, arguments);
        }

        public ConcurrentDictionary<Plc, Tid>? IsStillFollowedCached;
        public ConcurrentDictionary<(Plc, Plc), BlockReason>? BlockReasonCache;
        public ConcurrentDictionary<Plc, List<Relationship>>? SubscribedBlocklistsCache;
        public ConcurrentDictionary<Relationship, BlueskyList>? ListCache;
        public ConcurrentDictionary<(PostId RootPostId, Plc ReplyAuthor), bool>? ThreadgateAllowsUserCache;
        public ConcurrentDictionary<PostId, BlueskyThreadgate?>? ThreadgateCache;
        public ConcurrentBag<OperationLogEntry> OperationLogEntries = new();
        public ConcurrentBag<OperationLogEntry> LockLogEntries = new();
        public ConcurrentBag<OperationLogEntry>? NetworkLogEntries;

        public static ConcurrentQueue<RequestContext> RecentRequestContextsUrgent = new();
        public static ConcurrentQueue<RequestContext> RecentRequestContextsNonUrgent = new();

        public long MinVersion;

        private RequestContext()
        {
        }

        private int addedToMetricsTable;
        public void AddToMetricsTable()
        {
            if (Interlocked.Increment(ref addedToMetricsTable) != 1) return;
            var queue = IsUrgent ? RecentRequestContextsUrgent : RecentRequestContextsNonUrgent;
            queue.Enqueue(this);
            while (queue.Count >= 200)
                queue.TryDequeue(out _);
        }

        public override string? ToString()
        {
            return RequestUrl ?? Reason;
        }

        public required DateTime StartDate;

        private void InitializeDeadlines()
        {
            ShortDeadline = shortTimeout != null ? Task.Delay(shortTimeout.Value) : null;
        }

        public ConcurrentDictionary<Plc, BlueskyProfile>? ProfileCache;
        public ConcurrentDictionary<LabelId, BlueskyLabel>? LabelCache;

        public static RequestContext CreateForRequest(string reason, AppViewLiteSession? session = null, string? signalrConnectionId = null, bool urgent = true, string? requestUrl = null, long minVersion = default)
        {
            if (session != null && session.IsLoggedIn)
            {
                minVersion = Math.Max(minVersion, session.UserContext.MinVersion);
            }
            string? requestUrlForBucketing = null;
            if (requestUrl != null)
            {
                requestUrlForBucketing = requestUrl;
                var questionMark = requestUrl.IndexOf('?');
                if (questionMark != -1)
                    requestUrlForBucketing = requestUrlForBucketing.Substring(0, questionMark);
                var segments = requestUrlForBucketing.Split('/').AsSpan(1);
                if (segments.Length != 0 && segments[0].StartsWith('@') == true)
                {
                    segments[0] = "@DID";
                    if (segments.Length != 1)
                    {
                        if (segments[1].StartsWith('3')) segments[1] = "RKEY";
                        if (segments[1] is "feed" or "lists") segments[2] = "RKEY";
                    }
                }
                requestUrlForBucketing = "/" + string.Join('/', segments!);
            }
            var ctx = new RequestContext
            {
                Session = session!,
                shortTimeout = TimeSpan.FromSeconds(0.2),
                SignalrConnectionId = signalrConnectionId,
                RequestUrl = requestUrl,
                IsUrgent = urgent,
                AllowStale = true,
                StartDate = DateTime.UtcNow,
                MinVersion = minVersion,
                ProfileCache = new(),
                LabelCache = new(),
                Reason = reason,
                _urlForBucketing = requestUrlForBucketing,
                LabelSubscriptions = session != null && session.IsLoggedIn ? session.UserContext.PrivateProfile!.LabelerSubscriptions : BlueskyEnrichedApis.Instance.DangerousUnlockedRelationships.DefaultLabelSubscriptions
            };
            ctx.InitializeDeadlines();
            return ctx;
        }

        public static RequestContext CreateForTaskDictionary(RequestContext originalCtx, bool possiblyUrgent = false)
        {
            return new RequestContext()
            {
                Session = null!,
                IsUrgent = possiblyUrgent && originalCtx.IsUrgent,
                RequestUrl = originalCtx.RequestUrl,
                MinVersion = originalCtx.MinVersion,
                StartDate = DateTime.UtcNow,
                Reason = originalCtx.Reason,
                _urlForBucketing = originalCtx._urlForBucketing,
                OnBehalfOf = originalCtx,
                LabelSubscriptions = [],
            };
        }


        public static RequestContext ToNonUrgent(RequestContext originalCtx)
        {
            if (!originalCtx.IsUrgent) return originalCtx;
            return new RequestContext()
            {
                Session = originalCtx.Session,
                IsUrgent = false,
                AllowStale = originalCtx.AllowStale,
                ProfileCache = originalCtx.ProfileCache,
                ShortDeadline = originalCtx.ShortDeadline,
                SignalrConnectionId = originalCtx.SignalrConnectionId,
                shortTimeout = originalCtx.shortTimeout,
                RequestUrl = originalCtx.RequestUrl,
                MinVersion = originalCtx.MinVersion,
                StartDate = originalCtx.StartDate,
                LabelSubscriptions = originalCtx.LabelSubscriptions,
                Reason = originalCtx.Reason,
                _urlForBucketing = originalCtx._urlForBucketing,
                OnBehalfOf = originalCtx.OnBehalfOf,
            };
        }



        public static RequestContext CreateForFirehose(string reason, bool allowStale = false)
        {
            return new RequestContext()
            {
                Session = null!,
                StartDate = DateTime.UtcNow,
                LabelSubscriptions = [],
                Reason = reason,
                AllowStale = allowStale,
            };
        }

        public string DebugText => RequestUrl ?? Reason;

        public void IncreaseTimeout(TimeSpan? shortTimeout = null)
        {
            this.shortTimeout = shortTimeout;
            InitializeDeadlines();
        }

        public void BumpMinimumVersion(long version)
        {
            while (true)
            {
                var oldVersion = this.MinVersion;
                if (oldVersion >= version) break;

                Interlocked.CompareExchange(ref MinVersion, version, oldVersion);
            }


            if (IsLoggedIn)
            {
                var userContext = this.UserContext;
                while (true)
                {
                    var oldVersion = userContext.MinVersion;
                    if (oldVersion >= version) break;

                    Interlocked.CompareExchange(ref userContext.MinVersion, version, oldVersion);
                }
            }
        }

        public bool IsLoggedIn => Session != null && Session.IsLoggedIn;
        public Plc LoggedInUser => Session!.LoggedInUser!.Value;

        public required LabelerSubscription[] LabelSubscriptions;
        private HashSet<LabelId>? _needsLabels;
        public long CompletionTimeStopwatchTicks;

        public RequestContext? OnBehalfOf;
        public RequestContext RootOnBehalfOf
        {
            get 
            {
                var ctx = this;
                while (ctx.OnBehalfOf != null) ctx = ctx.OnBehalfOf;
                return ctx;
            }
        }

        public HashSet<LabelId> NeedsLabels => _needsLabels ??= (LabelSubscriptions.Where(x => x.ListRKey == 0).Select(x => new LabelId(new Plc(x.LabelerPlc), x.LabelerNameHash))).ToHashSet();


        private readonly static FrozenSet<string> _administratorDids = (AppViewLiteConfiguration.GetStringList(AppViewLiteParameter.APPVIEWLITE_ADMINISTRATIVE_DIDS) ?? []).ToFrozenSet();
        public void EnsureAdministrator()
        {
            if (!IsAdministrator())
                throw new UnauthorizedAccessException("This action requires administrative privileges.");
        }

        private bool IsAdministrator()
        {
            return _administratorDids.Contains("*") || (IsLoggedIn && _administratorDids.Contains(this.UserContext.Did!));
        }

        public IReadOnlyList<OperationLogEntry> GetOperationEntriesPlusImportant()
        {
            var entries = OperationLogEntries.OrderBy(x => x.Start.StopwatchTicks);
            var result = new List<OperationLogEntry>();
            OperationLogEntry? prev = null;
            foreach (var entry in entries)
            {
                if (prev != null)
                {
                    var delta = prev.End - entry.Start;
                    if (delta.MaxGcGeneration != -1 || delta.IoReadBytes != 0 || delta.IoReads != 0 || delta.MmapPotentialReadBytes != 0)
                        result.Add(new OperationLogEntry(prev.End, entry.Start, null, null, null));
                }
                result.Add(entry);
                prev = entry;
            }
            return result;
        }

        internal static void InitCacheField<TObject>([NotNull] ref TObject? field) where TObject: class, new()
        {
            if (field != null) return;
            Interlocked.CompareExchange(ref field, new TObject(), null);
        }
    }

    public record OperationLogEntry(PerformanceSnapshot Start, PerformanceSnapshot End, string? TableName, string? Operation, object? Argument)
    {
        public PerformanceSnapshot Delta => End - Start;
        public string TooltipText 
        {
            get
            {
                var delta = Delta;
                return
                    (Operation != null ? TableName + "/" + Operation + (Argument != null ? ": " + Argument.ToString() : null) : "(Unknown)") +
                    (delta.IoReads != 0 ? "\nReads: " + StringUtils.ToHumanBytes(delta.IoReadBytes, allowByteGranularity: true) + (delta.IoReads != 1 ? " (" + delta.IoReads + " reads)" : null) : null) +
                    (delta.MmapPotentialReadBytes != 0 ? "\nMmap: " + StringUtils.ToHumanBytes(delta.MmapPotentialReadBytes, allowByteGranularity: true) : null) +
                    (delta.MaxGcGeneration != -1 ? "\n[GC " + delta.MaxGcGeneration + "]" : null) + 
                    "\n" + StringUtils.ToHumanTimeSpanForProfiler(BlueskyRelationshipsClientBase.StopwatchTicksToTimespan(delta.StopwatchTicks)) + " (seek: "+ StringUtils.ToHumanTimeSpanForProfiler(BlueskyRelationshipsClientBase.StopwatchTicksToTimespan(delta.SeekStopwatchTicks)) + ")";
            }
        }
    }

}

