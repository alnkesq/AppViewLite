using AppViewLite.Models;
using System;
using System.Collections.Concurrent;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Diagnostics;
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
        public string? FirehoseReason { get; init; }

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

        public AccentColor AccentColor => IsLoggedIn ? PrivateProfile.AccentColor : AccentColor.Blue;

        public static Func<string, string, object[], Task>? SendSignalrImpl;

        public void SendSignalrAsync(string method, params object[] arguments)
        {
            if (SignalrConnectionId == null || SendSignalrImpl == null) return;
            SendSignalrImpl.Invoke(SignalrConnectionId, method, arguments);
        }


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
            if (queue.Count >= 1000)
                queue.TryDequeue(out _);
        }

        public override string? ToString()
        {
            return RequestUrl ?? FirehoseReason;
        }

        public required DateTime StartDate;

        private void InitializeDeadlines()
        {
            ShortDeadline = shortTimeout != null ? Task.Delay(shortTimeout.Value) : null;
        }

        public ConcurrentDictionary<Plc, BlueskyProfile>? ProfileCache;

        public static RequestContext CreateForRequest(AppViewLiteSession? session = null, string? signalrConnectionId = null, bool urgent = true, string? requestUrl = null, long minVersion = default)
        {
            if (session != null && session.IsLoggedIn)
            {
                minVersion = Math.Max(minVersion, session.UserContext.MinVersion);
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
                LabelSubscriptions = session != null && session.IsLoggedIn ? session.UserContext.PrivateProfile!.LabelerSubscriptions : BlueskyEnrichedApis.Instance.DangerousUnlockedRelationships.DefaultLabelSubscriptions
            };
            ctx.InitializeDeadlines();
            return ctx;
        }

        public static RequestContext CreateForTaskDictionary(RequestContext originalCtx, bool possiblyUrgent = false)
        {
            if (originalCtx.IsUrgent) { }
            return new RequestContext()
            {
                Session = null!,
                IsUrgent = possiblyUrgent && originalCtx.IsUrgent,
                RequestUrl = originalCtx.RequestUrl,
                MinVersion = originalCtx.MinVersion,
                StartDate = DateTime.UtcNow,
                FirehoseReason = originalCtx.FirehoseReason,
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
                FirehoseReason = originalCtx.FirehoseReason,
            };
        }



        public static RequestContext CreateForFirehose(string reason, bool allowStale = false)
        {
            return new RequestContext()
            {
                Session = null!,
                StartDate = DateTime.UtcNow,
                LabelSubscriptions = [],
                FirehoseReason = reason,
                AllowStale = allowStale,
            };
        }

        public string DebugText => FirehoseReason ?? RequestUrl ?? "Unknown";

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

    }

}

