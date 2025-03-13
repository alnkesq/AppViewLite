using AppViewLite.Models;
using System;
using System.Collections.Concurrent;
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
        public string? FirehoseReason { get; init; }

        public bool AllowStale { get; set; }
        public Stopwatch TimeSpentWaitingForLocks = new Stopwatch();
        
        public int WriteOrUpgradeLockEnterCount;
        public int ReadsFromPrimary;
        public int ReadsFromSecondary;

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
            if (IsUrgent && WriteOrUpgradeLockEnterCount != 0) { }
            if (Interlocked.Increment(ref addedToMetricsTable) != 1) return;
            var queue = IsUrgent ? RecentRequestContextsUrgent : RecentRequestContextsNonUrgent;
            queue.Enqueue(this);
            if (queue.Count >= 1000)
                queue.TryDequeue(out _);
        }

        public override string? ToString()
        {
            return RequestUrl;
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
            };
        }



        public static RequestContext CreateForFirehose(string reason)
        {
            return new RequestContext()
            {
                Session = null!,
                StartDate = DateTime.UtcNow,
                LabelSubscriptions = [],
            };
        }

        public void IncreaseTimeout(TimeSpan? shortTimeout = null)
        {
            this.shortTimeout = shortTimeout;
            InitializeDeadlines();
        }

        internal void BumpMinimumVersion(long version)
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



#if false
        // Blazor server code
        public RequestContext OnStateChanged(Action a)
        {
            var synchronizationContext = SynchronizationContext.Current!;
            var lockObj = new Lock();


            Watchdog? watchdog = null;
            var ctx = new RequestContext(Session, longTimeout, shortTimeout, SignalrConnectionId);

            ctx._triggerStateChange = () =>
            {
                Console.Error.WriteLine("   RecordReceived!");
                lock (lockObj)
                {
                    watchdog ??= Watchdog.Create(TimeSpan.FromMilliseconds(100), () =>
                    {
                        lock (lockObj)
                        {
                            watchdog = null;
                        }
                        synchronizationContext.Post(_ =>
                        {

                            a();
                            Console.Error.WriteLine("       ONSTATECHANGE!");
                        }, null);
                    });

                }

            };
            ctx.InitializeDeadlines(); // reinitialize, don't reuse (blazor server navigations don't provide a new request context)
            return ctx;
        }
#endif

        public bool IsLoggedIn => Session != null && Session.IsLoggedIn;
        public Plc LoggedInUser => Session!.LoggedInUser!.Value;

        public required LabelerSubscription[] LabelSubscriptions;
        private HashSet<LabelId>? _needsLabels;

        public HashSet<LabelId> NeedsLabels => _needsLabels ??= (LabelSubscriptions.Where(x => x.ListRKey == 0).Select(x => new LabelId(new Plc(x.LabelerPlc), x.LabelerNameHash))).ToHashSet();
    }

}

