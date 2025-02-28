using AppViewLite.Models;
using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
namespace AppViewLite
{
    public class RequestContext
    {
        private TimeSpan? shortTimeout;
        public Task? ShortDeadline { get; private set; }
        public required AppViewLiteSession Session { get; set; }
        public string? SignalrConnectionId { get; set; }
        public bool IsUrgent { get; init; }

        public string? RequestUrl { get; init; }
        public string? FirehoseReason { get; init; }

        public bool AllowStale { get; set; }
        public Stopwatch TimeSpentWaitingForLocks = new Stopwatch();
        public int ReadLockEnterCount;
        public int WriteOrUpgradeLockEnterCount;

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

        private bool addedToMetricsTable;
        public void AddToMetricsTable()
        {
            if (addedToMetricsTable) return;
            addedToMetricsTable = true;
            if (IsUrgent) { }
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



        public static RequestContext CreateForRequest(AppViewLiteSession? session = null, string? signalrConnectionId = null, bool urgent = true, string? requestUrl = null)
        {
            var ctx = new RequestContext
            {
                Session = session!,
                shortTimeout = TimeSpan.FromSeconds(0.2),
                SignalrConnectionId = signalrConnectionId,
                RequestUrl = requestUrl,
                IsUrgent = urgent,
                AllowStale = true,
                StartDate = DateTime.UtcNow,
            };
            ctx.InitializeDeadlines();
            return ctx;
        }

        public static RequestContext CreateForTaskDictionary(RequestContext? originalCtx)
        {
            return new RequestContext()
            {
                Session = null!,
                IsUrgent = originalCtx?.IsUrgent ?? false,
                RequestUrl = originalCtx?.RequestUrl,
                MinVersion = originalCtx?.MinVersion ?? 0,
                StartDate = DateTime.UtcNow,
            };
        }

        internal static RequestContext CreateForFirehose(string reason)
        {
            return new RequestContext()
            {
                Session = null!,
                StartDate = DateTime.UtcNow, 
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
                if (oldVersion >= version) return;

                Interlocked.CompareExchange(ref MinVersion, version, oldVersion);
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
    }

}

