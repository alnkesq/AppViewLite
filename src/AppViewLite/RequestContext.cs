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
        private TimeSpan? longTimeout;
        private TimeSpan? shortTimeout;
        public Task? LongDeadline { get; private set; }
        public Task? ShortDeadline { get; private set; }
        public AppViewLiteSession Session { get; set; }
        public string? SignalrConnectionId { get; set; }
        public bool IsUrgent { get; }

        public string? RequestUrl { get; }

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

        public RequestContext(AppViewLiteSession? session, TimeSpan? longTimeout, TimeSpan? shortTimeout, string? signalrConnectionId, bool urgent = false, string? requestUrl = null)
        {
            this.longTimeout = longTimeout;
            this.shortTimeout = shortTimeout;
            this.Session = session;
            this.SignalrConnectionId = signalrConnectionId;
            this.IsUrgent = urgent;
            this.StartDate = DateTime.UtcNow;
            this.RequestUrl = requestUrl;
            InitializeDeadlines();
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

        public DateTime StartDate;

        private void InitializeDeadlines()
        {
            ShortDeadline = shortTimeout != null ? Task.Delay(shortTimeout.Value) : null;
            LongDeadline = longTimeout != null ? Task.Delay(longTimeout.Value) : null;
        }

        public static RequestContext Create(AppViewLiteSession? session = null, string? signalrConnectionId = null, bool urgent = false, string? requestUrl = null)
        {
            return new RequestContext(session, TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(0.2), signalrConnectionId, urgent: urgent, requestUrl: requestUrl);
        }

        public static RequestContext CreateInfinite(AppViewLiteSession? session, string? signalrConnectionId = null, bool urgent = false)
        {
            return new RequestContext(session, null, null, signalrConnectionId, urgent: urgent);
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

