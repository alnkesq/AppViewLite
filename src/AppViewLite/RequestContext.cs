using AppViewLite.Models;
using System;
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



        public static Func<string, string, object[], Task>? SendSignalrImpl;

        public void SendSignalrAsync(string method, params object[] arguments)
        {
            if (SignalrConnectionId == null || SendSignalrImpl == null) return;
            SendSignalrImpl.Invoke(SignalrConnectionId, method, arguments);
        }

        public RequestContext(AppViewLiteSession? session, TimeSpan? longTimeout, TimeSpan? shortTimeout, string? signalrConnectionId)
        {
            this.longTimeout = longTimeout;
            this.shortTimeout = shortTimeout;
            this.Session = session;
            this.SignalrConnectionId = signalrConnectionId;
            InitializeDeadlines();
        }

        private void InitializeDeadlines()
        {
            ShortDeadline = shortTimeout != null ? Task.Delay(shortTimeout.Value) : null;
            LongDeadline = longTimeout != null ? Task.Delay(longTimeout.Value) : null;
        }

        public static RequestContext Create(AppViewLiteSession? session = null, string? signalrConnectionId = null)
        {
            return new RequestContext(session, TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(0.2), signalrConnectionId);
        }

        public static RequestContext CreateInfinite(AppViewLiteSession? session, string? signalrConnectionId = null)
        {
            return new RequestContext(session, null, null, signalrConnectionId);
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

