using AppViewLite;
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
        public AppViewLiteSession Session { get; private set; }
        private Action? _triggerStateChange;

        public void TriggerStateChange()
        {
            _triggerStateChange?.Invoke();
        }


        public RequestContext(AppViewLiteSession? session, TimeSpan? longTimeout, TimeSpan? shortTimeout)
        {
            this.longTimeout = longTimeout;
            this.shortTimeout = shortTimeout;
            this.Session = session;
            InitializeDeadlines();
        }

        private void InitializeDeadlines()
        {
            ShortDeadline = shortTimeout != null ? Task.Delay(shortTimeout.Value) : null;
            LongDeadline = longTimeout != null ? Task.Delay(longTimeout.Value) : null;
        }

        public static RequestContext Create(AppViewLiteSession? session = null)
        {
            return new RequestContext(session, TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(0.5));
        }

        public static RequestContext CreateInfinite(AppViewLiteSession? session)
        {
            return new RequestContext(session, null, null);
        }

        public RequestContext OnStateChanged(Action a)
        {
            var synchronizationContext = SynchronizationContext.Current!;
            var lockObj = new Lock();


            Watchdog? watchdog = null;
            var ctx = new RequestContext(Session, longTimeout, shortTimeout);

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

        public bool IsLoggedIn => Session != null && Session.IsLoggedIn;
        public Plc LoggedInUser => Session!.LoggedInUser!.Value;
    }

}

