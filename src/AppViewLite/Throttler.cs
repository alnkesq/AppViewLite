using System;
using System.Runtime.CompilerServices;
using System.Threading;

namespace AppViewLite
{
    public class Throttler<TArgs> : IDisposable
    {
        private Timer? _timer;
        private readonly TimeSpan _interval;
        private int _isPending;
        private StrongBox<TArgs>? _latestArgs;

        public Throttler(TimeSpan interval, Action<TArgs> callback)
        {
            _interval = interval;
            _timer = new Timer((_) =>
            {
                if (Interlocked.Exchange(ref _isPending, 0) == 1)
                {
                    callback(_latestArgs!.Value);
                }
            }, null, Timeout.Infinite, Timeout.Infinite);
        }

        public void Dispose()
        {
            var timer = Interlocked.Exchange(ref _timer, null);
            timer?.Dispose();
        }

        public void Notify(TArgs args)
        {
            _latestArgs = new StrongBox<TArgs>(args);
            if (Interlocked.Exchange(ref _isPending, 1) == 0)
            {
                _timer?.Change(_interval, Timeout.InfiniteTimeSpan);
            }
        }
    }
}

