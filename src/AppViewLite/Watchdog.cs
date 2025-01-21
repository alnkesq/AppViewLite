using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace AppViewLite
{

    public class Watchdog
    {
        private readonly Timer _timer;
        private readonly TimeSpan _timeout;

        public Watchdog(TimeSpan timeout, Action callback)
        {
            _timeout = timeout;
            _timer = new Timer((_) => 
            {
                _timer!.Dispose();
                callback();
            }, null, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
        }

        public static Watchdog Create(TimeSpan ts, Action callback)
        {
            var w = new Watchdog(ts, callback);
            w.Kick();
            return w;
        }

        public void Kick()
        {
            _timer.Change(_timeout, Timeout.InfiniteTimeSpan);
        }

 

    }

}

