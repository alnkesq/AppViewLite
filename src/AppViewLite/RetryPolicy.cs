using System;
using System.Diagnostics;
using System.Net.WebSockets;

namespace AppViewLite
{
    public class RetryPolicy
    {
        private Func<Exception, TimeSpan> getDelayAfterException;
        public RetryPolicy(Func<Exception, TimeSpan> getDelayAfterException)
        {
            this.getDelayAfterException = getDelayAfterException;
        }

        public TimeSpan OnException(Exception ex) => getDelayAfterException(ex);

        public static RetryPolicy CreateConstant(TimeSpan timeSpan)
        {
            return new RetryPolicy(x => timeSpan);
        }

        public static RetryPolicy CreateForUnreliableServer()
        {
            TimeSpan lastRetryDelay = default;
            long? previousFailureTicks = null;
            return new RetryPolicy(ex =>
            {
                var serverIsUnlikelyToBeAlive = false;
                if (ex is WebSocketException ws)
                {

                    serverIsUnlikelyToBeAlive = ws.WebSocketErrorCode != WebSocketError.ConnectionClosedPrematurely;
                }
                var t = ex.GetType().FullName;
                var currentTime = Stopwatch.GetTimestamp();
                TimeSpan? timeSincePreviousFailure = previousFailureTicks != null ? Stopwatch.GetElapsedTime(currentTime - previousFailureTicks.Value) : null;
                if (lastRetryDelay == default || timeSincePreviousFailure?.TotalMinutes > 10)
                {

                    lastRetryDelay = TimeSpan.FromMinutes(serverIsUnlikelyToBeAlive ? 30 : 1);
                }
                else
                {
                    lastRetryDelay *= 2;
                }
                previousFailureTicks = currentTime;
                return lastRetryDelay;
            });
        }
    }

}

