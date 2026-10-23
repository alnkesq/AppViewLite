using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;

namespace AppViewLite
{
    internal class OutOfOrderCompletionTracker
    {
        private readonly Dictionary<long, long?> eventIdToSeq = new();
        private long lastDefinitelyProcessedEventId;
        private long? lastDefinitelyProcessedSeq;
        private long lastGeneratedEventId;


        public long? LastDefinitelyProcessedSeq
        {
            [MethodImpl(MethodImplOptions.Synchronized)]
            get
            {
                return lastDefinitelyProcessedSeq;
            }
        }

        public long OnEventGenerated()
        {
            return Interlocked.Increment(ref this.lastGeneratedEventId);
        }


        [MethodImpl(MethodImplOptions.Synchronized)]
        public void OnEventProcessed(long eventId, long? seq)
        {
            if (eventId <= lastDefinitelyProcessedEventId)
            {
                throw new InvalidOperationException("OnEventProcessed was called with an eventId that was already supposedly processed.");
            }

            eventIdToSeq.Add(eventId, seq);
            UpdateLastDefinitelyProcessedEvent();

        }

        private void UpdateLastDefinitelyProcessedEvent()
        {
            while (eventIdToSeq.TryGetValue(lastDefinitelyProcessedEventId + 1, out var seq))
            {
                lastDefinitelyProcessedEventId++;
                if (seq != null)
                {
                    lastDefinitelyProcessedSeq = seq;
                }

                eventIdToSeq.Remove(lastDefinitelyProcessedEventId);
            }
        }
    }
}



