using Microsoft.Extensions.Logging;
using AppViewLite;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;

namespace AppViewLite
{
    internal class OutOfOrderCompletionTracker
    {
        private long lastDefinitelyProcessedEvent;
        private readonly HashSet<long> completed = new();
        private long previousGeneratedEventId;

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void OnEventGenerated(long eventId)
        {
            BlueskyRelationships.Assert(eventId == previousGeneratedEventId + 1);
            previousGeneratedEventId = eventId;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void OnEventProcessed(long eventId)
        {
            BlueskyRelationships.Assert(eventId > lastDefinitelyProcessedEvent);

            var added = completed.Add(eventId);
            BlueskyRelationships.Assert(added);

            while (completed.Remove(lastDefinitelyProcessedEvent + 1))
            {
                lastDefinitelyProcessedEvent++;
            }
        }



        public long LastDefinitelyProcessedEvent
        {
            get
            {
                lock (this)
                {
                    return lastDefinitelyProcessedEvent;
                }
            }
        }
    }
}

