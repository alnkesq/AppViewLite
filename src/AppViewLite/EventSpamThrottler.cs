using AppViewLite;
using System;
using System.Collections.Immutable;
using System.Globalization;
using System.IO.Hashing;
using System.Linq;
using System.Numerics;
using System.Runtime.InteropServices;
using System.Threading;

namespace AppViewLite
{
    public abstract class EventSpamThrottlerBase<TActor, TPerActorEventId>
        where TActor : unmanaged
        where TPerActorEventId : unmanaged
    {
        public long CountSpam;
        public long CountNonSpam;

        protected abstract bool TryAddEventCore(TActor actor, TPerActorEventId perActorEventId, DateTime date);
        public void AddEvent(TActor actor, TPerActorEventId perActorEventId, DateTime date)
        {
            if (!TryAddEvent(actor, perActorEventId, date))
                throw new UnexpectedFirehoseDataException($"Too many events for actor {actor} for the specified time slot.");
        }
        public bool TryAddEvent(TActor actor, TPerActorEventId perActorEventId, DateTime date)
        {
            if (TryAddEventCore(actor, perActorEventId, date))
            {
                Interlocked.Increment(ref CountNonSpam);
                return true;
            }
            else 
            {
                Interlocked.Increment(ref CountSpam);
                return false;
            }
        }

        public virtual object GetCounters() => new { CountSpam, CountNonSpam };

    }
    public class EventSpamThrottler<TActor, TPerActorEventId> : EventSpamThrottlerBase<TActor, TPerActorEventId>
        where TActor : unmanaged
        where TPerActorEventId : unmanaged
    {
        private readonly TimeSpan slotTime;
        private readonly int maxOnBitsPerSlot;
        private readonly ConcurrentFullEvictionCache<(TActor Actor, uint Slot), ulong> _seenEvents;
        private readonly int salt = Random.Shared.Next();
        private readonly int dictionarySize;
        public EventSpamThrottler(TimeSpan slotTime, int maxOnBitsPerSlot, int dictionarySize)
        {
            // Note: maxOnBitsPerSlot must be significantly smaller than 64 (collisions near the 64th on bit will be very frequent: undercounting)
            if (maxOnBitsPerSlot <= 0 || maxOnBitsPerSlot > 64) throw new ArgumentException();
            this.slotTime = slotTime;
            this.maxOnBitsPerSlot = maxOnBitsPerSlot;
            this.dictionarySize = dictionarySize;
            this._seenEvents = new(dictionarySize);
        }


        protected override bool TryAddEventCore(TActor actor, TPerActorEventId perActorEventId, DateTime date)
        {
            var actorSlot = (actor, (uint)(date.Ticks / slotTime.Ticks));

            var hashMaterial = (actorSlot, perActorEventId, salt);
            var fingerprint = (int)(XxHash64.HashToUInt64(MemoryMarshal.AsBytes([hashMaterial])) % 64);
            var fingerprintFlag = 1UL << fingerprint;

            var result = _seenEvents.AddOrUpdate(actorSlot, fingerprintFlag, (_, prev) => fingerprintFlag | prev);

            var bits = BitOperations.PopCount(result);
            return bits <= maxOnBitsPerSlot;
        }

        public override object GetCounters()
        {
            return new { CountSpam, CountNonSpam, _seenEvents.LastResetAgo, Parameters = maxOnBitsPerSlot + "/" + slotTime.TotalSeconds.ToString(CultureInfo.InvariantCulture) + "/" + dictionarySize};
        }

    }

    public class CombinedEventSpamThrottler<TActor, TPerActorEventId>(ImmutableArray<EventSpamThrottlerBase<TActor, TPerActorEventId>> inner)
        : EventSpamThrottlerBase<TActor, TPerActorEventId>
        where TActor : unmanaged
        where TPerActorEventId : unmanaged
    {
        protected override bool TryAddEventCore(TActor actor, TPerActorEventId perActorEventId, DateTime date)
        {
            foreach (var item in inner)
            {
                if (!item.TryAddEvent(actor, perActorEventId, date))
                    return false;
            }
            return true;
        }
        public override object GetCounters() => new { CountSpam, CountNonSpam, Inner = inner.Select(x => x.GetCounters()).ToArray() };
    }
}

