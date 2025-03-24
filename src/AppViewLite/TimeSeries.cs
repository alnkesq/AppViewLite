using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace AppViewLite
{
    public class TimeSeries
    {
        private readonly int[] slots;
        public readonly TimeSpan Interval;
        public readonly TimeSpan TotalTime;
        private readonly int properSlotCount;

        public TimeSeries(TimeSpan totalTime, TimeSpan interval, TimeSpan extraTime)
        {
            this.Interval = interval;
            this.TotalTime = totalTime;
            this.properSlotCount = (int)(totalTime.Ticks / interval.Ticks);
            var extraSlotCount = (int)(extraTime.Ticks / interval.Ticks);
            slots = new int[properSlotCount + extraSlotCount];
        }

        private int CurrentSlotIndex => (int)((Stopwatch.GetElapsedTime(0).Ticks / Interval.Ticks) % slots.Length);

        public static async Task StartClearThread(TimeSpan interval, TimeSpan clearFutureStart, TimeSpan clearFutureLength, TimeSeries[] timeSeries)
        {
            while (true)
            {
                await Task.Delay(interval).ConfigureAwait(false);
                foreach (var series in timeSeries)
                {
                    var slotsToSkip = (int)(clearFutureStart.Ticks / series.Interval.Ticks);
                    var slotsToClear = (int)(clearFutureLength.Ticks / series.Interval.Ticks);
                    ClearSlots(series, (series.CurrentSlotIndex + slotsToSkip) % series.slots.Length, slotsToClear);
                }
            }
        }

        private static void ClearSlots(TimeSeries series, int startIndex, int length)
        {
            var end = series.slots.AsSpan(startIndex);
            end = end.Slice(0, Math.Min(end.Length, length));
            end.Clear();
            var wraparound = length - end.Length;
            if (wraparound != 0)
            {
                var start = series.slots.AsSpan(0, wraparound);
                start.Clear();
            }
        }

        public void Increment()
        {
            Interlocked.Increment(ref slots[CurrentSlotIndex]);
        }

        public void Set(int value)
        {
            this.slots[CurrentSlotIndex] = value;
        }
        public (float[] Buckets, TimeSpan ActualBucketDuration) GetChart(int bucketCount, TimeSpan scope)
        {
            var bucketDuration = new TimeSpan(scope.Ticks / bucketCount);
            var slotsPerBucket = (int)Math.Max(1, Math.Round((double)bucketDuration.Ticks / Interval.Ticks));
            bucketDuration = Interval * slotsPerBucket;

            var buckets = new long[bucketCount];

            var currentBucketIndex = buckets.Length - 1;
            var currentSlotIndex = (this.CurrentSlotIndex - 1 + slots.Length) % slots.Length;
            var currentBucketSize = 0;
            var processedSlots = 0;
            while (true)
            {

                buckets[currentBucketIndex] += slots[currentSlotIndex];
                currentBucketSize++;
                processedSlots++;
                if (processedSlots == properSlotCount) break;

                if (currentBucketSize == slotsPerBucket)
                {
                    if (currentBucketIndex == 0) break;
                    currentBucketSize = 0;
                    currentBucketIndex--;
                }

                if (currentSlotIndex == 0) currentSlotIndex = slots.Length;
                currentSlotIndex--;
            }

            var smoothed = new float[buckets.Length];
            //var weights = new float[] { 1, 2, 3, 4 };
            var weights = new float[] { 1, 1, 1, 1 };
            var weightSum = weights.Sum();
            for (int i = 0; i < weights.Length; i++)
            {
                weights[i] /= weightSum;
            }
            for (int i = 0; i < buckets.Length; i++)
            {
                var curr = buckets[i];
                var prev = i >= 1 ? buckets[i - 1] : curr;
                var prev2 = i >= 2 ? buckets[i - 2] : prev;
                var prev3 = i >= 3 ? buckets[i - 3] : prev2;
                smoothed[i] =
                    prev3 * weights[0] +
                    prev2 * weights[1] +
                    prev * weights[2] +
                    curr * weights[3];
            }
            return (smoothed, bucketDuration);

        }

    }
}

