using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite
{
    public record struct PerformanceSnapshot(long StopwatchTicks, int Gc0Count, int Gc1Count, int Gc2Count, long AllocatedBytes)
    {
        public int MaxGcGeneration =>
                Gc2Count != 0 ? 2 :
                Gc1Count != 0 ? 1 :
                Gc0Count != 0 ? 0 :
                -1;

        public static PerformanceSnapshot Capture()
        {
            var allocatedBytes = GC.GetAllocatedBytesForCurrentThread();
            var result = new PerformanceSnapshot
            {
                StopwatchTicks = Stopwatch.GetTimestamp(),
                Gc0Count = GC.CollectionCount(0),
                Gc1Count = GC.CollectionCount(1),
                Gc2Count = GC.CollectionCount(2),
                AllocatedBytes = allocatedBytes,
            };
            return result;
        }

        public static PerformanceSnapshot operator -(PerformanceSnapshot end, PerformanceSnapshot begin)
        {
            return new PerformanceSnapshot(
                end.StopwatchTicks - begin.StopwatchTicks,
                end.Gc0Count - begin.Gc0Count,
                end.Gc1Count - begin.Gc1Count,
                end.Gc2Count - begin.Gc2Count,
                end.AllocatedBytes - begin.AllocatedBytes
                );
        }
    }
}

