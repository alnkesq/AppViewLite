using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite
{
    public record struct PerformanceSnapshot(long StopwatchTicks, int Gc0Count, int Gc1Count, int Gc2Count)
    {
        public static PerformanceSnapshot Capture()
        {
            var result = new PerformanceSnapshot
            {
                StopwatchTicks = Stopwatch.GetTimestamp(),
                Gc0Count = GC.CollectionCount(0),
                Gc1Count = GC.CollectionCount(1),
                Gc2Count = GC.CollectionCount(2),
            };
            return result;
        }
    }
}

