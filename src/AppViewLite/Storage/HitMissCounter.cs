using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace AppViewLite
{
    public class HitMissCounter
    {
        private long hitCount;
        private long missCount;
        public void OnHit()
        {
            Interlocked.Increment(ref hitCount);
        }

        public void OnMiss()
        {
            Interlocked.Increment(ref missCount);
        }

        public double HitRatio
        {
            get
            {
                var hit = Interlocked.Read(ref hitCount);
                var miss = Interlocked.Read(ref missCount);

                var total = hit + miss;
                if (total == 0) return 0;
                return (double)hit / total;
            }
        }
    }
}

