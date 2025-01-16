using System;

namespace AppViewLite.Numerics
{
    public record struct ApproximateDateTime24(UInt24 Value) : IComparable<ApproximateDateTime24>
    {
        public int CompareTo(ApproximateDateTime24 other)
        {
            return Value.CompareTo(other.Value);
        }

        private readonly static DateTime BaseTime = new DateTime(2023, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        private readonly static TimeSpan TickDuration = TimeSpan.FromSeconds(20);

        public static implicit operator DateTime(ApproximateDateTime24 d) => BaseTime + new TimeSpan(TickDuration.Ticks * d.Value);
        public static explicit operator ApproximateDateTime24(DateTime d)
        {
            if (d < MinValueAsDateTime) throw new ArgumentOutOfRangeException();
            if (d > MaxValueAsDateTime) throw new ArgumentOutOfRangeException();
            var v = (d - BaseTime).Ticks / TickDuration.Ticks;
            return new ApproximateDateTime24((UInt24)v);
        }


        public readonly ApproximateDateTime24 AddTicks(UInt24 ticks) => new ApproximateDateTime24(Value + ticks);

        public readonly static ApproximateDateTime24 MinValue = new ApproximateDateTime24(UInt24.MinValue);
        public readonly static ApproximateDateTime24 MaxValue = new ApproximateDateTime24(UInt24.MaxValue);
        public readonly static DateTime MinValueAsDateTime = MinValue;
        public readonly static DateTime MaxValueAsDateTime = MaxValue;

        public readonly override string ToString()
        {
            return ((DateTime)this).ToString();
        }
    }
}

