using System;

namespace AppViewLite.Numerics
{
    public record struct ApproximateDateTime32(uint Value) : IComparable<ApproximateDateTime32>
    {
        public int CompareTo(ApproximateDateTime32 other)
        {
            return Value.CompareTo(other.Value);
        }

        private readonly static DateTime BaseTime = new DateTime(2023, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        private readonly static TimeSpan TickDuration = TimeSpan.FromSeconds(0.2);

        public static implicit operator DateTime(ApproximateDateTime32 d) => BaseTime + new TimeSpan(TickDuration.Ticks * d.Value);
        public static explicit operator ApproximateDateTime32(DateTime d)
        {
            if (d < BaseTime) throw new ArgumentOutOfRangeException();
            if (d > MaxValue) throw new ArgumentOutOfRangeException();
            var v = (d - BaseTime).Ticks / TickDuration.Ticks;
            return new ApproximateDateTime32((uint)v);
        }


        public readonly ApproximateDateTime32 AddTicks(uint ticks) => new ApproximateDateTime32(Value + ticks);

        public readonly static ApproximateDateTime32 MinValue = new ApproximateDateTime32(uint.MinValue);
        public readonly static ApproximateDateTime32 MaxValue = new ApproximateDateTime32(uint.MaxValue);

        public readonly override string ToString()
        {
            return ((DateTime)this).ToString();
        }
    }
}

