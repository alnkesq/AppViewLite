using System;

namespace AppViewLite.Numerics
{
    public record struct ApproximateDateTime16(ushort Value) : IComparable<ApproximateDateTime16>
    {
        public int CompareTo(ApproximateDateTime16 other)
        {
            return Value.CompareTo(other.Value);
        }

        private readonly static DateTime BaseTime = new DateTime(2022, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        private readonly static TimeSpan TickDuration = TimeSpan.FromHours(2);

        public static implicit operator DateTime(ApproximateDateTime16 d) => BaseTime + new TimeSpan(TickDuration.Ticks * d.Value);
        public static explicit operator ApproximateDateTime16(DateTime d)
        {
            if (d < MinValueAsDateTime) throw new UnexpectedFirehoseDataException("Dates before 2022 are not supported: " + d);
            if (d > MaxValueAsDateTime) throw new ArgumentOutOfRangeException("Date is too much into the future for ApproximateDateTime16");
            var v = (d - BaseTime).Ticks / TickDuration.Ticks;
            return new ApproximateDateTime16((ushort)v);
        }


        public readonly ApproximateDateTime16 AddTicks(short ticks) => new ApproximateDateTime16((ushort)((long)Value + ticks));

        public readonly static ApproximateDateTime16 MinValue = new ApproximateDateTime16(ushort.MinValue);
        public readonly static ApproximateDateTime16 MaxValue = new ApproximateDateTime16(ushort.MaxValue);
        public readonly static DateTime MinValueAsDateTime = MinValue;
        public readonly static DateTime MaxValueAsDateTime = MaxValue;

        public readonly override string ToString()
        {
            return ((DateTime)this).ToString();
        }
    }
}

