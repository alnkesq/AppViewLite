using System;
using System.Runtime.InteropServices;

namespace AppViewLite.Numerics
{
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public record struct UInt40(uint High, byte Low) : IComparable<UInt40>
    {
        public int CompareTo(UInt40 other)
        {
            var z = High.CompareTo(other.High);
            if (z != 0) return z;
            return Low.CompareTo(other.Low);
        }

        public static implicit operator ulong(UInt40 n) => ((ulong)n.High << 8) | n.Low;
        public static explicit operator UInt40(ulong n) => new(checked((uint)(n >> 8)), unchecked((byte)n));
        public static implicit operator UInt40(uint n) => (UInt40)(ulong)n;

        public readonly static UInt40 MaxValue = new UInt40(uint.MaxValue, byte.MaxValue);
        public readonly static ulong MaxValueAsUInt64 = MaxValue;
        public override string ToString()
        {
            return ((ulong)this).ToString();
        }
    }
}

