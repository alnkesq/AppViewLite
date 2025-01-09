using System;
using System.Runtime.InteropServices;

namespace AppViewLite.Numerics
{
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public record struct UInt24(ushort High, byte Low) : IComparable<UInt24>
    {
        public int CompareTo(UInt24 other)
        {
            var z = High.CompareTo(other.High);
            if (z != 0) return z;
            return Low.CompareTo(other.Low);
        }

        public static implicit operator uint(UInt24 n) => (uint)n.High << 8 | n.Low;
        public static explicit operator UInt24(uint n) => new(checked((ushort)(n >> 8)), unchecked((byte)n));
        public static implicit operator UInt24(ushort n) => (UInt24)(uint)n;

        public readonly static UInt24 MinValue = new UInt24(ushort.MinValue, byte.MinValue);
        public readonly static UInt24 MaxValue = new UInt24(ushort.MaxValue, byte.MaxValue);

        public static UInt24 operator +(UInt24 a, UInt24 b)
        {
            var aa = (uint)a;
            var bb = (uint)b;
            var sum = aa + bb;
            return (UInt24)sum;
        }

        public override string ToString()
        {
            return ((uint)this).ToString();
        }
    }
}

