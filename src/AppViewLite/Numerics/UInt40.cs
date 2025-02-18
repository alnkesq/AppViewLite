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
        public static explicit operator uint(UInt40 n) => (uint)(ulong)n;
        public static explicit operator ushort(UInt40 n) => (ushort)(ulong)n;
        public static explicit operator byte(UInt40 n) => (byte)(ulong)n;

        public static implicit operator long(UInt40 n) => (long)(ulong)n;
        public static explicit operator int(UInt40 n) => (int)(ulong)n;
        public static explicit operator short(UInt40 n) => (short)(ulong)n;
        public static explicit operator sbyte(UInt40 n) => (sbyte)(ulong)n;

        public static explicit operator UInt40(ulong n) => new(checked((uint)(n >> 8)), unchecked((byte)n));
        public static implicit operator UInt40(uint n) => (UInt40)(ulong)n;
        public static implicit operator UInt40(ushort n) => (UInt40)(ulong)n;
        public static implicit operator UInt40(byte n) => (UInt40)(ulong)n;

        public static explicit operator UInt40(long n) => (UInt40)(ulong)n;
        public static implicit operator UInt40(int n) => (UInt40)(ulong)n;
        public static implicit operator UInt40(short n) => (UInt40)(ulong)n;
        public static implicit operator UInt40(sbyte n) => (UInt40)(ulong)n;

        public readonly static UInt40 MaxValue = new UInt40(uint.MaxValue, byte.MaxValue);
        public readonly static ulong MaxValueAsUInt64 = MaxValue;
        public override string ToString()
        {
            return ((ulong)this).ToString();
        }
    }
}

