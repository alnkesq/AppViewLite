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

        public static implicit operator ulong(UInt24 n) => (ulong)(uint)n;
        public static implicit operator uint(UInt24 n) => ((uint)n.High << 8) | n.Low;
        public static explicit operator ushort(UInt24 n) => (ushort)(uint)n;
        public static explicit operator byte(UInt24 n) => (byte)(uint)n;

        public static implicit operator long(UInt24 n) => (long)(uint)n;
        public static implicit operator int(UInt24 n) => (int)(uint)n;
        public static explicit operator short(UInt24 n) => (short)(uint)n;
        public static explicit operator sbyte(UInt24 n) => (sbyte)(uint)n;

        public static explicit operator UInt24(ulong n) => (UInt24)(uint)n;
        public static explicit operator UInt24(uint n) => new(checked((ushort)(n >> 8)), unchecked((byte)n));
        public static implicit operator UInt24(ushort n) => (UInt24)(uint)n;
        public static implicit operator UInt24(byte n) => (UInt24)(uint)n;

        public static explicit operator UInt24(long n) => (UInt24)(uint)n;
        public static explicit operator UInt24(int n) => (UInt24)(uint)n;
        public static implicit operator UInt24(short n) => (UInt24)(uint)n;
        public static implicit operator UInt24(sbyte n) => (UInt24)(uint)n;

        public readonly static UInt24 MinValue = new UInt24(ushort.MinValue, byte.MinValue);
        public readonly static UInt24 MaxValue = new UInt24(ushort.MaxValue, byte.MaxValue);
        public readonly static ulong MaxValueAsUInt32 = MaxValue;

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

