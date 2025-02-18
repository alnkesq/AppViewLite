using System;
using System.Runtime.InteropServices;

namespace AppViewLite.Numerics
{
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public record struct UInt48(uint High, ushort Low) : IComparable<UInt48>
    {
        public int CompareTo(UInt48 other)
        {
            var z = High.CompareTo(other.High);
            if (z != 0) return z;
            return Low.CompareTo(other.Low);
        }

        public static implicit operator ulong(UInt48 n) => ((ulong)n.High << 16) | n.Low;
        public static explicit operator uint(UInt48 n) => (uint)(ulong)n;
        public static explicit operator ushort(UInt48 n) => (ushort)(ulong)n;
        public static explicit operator byte(UInt48 n) => (byte)(ulong)n;

        public static implicit operator long(UInt48 n) => (long)(ulong)n;
        public static explicit operator int(UInt48 n) => (int)(ulong)n;
        public static explicit operator short(UInt48 n) => (short)(ulong)n;
        public static explicit operator sbyte(UInt48 n) => (sbyte)(ulong)n;

        public static explicit operator UInt48(ulong n) => new(checked((uint)(n >> 16)), unchecked((ushort)n));
        public static implicit operator UInt48(uint n) => (UInt48)(ulong)n;
        public static implicit operator UInt48(ushort n) => (UInt48)(ulong)n;
        public static implicit operator UInt48(byte n) => (UInt48)(ulong)n;

        public static explicit operator UInt48(long n) => (UInt48)(ulong)n;
        public static implicit operator UInt48(int n) => (UInt48)(ulong)n;
        public static implicit operator UInt48(short n) => (UInt48)(ulong)n;
        public static implicit operator UInt48(sbyte n) => (UInt48)(ulong)n;

        public readonly static UInt48 MaxValue = new UInt48(uint.MaxValue, ushort.MaxValue);
        public readonly static ulong MaxValueAsUInt64 = MaxValue;

        public override string ToString()
        {
            return ((ulong)this).ToString();
        }
    }
}

