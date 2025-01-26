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
        public static implicit operator long(UInt48 n) => (long)(ulong)n;
        public static explicit operator UInt48(ulong n) => new(checked((uint)(n >> 16)), unchecked((ushort)n));
        public static implicit operator UInt48(uint n) => (UInt48)(ulong)n;

        public readonly static UInt48 MaxValue = new UInt48(uint.MaxValue, ushort.MaxValue);
        public readonly static ulong MaxValueAsUInt64 = MaxValue;

        public override string ToString()
        {
            return ((ulong)this).ToString();
        }
    }
}

