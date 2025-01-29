using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace AppViewLite.Models
{
    [InlineArray(MaxLength)]
    public struct SizeLimitedWord2 : IComparable<SizeLimitedWord2>, IEquatable<SizeLimitedWord2>
    {
        private byte _byte;

        public static SizeLimitedWord2 Create(string? str)
        {
            if (string.IsNullOrEmpty(str)) return default;
            return Create(Encoding.UTF8.GetBytes(str));
        }

        public static SizeLimitedWord2 Create(ReadOnlySpan<byte> utf8)
        {
            if (utf8.Length > MaxLength) utf8 = utf8.Slice(0, MaxLength);
            var result = new SizeLimitedWord2();
            Span<byte> resultAsSpan = result;
            utf8.CopyTo(resultAsSpan);
            return result;
        }

        public int Length => AllBytes.IndexOf((byte)0);
        public bool IsEmpty => AsUint16 == 0;

        [UnscopedRef] public ReadOnlySpan<byte> AllBytes => this;
        [UnscopedRef] public ReadOnlySpan<byte> Bytes => AllBytes.Slice(0, Length);
        public int CompareTo(SizeLimitedWord2 other)
        {
            return this.AllBytes.SequenceCompareTo(other.AllBytes);
        }

        public override string ToString()
        {
            return Encoding.UTF8.GetString(Bytes);
        }

        public override bool Equals([NotNullWhen(true)] object? obj)
        {
            if (obj is SizeLimitedWord2 other)
            {
                return this.Equals(other);
            }
            return false;
        }

        public bool Equals(SizeLimitedWord2 other)
        {
            return this.AsUint16 == other.AsUint16;
        }

        public override int GetHashCode()
        {
            return AsUint16.GetHashCode();
        }

        private ushort AsUint16 => Unsafe.BitCast<SizeLimitedWord2, ushort>(this);

        const int MaxLength = 2;

        public readonly static SizeLimitedWord2 MaxValue = Unsafe.BitCast<ushort, SizeLimitedWord2>(ushort.MaxValue);

        public SizeLimitedWord2 GetMaxExclusiveForPrefixRange()
        {
            // hello -> hellp

            var maxExclusive = this.Bytes.ToArray();
            var i = maxExclusive.Length - 1;
            while (true)
            {
                if (i < 0) return MaxValue;

                if (maxExclusive[i] == byte.MaxValue)
                {
                    maxExclusive[i] = 0;
                    i--;
                }
                else
                {
                    maxExclusive[i] = (byte)(maxExclusive[i] + 1);
                    break;
                }
            }
            return Create(maxExclusive);
        }
    }
}

