using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace AppViewLite.Numerics
{
    [InlineArray(MaxLength)]
    public struct SizeLimitedWord8 : IComparable<SizeLimitedWord8>, IEquatable<SizeLimitedWord8>
    {
        private byte _byte;

        public static SizeLimitedWord8 Create(string? str)
        {
            if (string.IsNullOrEmpty(str)) return default;
            return Create(Encoding.UTF8.GetBytes(str));
        }

        public static SizeLimitedWord8 Create(ReadOnlySpan<byte> utf8)
        {
            if (utf8.Length > MaxLength) utf8 = utf8.Slice(0, MaxLength);
            var result = new SizeLimitedWord8();
            Span<byte> resultAsSpan = result;
            utf8.CopyTo(resultAsSpan);
            return result;
        }
        public int Length
        {
            get
            {
                var index = AllBytes.IndexOf((byte)0);
                return index != -1 ? index : MaxLength;
            }
        }

        public bool IsEmpty => AsUint64 == 0;

        [UnscopedRef] public ReadOnlySpan<byte> AllBytes => this;
        [UnscopedRef] public ReadOnlySpan<byte> Bytes => AllBytes.Slice(0, Length);
        public int CompareTo(SizeLimitedWord8 other)
        {
            return AllBytes.SequenceCompareTo(other.AllBytes);
        }

        public override string ToString()
        {
            return Encoding.UTF8.GetString(Bytes);
        }

        public override bool Equals([NotNullWhen(true)] object? obj)
        {
            if (obj is SizeLimitedWord8 other)
            {
                return Equals(other);
            }
            return false;
        }

        public bool Equals(SizeLimitedWord8 other)
        {
            return AsUint64 == other.AsUint64;
        }

        public override int GetHashCode()
        {
            return AsUint64.GetHashCode();
        }

        private ulong AsUint64 => Unsafe.BitCast<SizeLimitedWord8, ulong>(this);

        const int MaxLength = 8;

        public readonly static SizeLimitedWord8 MaxValue = Unsafe.BitCast<ulong, SizeLimitedWord8>(ulong.MaxValue);

        public SizeLimitedWord8 GetMaxExclusiveForPrefixRange()
        {
            // hello -> hellp

            var maxExclusive = Bytes.ToArray();
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

