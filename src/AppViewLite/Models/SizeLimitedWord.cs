using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Text;

namespace AppViewLite.Models
{
    [InlineArray(MaxLength)]
    public struct SizeLimitedWord : IComparable<SizeLimitedWord>, IEquatable<SizeLimitedWord>
    {
        private byte _byte;

        public static SizeLimitedWord Create(string str)
        {
            ReadOnlySpan<byte> bytes = Encoding.UTF8.GetBytes(str);
            if (bytes.Length > MaxLength) bytes = bytes.Slice(0, MaxLength);
            var result = new SizeLimitedWord();
            bytes.CopyTo(result);
            return result;
        }

        [UnscopedRef] public ReadOnlySpan<byte> AllBytes => this;

        public int CompareTo(SizeLimitedWord other)
        {
            return this.AllBytes.SequenceCompareTo(other.AllBytes);
        }

        public override bool Equals([NotNullWhen(true)] object? obj)
        {
            if (obj is SizeLimitedWord other)
            {
                return this.Equals(other);
            }
            return false;
        }

        public bool Equals(SizeLimitedWord other)
        {
            return this.AsUint64 == other.AsUint64;
        }

        public override int GetHashCode()
        {
            return AsUint64.GetHashCode();
        }

        private ulong AsUint64 => Unsafe.BitCast<SizeLimitedWord, ulong>(this);

        const int MaxLength = 8;
    }
}

