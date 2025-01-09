using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Drawing;
using System.Linq;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace AppViewLite.Numerics
{
    [StructLayout(LayoutKind.Explicit)]
    [SuppressMessage("Design", "CA1036:Override methods on comparable types")]
    public readonly struct DuckDbUuid : IEquatable<DuckDbUuid>, IComparable<DuckDbUuid>, IParsable<DuckDbUuid>
    {
        private DuckDbUuid(long upper, ulong lower)
        {
            this.upper = upper;
            this.lower = lower;
        }

        public static DuckDbUuid FromUpperLower(long upper, ulong lower) => new DuckDbUuid(upper, lower);
        public static DuckDbUuid FromUpperLowerFlat(ulong upper, ulong lower) => new DuckDbUuid((long)(upper ^ (1UL << 63)), lower);

        public DuckDbUuid(ReadOnlySpan<byte> uuidBigEndian)
        {
            if (uuidBigEndian.Length != 16) throw new ArgumentException();

            lower = BinaryPrimitives.ReadUInt64BigEndian(uuidBigEndian.Slice(8));
            upper = BinaryPrimitives.ReadInt64BigEndian(uuidBigEndian);

            // As per duckdb/src/common/types/uuid.cpp
            upper ^= (1L << 63);

        }



        [FieldOffset(0)]
        private readonly ulong lower;
        [FieldOffset(8)]
        private readonly long upper;

        public override int GetHashCode()
        {
            return HashCode.Combine(lower, upper);
        }

        public bool Equals(DuckDbUuid other)
        {
            return this.upper == other.upper && this.lower == other.lower;
        }

        public override bool Equals([NotNullWhen(true)] object? obj)
        {
            return obj is DuckDbUuid other && this == other;
        }

        public static bool operator ==(DuckDbUuid a, DuckDbUuid b) => a.Equals(b);
        public static bool operator !=(DuckDbUuid a, DuckDbUuid b) => !a.Equals(b);

        public int CompareTo(DuckDbUuid other)
        {
            var cmp = this.upper.CompareTo(other.upper);
            if (cmp != 0) return cmp;
            return this.lower.CompareTo(other.lower);
        }

        public void WriteBytes(Span<byte> destinationBigEndian)
        {
            BinaryPrimitives.WriteUInt64BigEndian(destinationBigEndian.Slice(8), this.lower);
            var upperFlipBack = this.upper;

            // As per duckdb/src/common/types/uuid.cpp
            upperFlipBack ^= (1L << 63);
            BinaryPrimitives.WriteInt64BigEndian(destinationBigEndian, upperFlipBack);
        }

        public byte[] ToByteArray()
        {
            var result = new byte[16];
            WriteBytes(result);
            return result;
        }

        public static DuckDbUuid Parse(ReadOnlySpan<char> str) => Guid.Parse(str);

        public static DuckDbUuid Parse(string str) => Guid.Parse(str);

        [SkipLocalsInit]
        public static implicit operator Guid(DuckDbUuid uuid)
        {
            Span<byte> buffer = stackalloc byte[16];
            uuid.WriteBytes(buffer);
            return new Guid(buffer, bigEndian: true);
        }


        [SkipLocalsInit]
        public static implicit operator DuckDbUuid(Guid guid)
        {
            Span<byte> buffer = stackalloc byte[16];

            guid.TryWriteBytes(buffer, bigEndian: true, out _);
            return new DuckDbUuid(buffer);
        }

        public static bool operator <(DuckDbUuid left, DuckDbUuid right)
        {
            return left.CompareTo(right) < 0;
        }

        public static bool operator <=(DuckDbUuid left, DuckDbUuid right)
        {
            return left.CompareTo(right) <= 0;
        }

        public static bool operator >(DuckDbUuid left, DuckDbUuid right)
        {
            return left.CompareTo(right) > 0;
        }

        public static bool operator >=(DuckDbUuid left, DuckDbUuid right)
        {
            return left.CompareTo(right) >= 0;
        }

        public override string ToString()
        {
            return ((Guid)this).ToString();
        }
        public string ToString([StringSyntax("GuidFormat")] string? format)
        {
            return ((Guid)this).ToString(format);
        }

        public ulong GetLower()
        {
            return lower;
        }
        public long GetUpper()
        {
            return upper;
        }

        public ulong GetUpperFlat()
        {
            return (ulong)upper ^ (1UL << 63);
        }
        public static DuckDbUuid Parse(string s, IFormatProvider? provider)
        {
            return Guid.Parse(s, provider);
        }
        public static bool TryParse([NotNullWhen(true)] string? s, [MaybeNullWhen(false)] out DuckDbUuid result)
        {
            return TryParse(s, null, out result);
        }

        public static bool TryParse(ReadOnlySpan<char> s, [MaybeNullWhen(false)] out DuckDbUuid result)
        {
            return TryParse(s, null, out result);
        }

        public static bool TryParse([NotNullWhen(true)] string? s, IFormatProvider? provider, [MaybeNullWhen(false)] out DuckDbUuid result)
        {

            return TryParse(s.AsSpan(), provider, out result);
        }

        public static bool TryParse(ReadOnlySpan<char> s, IFormatProvider? provider, [MaybeNullWhen(false)] out DuckDbUuid result)
        {

            if (Guid.TryParse(s, provider, out var guid))
            {
                result = guid;
                return true;
            }

            result = default;
            return false;
        }
    }
}
