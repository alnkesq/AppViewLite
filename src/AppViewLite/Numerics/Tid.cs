using System;

namespace AppViewLite.Numerics
{
    public record struct Tid(long TidValue) : IComparable<Tid>
    {
        public static Tid Parse(string rkey)
        {
            if (!TryParse(rkey, out var r))
            {
                if (long.TryParse(rkey, out _)) throw new UnexpectedFirehoseDataException("Decimal timestamp IDs are not supported: " + rkey);
                else throw new UnexpectedFirehoseDataException("Timestamp ID could not be parsed: " + rkey);
            }
            return r;
        }
        public static bool TryParse(string rkey, out Tid result)
        {
            result = default;
            if (rkey.Length != 13) return false;
            var tsPart = rkey.Substring(0, rkey.Length - 2);
            var clockPart = rkey.Substring(rkey.Length - 2);


            var tsMicros = AtProtoS32.TryDecode(tsPart.Substring(0, tsPart.Length));
            if (tsMicros == -1) return false;
            if (tsMicros > MAX_SAFE_INTEGER) return false;
            var clockId = AtProtoS32.TryDecode(clockPart);
            if (clockId == -1) return false;

            var z = Tid.FromMicroseconds(tsMicros, (uint)clockId);
            var roundtripped = z.ToString();
            if (roundtripped != rkey) return false;
            result = z;

            return true;
        }
        const long MAX_SAFE_INTEGER = 9007199254740991;
        public long Timestamp => this.TidValue >> 10;
        public int ClockId => (int)(this.TidValue & 0b_11111_11111);

        public DateTime Date => DateTime.UnixEpoch.AddMicroseconds(Timestamp);

        public static Tid FromDateTime(DateTime d, uint clockId = 0) => FromMicroseconds(checked((d - DateTime.UnixEpoch).Ticks / TimeSpan.TicksPerMicrosecond), clockId);
        public static Tid FromMicroseconds(long microseconds, uint clockId) => new Tid((microseconds << 10) | clockId);
        public override string? ToString()
        {
            if (this == MaxValue) return "(max value)";
            if (this == default) return null;
            //return Date.ToString("yyyy-MM-dd HH:mm:ss.fff");

            return AtProtoS32.Encode(Timestamp) + AtProtoS32.Encode(ClockId).PadLeft(2, '2');
        }

        public readonly static Tid MaxValue = new Tid(long.MaxValue);
        public int CompareTo(Tid other)
        {
            return this.TidValue.CompareTo(other.TidValue);
        }
        public string RkeyString => ToString()!;

        public Tid GetNext() => new Tid(TidValue + 1);
    }
}

