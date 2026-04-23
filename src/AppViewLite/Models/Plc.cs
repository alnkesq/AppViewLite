using System;

namespace AppViewLite.Models
{
    public record struct Plc(int PlcValue) : IComparable<Plc>
    {
        public readonly int CompareTo(Plc other)
        {
            return PlcValue.CompareTo(other.PlcValue);
        }

        public readonly Plc GetNext() => new Plc(PlcValue + 1);

        public readonly static Plc MaxValue = new(int.MaxValue);

        public override readonly string ToString()
        {
            return $"Plc({PlcValue})";
        }
    }
}

