using System;

namespace AppViewLite.Models
{
    public record struct Plc(int PlcValue) : IComparable<Plc>
    {
        public int CompareTo(Plc other)
        {
            return PlcValue.CompareTo(other.PlcValue);
        }

        public Plc GetNext() => new Plc(PlcValue + 1);

        public readonly static Plc MaxValue = new(int.MaxValue);

        public override string ToString()
        {
            return $"Plc({PlcValue})";
        }
    }
}

