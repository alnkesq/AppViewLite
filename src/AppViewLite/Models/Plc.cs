using System;

namespace AppViewLite.Models
{
    public record struct Plc(int PlcValue) : IComparable<Plc>
    {
        public int CompareTo(Plc other)
        {
            return PlcValue.CompareTo(other.PlcValue);
        }
    }
}

