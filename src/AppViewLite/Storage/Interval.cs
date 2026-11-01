using System;
using System.Diagnostics;
using System.Numerics;

namespace AppViewLite
{
    [DebuggerDisplay($"{{{nameof(GetDebuggerDisplay)}(),nq}}")]
    public readonly struct Interval<T> : IEquatable<Interval<T>> where T : IComparisonOperators<T, T, bool>
    {
        public readonly T Min;
        public readonly T Max;
        public readonly bool InclusiveMin;
        public readonly bool InclusiveMax;

        public Interval(T min, T max, bool inclusiveMin = true, bool inclusiveMax = true)
        {
            if (min > max) throw new ArgumentException();
            if (min == max)
            {
                if (inclusiveMin || inclusiveMax)
                    inclusiveMin = inclusiveMax = true;
            }
            Min = min;
            Max = max;
            InclusiveMin = inclusiveMin;
            InclusiveMax = inclusiveMax;
        }

        public override string ToString()
        {
            return (InclusiveMin ? "[" : "(") + Min + ", " + Max + (InclusiveMax ? "]" : ")");
        }

        public bool Contains(T point)
        {
            if (point < Min) return false;
            if (point > Max) return false;

            if (point == Min && !InclusiveMin) return false;
            if (point == Max && !InclusiveMax) return false;
            return true;
        }

        public readonly bool OverlapsWith(Interval<T> other)
        {
            return !IsDisjointFrom(other);
        }

        public readonly bool IsDisjointFrom(Interval<T> other)
        {
            return this.IsBeforeAndNonOverlapping(other) || other.IsBeforeAndNonOverlapping(this);
        }

        public readonly bool IsBeforeAndNonOverlapping(Interval<T> after)
        {
            var endOfBefore = this.Max;
            var startOfAfter = after.Min;

            if (endOfBefore < startOfAfter) return true;
            if (endOfBefore > startOfAfter) return false;


            var endOfBeforeIsInclusive = this.InclusiveMax;
            var startOfAfterIsInclusive = after.InclusiveMin;
            var overlaps = endOfBeforeIsInclusive && startOfAfterIsInclusive;
            return !overlaps;

        }


        public readonly Interval<T> GetMergedWith(Interval<T> other)
        {
            if (TryGetMergedWith(other, out var result)) return result;
            throw new ArgumentException($"The intervals {this} and {other} don't overlap nor are they adjacent.");
        }

        public readonly bool TryGetMergedWith(Interval<T> other, out Interval<T> result)
        {
            if (!OverlapsWith(other))
            {
                if (this.Max == other.Min)
                {
                    // [this)[other]
                    result = new Interval<T>(this.Min, other.Max, this.InclusiveMin, other.InclusiveMax);
                    return true;
                }
                if (this.Min == other.Max)
                {
                    // [other)[this]
                    result = new Interval<T>(other.Min, this.Max, other.InclusiveMin, this.InclusiveMax);
                    return true;
                }
                result = default;
                return false;
            }

            T resultMin;
            bool resultMinInclusive;
            T resultMax;
            bool resultMaxInclusive;

            if (this.Min == other.Min)
            {
                resultMin = this.Min;
                resultMinInclusive = this.InclusiveMin || other.InclusiveMin;
            }
            else if (this.Min < other.Min)
            {
                resultMin = this.Min;
                resultMinInclusive = this.InclusiveMin;
            }
            else
            {
                resultMin = other.Min;
                resultMinInclusive = other.InclusiveMin;
            }


            if (this.Max == other.Max)
            {
                resultMax = this.Max;
                resultMaxInclusive = this.InclusiveMax || other.InclusiveMax;
            }
            else if (this.Max > other.Max)
            {
                resultMax = this.Max;
                resultMaxInclusive = this.InclusiveMax;
            }
            else
            {
                resultMax = other.Max;
                resultMaxInclusive = other.InclusiveMax;
            }


            result = new Interval<T>(resultMin, resultMax, resultMinInclusive, resultMaxInclusive);
            return true;
        }

        public override bool Equals(object? obj)
        {
            return obj is Interval<T> interval && Equals(interval);
        }

        public bool Equals(Interval<T> other)
        {
            return this.Min == other.Min &&
                   this.Max == other.Max &&
                   InclusiveMin == other.InclusiveMin &&
                   InclusiveMax == other.InclusiveMax;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Min, Max, InclusiveMin, InclusiveMax);
        }
        public static bool operator ==(Interval<T> a, Interval<T> b) => a.Equals(b);
        public static bool operator !=(Interval<T> a, Interval<T> b) => !(a == b);

        private string GetDebuggerDisplay()
        {
            return ToString();
        }
    }
}

