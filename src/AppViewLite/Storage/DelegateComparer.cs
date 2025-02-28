using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Storage
{
    public class DelegateComparer<T> : IComparer<T>, IEqualityComparer<T>
    {
        private readonly Func<T?, T?, int> compare;
        public DelegateComparer(Func<T?, T?, int> compare)
        {
            this.compare = compare;
        }

        public int Compare(T? x, T? y)
        {
            return compare(x, y);
        }

        public bool Equals(T? x, T? y)
        {
            return compare(x, y) == 0;
        }

        public int GetHashCode([DisallowNull] T obj)
        {
            throw new NotSupportedException();
        }
    }
    public class DelegateEqualityComparer<T> : IEqualityComparer<T>
    {
        private readonly Func<T?, T?, bool> equals;
        private readonly Func<T, int>? hash;
        public DelegateEqualityComparer(Func<T?, T?, bool> equals, Func<T, int>? hash = null)
        {
            this.equals = equals;
            this.hash = hash;
        }
        public bool Equals(T? x, T? y)
        {
            return equals(x, y);
        }

        public int GetHashCode([DisallowNull] T obj)
        {
            return hash!(obj);
        }
    }
}

