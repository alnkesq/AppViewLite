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

    public class ProjectionComparer<T, TKey> : IComparer<T>
    {
        private readonly Func<T, TKey> _keySelector;
        private readonly IComparer<TKey> _comparer;

        public ProjectionComparer(Func<T, TKey> keySelector, IComparer<TKey>? comparer = null)
        {
            _keySelector = keySelector ?? throw new ArgumentNullException(nameof(keySelector));
            _comparer = comparer ?? Comparer<TKey>.Default;
        }

        public int Compare(T? x, T? y)
        {
            return _comparer.Compare(_keySelector(x!), _keySelector(y!));
        }
    }

    public class ReverseProjectionComparer<T, TKey> : IComparer<T>
    {
        private readonly Func<T, TKey> _keySelector;
        private readonly IComparer<TKey> _comparer;

        public ReverseProjectionComparer(Func<T, TKey> keySelector, IComparer<TKey>? comparer = null)
        {
            _keySelector = keySelector ?? throw new ArgumentNullException(nameof(keySelector));
            _comparer = comparer ?? Comparer<TKey>.Default;
        }

        public int Compare(T? x, T? y)
        {
            return _comparer.Compare(_keySelector(y!), _keySelector(x!));
        }
    }
}

