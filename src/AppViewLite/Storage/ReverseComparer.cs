using System.Collections.Generic;

namespace AppViewLite.Storage
{

    public class ReverseComparer<T> : IComparer<T>
    {
        public readonly static ReverseComparer<T> Instance = new();
        public int Compare(T? x, T? y)
        {
            return Comparer<T>.Default.Compare(y, x);
        }
    }
}

