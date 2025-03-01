using AppViewLite.Numerics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Models
{
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public record struct BookmarkPostFirst(PostIdTimeFirst PostId, Tid BookmarkRKey) : IComparable<BookmarkPostFirst>
    {
        public int CompareTo(BookmarkPostFirst other)
        {
            var cmp = this.PostId.CompareTo(other.PostId);
            if (cmp != 0) return cmp;
            return this.BookmarkRKey.CompareTo(other.BookmarkRKey);
        }
    }

    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public record struct BookmarkDateFirst(Tid BookmarkRKey, PostIdTimeFirst PostId) : IComparable<BookmarkDateFirst>
    {
        public int CompareTo(BookmarkDateFirst other)
        {
            var cmp = this.BookmarkRKey.CompareTo(other.BookmarkRKey);
            if (cmp != 0) return cmp;
            return this.PostId.CompareTo(other.PostId);
        }
    }
}

