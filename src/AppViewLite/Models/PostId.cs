using AppViewLite.Numerics;
using System;
using System.Runtime.InteropServices;

namespace AppViewLite.Models
{
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public record struct PostId(Plc Author, Tid PostRKey) : IComparable<PostId>
    {
        public int CompareTo(PostId other)
        {
            var z = Author.CompareTo(other.Author);
            if (z != 0) return z;
            return PostRKey.CompareTo(other.PostRKey);
        }
        public string Serialize() => ((PostIdTimeFirst)this).Serialize();
        public static PostId Deserialize(string s) => PostIdTimeFirst.Deserialize(s);
    }
}

