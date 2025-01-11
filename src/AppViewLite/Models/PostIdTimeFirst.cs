using AppViewLite.Numerics;
using System;
using System.Runtime.InteropServices;

namespace AppViewLite.Models
{
    [StructLayout(LayoutKind.Sequential, Pack = 4)]
    public record struct PostIdTimeFirst(Tid PostRKey, Plc Author) : IComparable<PostIdTimeFirst>
    {
        public int CompareTo(PostIdTimeFirst other)
        {
            var z = PostRKey.CompareTo(other.PostRKey);
            if (z != 0) return z;
            return Author.CompareTo(other.Author);
        }
        public static implicit operator PostId(PostIdTimeFirst a) => new(a.Author, a.PostRKey);
        public static implicit operator PostIdTimeFirst(PostId a) => new(a.PostRKey, a.Author);

        public string Serialize() => PostRKey.TidValue + "_" + Author.PlcValue;
        public static PostIdTimeFirst Deserialize(string s)
        {
            var parts = s.Split('_');
            return new PostIdTimeFirst(new Tid(long.Parse(parts[0])), new Plc(int.Parse(parts[1])));
        }
    }
}

