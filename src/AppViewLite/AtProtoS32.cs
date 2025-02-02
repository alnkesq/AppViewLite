using System;
using System.Text;

namespace AppViewLite
{
    public static class AtProtoS32
    {
        private const string S32_CHAR = "234567abcdefghijklmnopqrstuvwxyz";

        public static string Encode(long i)
        {

            var sb = new StringBuilder();
            do
            {
                sb.Append(S32_CHAR[(int)(i % 32)]);
                i /= 32;
            } while (i > 0);

            for (int j = 0; j < sb.Length / 2; j++)
            {
                var temp = sb[j];
                sb[j] = sb[sb.Length - 1 - j];
                sb[sb.Length - 1 - j] = temp;
            }
            return sb.ToString();
        }
        public static string EncodePadded(UInt128 i)
        {
            return Encode(i).PadLeft(24, 'a');
        }

        public static string Encode(UInt128 i)
        {

            var sb = new StringBuilder();
            do
            {
                sb.Append(S32_CHAR[(int)(i % 32)]);
                i /= 32;
            } while (i > 0);

            for (int j = 0; j < sb.Length / 2; j++)
            {
                var temp = sb[j];
                sb[j] = sb[sb.Length - 1 - j];
                sb[sb.Length - 1 - j] = temp;
            }
            return sb.ToString();
        }

        public static long TryDecode(string s)
        {
            long i = 0;
            foreach (var c in s)
            {
                i *= 32;
                var h = S32_CHAR.IndexOf(c);
                if (h == -1) return -1;
                i += h;
            }
            return i;
        }
        public static UInt128? TryDecode128(string s)
        {
            UInt128 i = 0;
            foreach (var c in s)
            {
                i *= 32;
                var h = S32_CHAR.IndexOf(c);
                if (h == -1) return null;
                i += (uint)h;
            }
            return i;
        }
    }
}

