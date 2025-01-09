using System.Text;

namespace AppViewLite
{
    internal static class AtProtoS32
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
    }
}

