using AppViewLite.Numerics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace AppViewLite
{
    internal static class StringUtils
    {
        [SkipLocalsInit]
        public static DuckDbUuid HashUnicodeToUuid(ReadOnlySpan<char> b)
        {
            return HashToUuid(MemoryMarshal.AsBytes(b));
        }


        [SkipLocalsInit]
        public static DuckDbUuid HashToUuid(ReadOnlySpan<byte> b)
        {
            Span<byte> buffer = stackalloc byte[32];
            System.Security.Cryptography.SHA256.HashData(b, buffer);
            return new DuckDbUuid(buffer.Slice(0, 16));
        }
        public static IEnumerable<string> GetAllWords(string? text)
        {
            if (string.IsNullOrEmpty(text)) return [];

            text = RemoveDiacritics(text);

            return Regex.Matches(text.ToLowerInvariant(), @"\w+").Select(x => x.Value);
        }

        public static string[] GetDistinctWords(string? text)
        {
            return GetAllWords(text).Distinct().ToArray();
        }


        public static List<string[]> GetExactPhrases(string? text)
        {
            if (string.IsNullOrEmpty(text)) return [];
            var result = new List<string[]>();
            foreach (Match phrase in Regex.Matches(text, @""".*?"""))
            {
                var words = GetAllWords(phrase.Value).ToArray();
                if (words.Length != 0)
                    result.Add(words);
            }
            return result;
        }


        private static string RemoveDiacritics(string text)
        {
            var sb = new StringBuilder();
            var hasDiacritics = false;
            foreach (var ch in text.Normalize(NormalizationForm.FormD))
            {
                if (char.GetUnicodeCategory(ch) != System.Globalization.UnicodeCategory.NonSpacingMark)
                    sb.Append(ch);
                else
                    hasDiacritics = true;
            }
            if (!hasDiacritics) return text;
            return sb.ToString();
        }
    }
}


