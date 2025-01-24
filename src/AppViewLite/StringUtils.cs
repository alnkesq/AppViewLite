using AppViewLite.Numerics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.RegularExpressions;

namespace AppViewLite
{
    public static class StringUtils
    {
        public static string AndJoin(string[] items)
        {
            if (items.Length == 0) return string.Empty;
            if (items.Length == 1) return items[0];

            var sb = new StringBuilder();
            for (int i = 0; i < items.Length; i++)
            {
                if (i != 0)
                {
                    if (i == items.Length - 1) sb.Append(", and ");
                    else sb.Append(", ");
                }
                sb.Append(items[i]);
            }
            return sb.ToString();
        }

        public static string FormatEngagementCount(long value)
        {
            if (value < 1_000)
            {
                // 1..999
                return value.ToString();
            }
            else if (value < 1_000_000)
            {
                // 1.0K..9.9K
                // 19K..999K
                return FormatTwoSignificantDigits(value / 1_000.0) + "K";
            }
            else
            {
                // 1.0M..9.9M
                // 10M..1234567M
                return FormatTwoSignificantDigits(value / 1_000_000.0) + "M";
            }

            
        }

        private static string FormatTwoSignificantDigits(double displayValue)
        {
            var r = (Math.Floor(displayValue * 10) / 10).ToString("0.0");
            if(r.Length > 3)
                r = Math.Floor(displayValue).ToString("0");
            return r;
        }

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

        internal static void ParseQueryModifiers(ref string? query, Func<string, string, bool> onModifier)
        {
            if (query == null) return;
            while (true)
            {
                var q = query;
                var matches = Regex.Matches(q, @"\b[a-zA-Z_]+:");

                var found = false;
                foreach (Match match in matches.Where(x =>
                {
                    var r = x.Index;
                    var quotesBefore = q.AsSpan(0, r).Count('"');
                    return quotesBefore % 2 == 0;
                }))
                {

                    if (match == null) break;

                    var modifierAndRest = q.AsSpan(match.Index);
                    var space = modifierAndRest.IndexOf(' ');
                    if (space == -1) space = modifierAndRest.Length;

                    var modifierName = match.ValueSpan[..^1];
                    var modifierValue = modifierAndRest.Slice(0, space).Slice(match.Length);
                    if (onModifier(modifierName.ToString(), modifierValue.ToString()))
                    {
                        var rest = modifierAndRest.Slice(space);
                        if (!rest.IsEmpty) rest = rest.Slice(1);

                        query = string.Concat(query.AsSpan(0, match.Index), rest);
                        found = true;
                        break;
                    }

                }

                if (!found) break;

            }
        }
    }
}


