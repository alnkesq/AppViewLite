using AppViewLite.Models;
using DuckDbSharp.Types;
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

        public static (string[] DistinctWords, string? LastWordPrefix) GetDistinctWordsAndLastPrefix(string? text, bool allowLastWordPrefix = true)
        {
            if (!allowLastWordPrefix || string.IsNullOrEmpty(text) || !char.IsLetterOrDigit(text![^1])) return (GetDistinctWords(text), null);
            var allWords = GetAllWords(text).ToArray();
            return (allWords.Take(allWords.Length - 1).Distinct().ToArray(), allWords[^1]);
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

        public static FacetData[]? GuessFacets(string? text)
        {
            if (string.IsNullOrEmpty(text)) return null;
            var facets = new List<FacetData>();
            void AddFacetIfNoOverlap(FacetData f)
            {
                if (facets.All(x => x.IsDisjoint(f)))
                    facets.Add(f);
            }
            foreach (Match m in Regex.Matches(text, @"@[\w\.\-]+@[\w\.\-]+")) // @example@mastodon.social
            {
                if (m.Index != 0 && !char.IsWhiteSpace(text[m.Index - 1])) continue;
                var v = m.Value.Substring(1).Split('@');
                AddFacetIfNoOverlap(CreateFacetFromUnicodeIndexes(text, m.Index, m.Length, "https://" + v[1] + "/@" + v[0]));
            }
            foreach (Match m in Regex.Matches(text, @"[\w\.\-]+@[\w\.\-]+")) // example@gmail.com
            {
                if (m.Index != 0 && !char.IsWhiteSpace(text[m.Index - 1])) continue;
                AddFacetIfNoOverlap(CreateFacetFromUnicodeIndexes(text, m.Index, m.Length, "mailto:" + m.Value));
            }

            foreach (Match m in Regex.Matches(text, @"@[\w\.\-]+")) // @example.bsky.social
            {
                if (m.Index != 0 && !char.IsWhiteSpace(text[m.Index - 1])) continue;
                AddFacetIfNoOverlap(CreateFacetFromUnicodeIndexes(text, m.Index, m.Length, null, m.Value.Substring(1)));
            }

            foreach (Match m in Regex.Matches(text, @"\bhttps?\:\/\/[\w\-]+(?:\.[\w\-]+)+(?:/[^\s]*)?")) // http://example.com or // http://example.com/path
            {
                if (m.Index != 0 && !char.IsWhiteSpace(text[m.Index - 1])) continue;
                var link = m.Value;
                AddFacetIfNoOverlap(CreateFacetFromUnicodeIndexes(text, m.Index, m.Length, link));
            }
            foreach (Match m in Regex.Matches(text, @"\b[\w\-]+(?:\.[\w\-]+)+(?:/[^\s]*)?")) // example.com or // example.com/path
            {
                if (m.Index != 0 && (!char.IsWhiteSpace(text[m.Index - 1]) || text[m.Index - 1] == '/')) continue;
                var link = m.Value;
                AddFacetIfNoOverlap(CreateFacetFromUnicodeIndexes(text, m.Index, m.Length, "https://" + link));
            }
            foreach (var item in GuessHashtagFacets(text))
            {
                AddFacetIfNoOverlap(item);
            }
            return facets.OrderBy(x => x.Start).ToArray();

        }


        public static List<FacetData> GuessHashtagFacets(string text)
        {
            var hashtags = new List<FacetData>();
            foreach (Match m in Regex.Matches(text, @"#\w+"))
            {
                if (m.Index != 0 && !char.IsWhiteSpace(text[m.Index - 1])) continue;
                var value = m.ValueSpan.Slice(1);
                if (value.ContainsAnyExceptInRange('0', '9')) continue; // e.g. #1
                hashtags.Add(CreateFacetFromUnicodeIndexes(text, m.Index, m.Length, "/search?q=" + Uri.EscapeDataString(m.Value), null));
            }
            return hashtags;
        }
        private static FacetData CreateFacetFromUnicodeIndexes(string originalString, int index, int length, string? link, string? did = null)
        {
            var startUtf8 = System.Text.Encoding.UTF8.GetByteCount(originalString.AsSpan(0, index));
            var lengthUtf8 = System.Text.Encoding.UTF8.GetByteCount(originalString.AsSpan(index, length));
            return new FacetData { Start = startUtf8, Length = lengthUtf8, Link = link, Did = did };
        }

        public static string NormalizeHandle(string handle)
        {
            return handle.ToLowerInvariant();
        }
    }
}


