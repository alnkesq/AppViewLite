using AngleSharp;
using AngleSharp.Dom;
using AngleSharp.Html.Dom;
using AngleSharp.Html.Parser;
using AppViewLite.Models;
using AppViewLite.Storage;
using DuckDbSharp.Types;
using System;
using System.Buffers.Text;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.RegularExpressions;
using System.Xml.Linq;

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
                    if (i == items.Length - 1) sb.Append(items.Length == 2 ? " and " : ", and ");
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

            var emojis = GetEmojis(text);
            text = RemoveDiacritics(text);

            var words = Regex.Matches(text.ToLowerInvariant(), @"\w+").Select(x => x.Value);

            if (emojis != null)
                words = words.Concat(emojis);

            return words;
        }

        private readonly static Rune ZWJ = new Rune(0x200D);
        private readonly static Rune VariationSelectorMin = new Rune(0xFE00);
        private readonly static Rune VariationSelectorMax = new Rune(0xFE0F);
        private readonly static Rune RegionalIndicatorA = new Rune(0x1F1E6);
        private readonly static Rune RegionalIndicatorZ = new Rune(0x1F1FF);
        private readonly static Rune TagMin = new Rune(0xE0000);
        private readonly static Rune TagMax = new Rune(0xE007F);

        private static IEnumerable<string>? GetEmojis(string text)
        {
            if (Ascii.IsValid(text)) return null;
            var result = new List<string>();

            var currentEmoji = new StringBuilder();

            Span<char> runeBuffer = stackalloc char[2];

            var previousEmojiContinues = false;

            var previousRuneIsRegionalIndicator = false;

            foreach (var rune in text.EnumerateRunes())
            {
                
                var isVariationSelector = rune >= VariationSelectorMin && rune <= VariationSelectorMax;
                if (isVariationSelector) continue; // color vs black and white emoji

                var isZwj = rune == ZWJ && currentEmoji.Length != 0;

                var isTag = rune >= TagMin && rune <= TagMax;
                var isRegionalIndicator = rune >= RegionalIndicatorA && rune <= RegionalIndicatorZ;

                if (previousRuneIsRegionalIndicator && isRegionalIndicator)
                {
                    previousEmojiContinues = true;
                    isRegionalIndicator = false; // so that we keep consecutive flags separated
                }

                var category = Rune.GetUnicodeCategory(rune);
                if (category is UnicodeCategory.ModifierSymbol || isTag || isZwj)
                {
                    previousEmojiContinues = true;
                }

                if (currentEmoji.Length != 0 && !previousEmojiContinues)
                {
                    result.Add(currentEmoji.ToString());
                    currentEmoji.Clear();
                }

                if (category is UnicodeCategory.OtherSymbol or UnicodeCategory.ModifierSymbol || isTag || isZwj)
                {
                    rune.EncodeToUtf16(runeBuffer);
                    currentEmoji.Append(runeBuffer.Slice(0, rune.Utf16SequenceLength));
                }

                previousEmojiContinues = isZwj;
                previousRuneIsRegionalIndicator = isRegionalIndicator;
            }
            if (currentEmoji.Length != 0)
                result.Add(currentEmoji.ToString());
            return result;
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

        public static FacetData[]? GuessFacets(string? text, bool includeImplicitFacets = true)
        {
            if (string.IsNullOrEmpty(text)) return null;
            var facets = new List<FacetData>();
            void AddFacetIfNoOverlap(FacetData? f)
            {
                if (f != null && facets.All(x => x.IsDisjoint(f)))
                    facets.Add(f);
            }
            foreach (Match m in Regex.Matches(text, @"@[\w\.\-]+@[\w\.\-]+\w+")) // @example@mastodon.social
            {
                if (m.Index != 0 && !char.IsWhiteSpace(text[m.Index - 1])) continue;
                var v = m.Value.Substring(1).Split('@');
                AddFacetIfNoOverlap(CreateFacetFromUnicodeIndexes(text, m.Index, m.Length, "https://" + v[1] + "/@" + v[0]));
            }
            foreach (Match m in Regex.Matches(text, @"[\w\.\-]+@[\w\.\-]+\w+")) // example@gmail.com
            {
                if (m.Index != 0 && !char.IsWhiteSpace(text[m.Index - 1])) continue;
                AddFacetIfNoOverlap(CreateFacetFromUnicodeIndexes(text, m.Index, m.Length, "mailto:" + m.Value));
            }

            foreach (Match m in Regex.Matches(text, @"@[\w\.\-]+\.\w+")) // @example.bsky.social
            {
                if (m.Index != 0 && !char.IsWhiteSpace(text[m.Index - 1])) continue;
                AddFacetIfNoOverlap(CreateFacetFromUnicodeIndexes(text, m.Index, m.Length, did: m.Value.Substring(1)));
            }

            foreach (Match m in Regex.Matches(text, @"\bhttps?\:\/\/[\w\-]+(?:\.[\w\-]+)+(?:/[^\s]*)?")) // http://example.com or // http://example.com/path
            {
                if (m.Index != 0 && !char.IsWhiteSpace(text[m.Index - 1])) continue;
                if (!IsValidUrl(m.Value)) continue;
                AddFacetIfNoOverlap(CreateFacetFromUnicodeIndexes(text, m.Index, m.Length, sameLinkAsText: true));
            }
            foreach (Match m in Regex.Matches(text, @"\b[\w\-]+(?:\.[\w\-]+)+(?:/[^\s]*)?")) // example.com or example.com/path
            {
                if (m.Index != 0 && (!char.IsWhiteSpace(text[m.Index - 1]) || text[m.Index - 1] == '/')) continue;
                var len = m.Length;
                var link = m.Value;
                if (link.EndsWith('.') || link.EndsWith(')'))
                {
                    len--;
                    link = link.Substring(0, len);
                }
                if (!link.Contains('/'))
                {
                    var dot = link.LastIndexOf('.');
                    var tld = link.Substring(dot + 1);
                    if (IsKnownFileTypeAsOpposedToTld(tld)) continue;
                }
                AddFacetIfNoOverlap(CreateFacetFromUnicodeIndexes(text, m.Index, len, "https://" + link));
            }
            if (includeImplicitFacets)
            {
                foreach (var item in GuessImplicitFacets(text))
                {
                    AddFacetIfNoOverlap(item);
                }
                
            }
            return facets.OrderBy(x => x.Start).ToArray();

        }

        private static bool IsValidUrl(string value)
        {
            if (!Uri.TryCreate(value, UriKind.Absolute, out var url)) return false;
            
            if (!string.IsNullOrEmpty(url.Host))
            {
                var tld = url.Host.Split('.')[^1];
                if (tld.Length < 2) return false;
                if (tld.AsSpan().ContainsAnyInRange('0', '9')) // raw IPs are ok, but numeric TLDs are not.
                { 
                    if(!IPAddress.TryParse(url.Host, out _))
                        return false;
                }
            }

            return true;
        }

        public static List<FacetData> GuessCustomEmojiFacets(string? text, Func<string, DuckDbUuid?> getEmojiHash, Func<string, Match, bool>? isValidCandidate = null)
        {
            if (string.IsNullOrEmpty(text) || !text.Contains(':')) return [];
            var result = new List<FacetData>();
            foreach (Match m in Regex.Matches(text, @":\w+:"))
            {
                if (isValidCandidate == null || isValidCandidate(text, m))
                {
                    var hash = getEmojiHash(m.Value.Substring(1, m.Value.Length - 2));
                    if (hash != null)
                        result.Add(CreateFacetFromUnicodeIndexes(text, m.Index, m.Length, customEmojiHash: hash)!);
                }
            }
            return result;
        }

        public static void GuessCustomEmojiFacetsNoAdjacent(string? text, ref FacetData[]? facets, CustomEmoji[]? customEmojis)
        {
            GuessCustomEmojiFacets(text, ref facets, customEmojis, IsValidNonAdjacentCustomEmoji);
        }
        public static void GuessCustomEmojiFacets(string? text, ref FacetData[]? facets, CustomEmoji[]? customEmojis, Func<string, Match, bool>? isValidCandidate = null)
        {
            if (text != null && customEmojis != null && customEmojis.Length != 0)
            {
                var emojiFacets = StringUtils.GuessCustomEmojiFacets(text, shortname =>
                {
                    return customEmojis.FirstOrDefault(x => x.ShortCode == shortname)?.Hash;
                }, isValidCandidate);
                if (emojiFacets.Count == 0) return;
                facets = (facets ?? []).Concat(emojiFacets).ToArray();
            }
        }


        private static bool IsValidNonAdjacentCustomEmoji(string text, Match m)
        {
            return
                IsValidAdjacentCharacterForCustomEmoji(text, m.Index - 1) &&
                IsValidAdjacentCharacterForCustomEmoji(text, m.Index + m.Length);
        }

        private static bool IsValidAdjacentCharacterForCustomEmoji(string text, int index)
        {
            if (index == -1 || index == text.Length) return true;
            var ch = text[index];
            if (char.IsLetterOrDigit(ch)) return false;
            if (ch == ':') return false;
            return true;
        }

        public static bool CouldContainImplicitFacets(ReadOnlySpan<char> text)
        {
            return text.ContainsAny('#', '>');
        }
        public static bool CouldContainImplicitFacets(ReadOnlySpan<byte> text)
        {
            return text.ContainsAny((byte)'#', (byte)'>');
        }

        public static List<FacetData> GuessImplicitFacets(string? text)
        {
            if (!CouldContainImplicitFacets(text)) return [];
            var implicitFacets = new List<FacetData>();
            foreach (Match m in Regex.Matches(text!, @"#\w+"))
            {
                if (IsValidHashtagFacet(text!, m)) 
                    implicitFacets.Add(CreateFacetFromUnicodeIndexes(text!, m.Index, m.Length, "/search?q=" + Uri.EscapeDataString(m.Value), null, verifyLink: false)!);
            }

            foreach (Match m in Regex.Matches(text!, @"^>.*$", RegexOptions.Multiline))
            {
                implicitFacets.Add(CreateFacetFromUnicodeIndexes(text!, m.Index, m.Length, quote: true)!);
            }

            
            return implicitFacets;
        }

        public static string[] GuessHashtags(string? text)
        {
            if (string.IsNullOrEmpty(text) || !text.Contains('#')) return [];
            return Regex.Matches(text, @"#\w+").Where(m => IsValidHashtagFacet(text, m)).Select(x => x.Value).ToArray();

        }

        private static bool IsValidHashtagFacet(string fullText, Match m)
        {
            if (m.Index != 0 && !char.IsWhiteSpace(fullText[m.Index - 1])) return false;
            var value = m.ValueSpan.Slice(1);
            if (!value.ContainsAnyExceptInRange('0', '9')) return false; // e.g. #1
            return true;
        }

        private static FacetData? CreateFacetFromUnicodeIndexes(string originalString, int index, int length, string? link = null, string? did = null, DuckDbUuid? customEmojiHash = null, bool? sameLinkAsText = null, bool verifyLink = true, bool quote = false)
        {
            var startUtf8 = System.Text.Encoding.UTF8.GetByteCount(originalString.AsSpan(0, index));
            var lengthUtf8 = System.Text.Encoding.UTF8.GetByteCount(originalString.AsSpan(index, length));
            if (verifyLink && link != null)
            {
                if (!IsValidUrl(link))
                    return null;
            }
            return new FacetData { Start = startUtf8, Length = lengthUtf8, Link = link, Did = did, CustomEmojiHash = customEmojiHash, SameLinkAsText = sameLinkAsText, Quote = quote };
        }

        public static string NormalizeHandle(string handle)
        {
            if (handle.StartsWith("did:", StringComparison.Ordinal))
                return handle;
            return handle.ToLowerInvariant();
        }

        public static (string? Text, FacetData[]? Facets) HtmlToFacets(INode dom, Func<IElement, FacetData?> getFacet)
        {
            return HtmlToFacets([dom], getFacet);
        }
        public static (string? Text, FacetData[]? Facets) HtmlToFacets(INode[] dom, Func<IElement, FacetData?> getFacet)
        {
            var sb = new StringBuilder();
            var utf8length = 0;
            var facets = new List<FacetData>();
            void AppendText(string? text)
            {
                if (string.IsNullOrEmpty(text)) return;
                sb.Append(text);
                utf8length += Encoding.UTF8.GetByteCount(text);
            }
            void AppendChar(char ch)
            {
                sb.Append(ch);
                utf8length += new Rune(ch).Utf8SequenceLength;
            }
            void AppendRune(Rune rune)
            {
                Span<char> buffer = stackalloc char[2];
                var len = rune.EncodeToUtf16(buffer);
                sb.Append(buffer.Slice(0, len));
                utf8length += rune.Utf8SequenceLength;
            }
            void AppendNewLineIfNecessary()
            {
                if (sb.Length != 0 && sb[sb.Length - 1] != '\n')
                    AppendChar('\n');
            }


            void Recurse(INode node, bool pre)
            {
                if (node.NodeType == NodeType.Text)
                {
                    var text = node.TextContent;
                    if (pre)
                    {
                        AppendText(text);
                    }
                    else
                    {

                        for (int i = 0; i < text.Length; i++)
                        {
                            var ch = text[i];
                            if (char.IsWhiteSpace(ch))
                            {
                                if (sb.Length != 0 && !char.IsWhiteSpace(sb[sb.Length - 1]))
                                {
                                    AppendChar(' ');
                                }
                            }
                            else if (char.IsHighSurrogate(ch))
                            {
                                if (i != text.Length - 1)
                                    AppendRune(new Rune(ch, text[++i]));
                            }
                            else
                            {
                                AppendChar(ch);
                            }

                        }
                    }
                }
                else if (node.NodeType == NodeType.Element)
                {
                    var element = (IElement)node;
                    var tagName = element.TagName;
                    var isBlockElement = tagName is "P" or "DIV" or "BLOCKQUOTE" or "LI";
                    if (isBlockElement) AppendNewLineIfNecessary();

                    if (tagName == "LI")
                        AppendText("• ");

                    if (tagName is "PRE" or "CODE")
                        pre = true;

                    var startIndex = utf8length;
                    foreach (var child in node.ChildNodes)
                    {
                        Recurse(child, pre);
                    }

                    var facet = getFacet(element);
                    if (facet != null)
                    {
                        facet.Start = startIndex;
                        facet.Length = utf8length - startIndex;
                        facets.Add(facet);
                    }
                    
                    

                    if (isBlockElement) AppendNewLineIfNecessary();
                    if (tagName is "BR") AppendText("\n");

                }
            }

            foreach (var child in dom)
            {
                Recurse(child, pre: false);
            }


            if (facets.Count == 0) facets = null;

            while (sb.Length != 0 && sb[sb.Length - 1] == '\n')
            {
                sb.Length--;
            }

            return (sb.Length != 0 ? sb.ToString() : null, facets?.ToArray());
        }

        public static (string? Text, FacetData[]? Facets) ParseHtmlToText(string? html, out IHtmlElement? body, Func<IElement, FacetData?> getFacet)
        {
            if (string.IsNullOrWhiteSpace(html))
            {
                body = null;
                return default;
            }
            var doc = ParseHtml(html);
            body = doc.Body!;
            foreach (var emoji in body.QuerySelectorAll(".wp-smiley").ToArray())
            {
                var text = emoji.GetAttribute("alt");
                emoji.ReplaceWith(doc.CreateTextNode(text ?? string.Empty));
            }
            return HtmlToFacets(body, getFacet);
        }
        public static IHtmlDocument ParseHtml(string? html)
        {
            var parser = new HtmlParser();
            return parser.ParseDocument(html ?? string.Empty);
        }

        internal static IEnumerable<string> ReadTextFile(string? path)
        {
            if (path == null) yield break;
            foreach (var line_ in System.IO.File.ReadLines(path))
            {
                var line = line_;
                var hash = line.IndexOf('#');
                if (hash != -1)
                    line = line.Substring(0, hash);
                line = line.Trim();
                if (!string.IsNullOrEmpty(line))
                    yield return line;

            }
        }

        public static Uri? TryParseUri(string? uri) => Uri.TryCreate(uri, UriKind.Absolute, out var url) ? url : null;

        public static string? GetFileName(this Uri url)
        {
            var path = Path.GetFileName(url.AbsolutePath);
            if (string.IsNullOrEmpty(path))
                return null;
            return path;
        }

        public static string GetDisplayHost(Uri url) => GetDisplayHost(url.Host);
        public static string GetDisplayHost(string host)
        {
            return TrimWww(host);
        }

        public static string GetDisplayUrl(Uri url)
        {

            var authority = StringUtils.TrimWww(url.Authority);

            if (url.PathAndQuery == "/") return authority;

            var pathAndQuery = url.ToString(); // minimal escaping
            var slash = pathAndQuery.IndexOf('/', 8);
            pathAndQuery = pathAndQuery.Substring(slash);

            if (pathAndQuery.Length >= 25)
                pathAndQuery = string.Concat(pathAndQuery.AsSpan(0, 20), "…");
            return authority + pathAndQuery;

        }


        public static string ToFullHumanDate(DateTime date)
        {
            return date.ToString("MMM d, yyyy HH:mm");
        }
        public static string ToHumanDate(DateTime date, bool showAgo = false)
        {
            var agoSuffix = showAgo ? " ago" : null;
            var ago = DateTime.UtcNow - date;
            if (ago.TotalHours < 1) return (int)Math.Max(ago.TotalMinutes, 1) + "m" + agoSuffix;
            if (ago.TotalDays < 1) return (int)ago.TotalHours + "h" + agoSuffix;
            if (date.Year == DateTime.UtcNow.Year) return date.ToString("MMM d");
            return date.ToString("MMM d, yyyy");
        }

        public static string ToHumanBytes(long bytes)
        {
            return CombinedPersistentMultiDictionary.ToHumanBytes(bytes);
        }

        public static string[] GetSegments(this Uri url) => url.AbsolutePath.Split('/', StringSplitOptions.RemoveEmptyEntries);

        public static bool HasHostSuffix(this Uri url, string suffix) => url.Host == suffix || url.Host.EndsWith("." + suffix, StringComparison.Ordinal);

        internal static FacetData? DefaultElementToFacet(IElement element, Uri? baseUrl, bool includeInlineImages = false)
        {
            if (element.TagName == "A")
            {
                var link = element.GetAttribute("href");
                if (!string.IsNullOrEmpty(link) && link != "#")
                {
                    Uri? url;
                    if (baseUrl != null ? Uri.TryCreate(link, UriKind.Absolute, out url) : Uri.TryCreate(baseUrl, link, out url))
                    {
                        link = url.ToString();
                        FacetData facet;
                        if (link == element.Text())
                            facet = new FacetData { SameLinkAsText = true };
                        else
                            facet = new FacetData { Link = link };
                        if (includeInlineImages)
                        {
                            var img = element.Children.FirstOrDefault(x => x.TagName == "IMG");
                            if (img != null)
                            {
                                var inner = DefaultElementToFacet(img, baseUrl, true);
                                if (inner != null)
                                {
                                    facet.InlineImageAlt = inner.InlineImageAlt;
                                    facet.InlineImageUrl = inner.InlineImageUrl;
                                }
                            }
                        }
                        return facet;
                    }
                }

            }

            if (includeInlineImages)
            {
                if (element.TagName == "IMG")
                {
                    var src = element.GetAttribute("src");
                    Uri? url;
                    if (baseUrl != null ? Uri.TryCreate(src, UriKind.Absolute, out url) : Uri.TryCreate(baseUrl, src, out url))
                    {
                        return new FacetData { InlineImageUrl = src };
                    }
                }
            }

            if (element.TagName == "DEL")
            {
                return new FacetData { Del = true };
            }

            if (element.TagName is "B" or "I")
            {
                return new FacetData { Bold = true };
            }
            return null;
        }

        internal static IHtmlElement AsWrappedTextNode(string text)
        {
            var document = ParseHtml(null);
            document.Body!.AppendChild(document.CreateTextNode(text));
            return document.Body;


        }

        public static bool IsKnownFileTypeAsOpposedToTld(ReadOnlySpan<char> ext)
        {
            return KnownFileExtensions.Contains(ext);
        }

        public static string GetDomainTrimWww(this Uri url)
        {
            return TrimWww(url.Host);
        }

        public static string TrimWww(string host)
        {
            if (host.StartsWith("www.", StringComparison.Ordinal))
                return host.Substring(4);
            return host;
        }

        internal static Uri? GetSrcSetLargestImageUrl(IElement img, Uri pageUrl)
        {
            var srcset = img.GetAttribute("srcset");
            if (srcset != null)
            {
                var sizes = srcset.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
                return sizes.Select(x =>
                {
                    var parts = x.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
                    if (parts.Length < 2) return default;

                    return (Url: Uri.TryCreate(pageUrl, parts[0], out var url) ? url : null, Size: int.Parse(Regex.Replace(parts[1], @"\D", string.Empty)));
                }).Where(x => x.Url != null).OrderByDescending(x => x.Size).FirstOrDefault().Url;
            }

            return Uri.TryCreate(pageUrl, img.GetAttribute("src"), out var url) ? url : null;
        }

        public static T? DeserializeFromString<T>(string? continuation) where T : unmanaged
        {
            if (string.IsNullOrEmpty(continuation)) return null;
            var bytes = Base64Url.DecodeFromChars(continuation);
            if (bytes.Length != Unsafe.SizeOf<T>())
                throw new ArgumentException();
            return MemoryMarshal.Cast<byte, T>(bytes)[0];   
        }

        public static string SerializeToString<T>(T value) where T : unmanaged
        {
            var bytes = MemoryMarshal.AsBytes<T>(new ReadOnlySpan<T>(in value));
            return Base64Url.EncodeToString(bytes);
        }

        public static object ParseRecord(Type type, string str)
        {
            var consumed = 0;
            var parts = str.Split(',', StringSplitOptions.TrimEntries);


            object ParseRecordCore(Type type)
            {
                if(consumed == parts.Length)
                    throw new ArgumentException($"Too few key components in the string '{str}' for type {type}.");
                if (type == typeof(DateTime))
                {
                    return DateTime.Parse(parts[consumed++], CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal);
                }
                var parseMethod = type.GetMethod("Parse", System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static, [typeof(string)]);
                if (parseMethod != null)
                {
                    var r = parseMethod.Invoke(null, [(object?)parts[consumed++]])!;

                    return r;
                }
                var ctor = type.GetConstructors().Single(x => x.GetParameters().Length != 0);
                var ctorParams = ctor.GetParameters();
                var ctorArgs = new object[ctorParams.Length];
                for (int i = 0; i < ctorArgs.Length; i++)
                {
                    ctorArgs[i] = ParseRecordCore(ctorParams[i].ParameterType);
                }
                return ctor.Invoke(ctorArgs);
            }
            var result = ParseRecordCore(type);
            if (consumed != parts.Length)
                throw new ArgumentException($"Too many key components in the string '{str}' for type {type}.");
            return result;
        }

        public static string FormatPercent(long done, long total)
        {
            if (total == 0) return "0%";
            if (done == total) return "100%";
            
            return (Math.Min(100.0 * done / total, 99.9)).ToString("0.0") + "%";
        }

        private readonly static FrozenSet<string>.AlternateLookup<ReadOnlySpan<char>> KnownFileExtensions = new[]
        {
            "7z",
            "css",
            "gif",
            "html",
            "ico",
            "jpg",
            "js",
            "mkv",
            "mp3",
            "mp4",
            "png",
            "rar",
            "txt",
            "webm",
            "xml",
            "zip",
        }.ToFrozenSet().GetAlternateLookup<ReadOnlySpan<char>>();
    }
}


