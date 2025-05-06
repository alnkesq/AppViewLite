using AngleSharp.Dom;
using AngleSharp.Html.Dom;
using AppViewLite.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite
{
    public static class OpenGraph
    {
        public static async Task<OpenGraphData> TryRetrieveOpenGraphDataAsync(Uri url)
        {
            try
            {
                var redirects = 0;
                while (true)
                {

                    using var response = await BlueskyEnrichedApis.DefaultHttpClientOpenGraph.GetAsync(url);
                    var redirectUrl = response.GetRedirectLocationUrl();
                    if (redirectUrl != null)
                    {
                        if (redirects++ >= 5) throw new Exception("Too many redirects.");
                        url = redirectUrl;
                        continue;
                    }
                    if (!response.IsSuccessStatusCode)
                    {
                        return new OpenGraphData
                        {
                            ExternalUrl = url.AbsoluteUri,
                            HttpStatusCode = response.StatusCode,
                            DateFetched = DateTime.UtcNow,
                        };
                    }

                    var html = await response.Content.ReadAsStringAsync();
                    var dom = StringUtils.ParseHtml(html);
                    var imageUrl = GetMetaProperty(dom, "og:image");
                    var pageTitle = StringUtils.NormalizeNull(dom.QuerySelector("title")?.TextContent?.Trim());
                    var result = new OpenGraphData
                    {
                        ExternalTitle = GetMetaProperty(dom, "og:title") ?? pageTitle,
                        ExternalDescription = GetMetaProperty(dom, "og:description"),
                        DateFetched = DateTime.UtcNow,
                        ExternalUrl = url.AbsoluteUri,
                        ExternalThumbnailUrl = (imageUrl != null ? new Uri(url, imageUrl) : null)?.AbsoluteUri,
                    };

                    if (url.HasHostSuffix("tumblr.com"))
                    {
                        // Title is just the author's display name
                        result.ExternalTitle = result.ExternalDescription;
                        result.ExternalDescription = null;


                        if (result.ExternalTitle != null)
                        {
                            var dot = result.ExternalTitle.IndexOf('·');
                            if (dot != -1)
                                result.ExternalTitle = result.ExternalTitle.Substring(dot + 1).Trim(); // trims the "💬 1  🔁 2  ❤️ 3 ·" prefix 
                            else if (result.ExternalTitle.Contains("💬") && result.ExternalTitle.Contains("🔁") && result.ExternalTitle.Contains("❤️"))
                                result.ExternalTitle = pageTitle;
                        }

                        if (result.ExternalThumbnailUrl != null)
                        {
                            var thumbUrl = new Uri(result.ExternalThumbnailUrl);
                            if (thumbUrl.HasHostSuffix("media.tumblr.com"))
                            {
                                var segments = thumbUrl.GetSegments();
                                var size = segments.ElementAtOrDefault(2);
                                if (size?.Contains("128x128") == true)
                                {
                                    result.ExternalThumbnailUrl = null; // avatar thumbnail
                                }
                            }
                        }
                    }

                    if (result.ExternalDescription == result.ExternalTitle)
                        result.ExternalDescription = null;

                    result.ExternalTitle = StringUtils.TrimTextWithEllipsis(result.ExternalTitle, 300);

                    if (BlueskyEnrichedApis.ExternalDomainsIgnoreDescription.Contains(StringUtils.GetDomainTrimWww(url)))
                        result.ExternalDescription = null;
                    else
                        result.ExternalDescription = StringUtils.TrimTextWithEllipsis(result.ExternalDescription, 1000);
                    return result;
                }
            }
            catch (Exception ex)
            {
                var httpException = ex as HttpRequestException;
                return new OpenGraphData
                {
                    ExternalUrl = url.AbsoluteUri,
                    HttpError = httpException?.HttpRequestError,
                    DateFetched = DateTime.UtcNow,
                    Error = httpException == null ? ex.Message : null,
                };
            }

        }

        private static string? GetMetaProperty(IHtmlDocument document, string name)
        {
            var value = document.Head?.Descendants().OfType<IElement>().FirstOrDefault(x => x.TagName == "META" && x.GetAttribute("property") == name)?.GetAttribute("content")?.Trim();
            return StringUtils.NormalizeNull(value);
        }
    }
}

