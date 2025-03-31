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
                    using var response = await BlueskyEnrichedApis.DefaultHttpClientNoAutoRedirect.GetAsync(url);
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

                    var dom = StringUtils.ParseHtml(await response.Content.ReadAsStringAsync());
                    var imageUrl = GetMetaProperty(dom, "og:image");
                    var result = new OpenGraphData
                    {
                        ExternalTitle = GetMetaProperty(dom, "og:title"),
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
                    }

                    if (result.ExternalDescription == result.ExternalTitle)
                        result.ExternalDescription = null;

                    result.ExternalTitle = StringUtils.TrimTextWithEllipsis(result.ExternalTitle, 300);
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
            if (string.IsNullOrEmpty(value)) return null;
            return value;
        }
    }
}

