using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite
{
    public static class AppViewLiteConfiguration
    {
        public static string? GetString(AppViewLiteParameter parameter)
        {
            if (parameter == default) throw new ArgumentException();
            return Environment.GetEnvironmentVariable(parameter.ToString());
        }
        public static int? GetInt32(AppViewLiteParameter parameter)
        {
            var s = GetString(parameter);
            return s != null ? int.Parse(s) : null;
        }
        public static bool? GetBool(AppViewLiteParameter parameter)
        {
            var s = GetString(parameter)?.ToLowerInvariant();
            return s switch
            {
                "1" or "y" or "true" => true,
                "0" or "n" or "false" => false,
                null => null,
                _ => throw new Exception($"Unparseable boolean configuration: {parameter}={s}")
            };
        }
    }

    public enum AppViewLiteParameter
    { 
        None,
        APPVIEWLITE_DIRECTORY,
        APPVIEWLITE_WIKIDATA_VERIFICATION,
        APPVIEWLITE_PRINT_LONG_LOCKS_MS,
        APPVIEWLITE_PLC_DIRECTORY_BUNDLE,
        APPVIEWLITE_READONLY,
        APPVIEWLITE_CDN,
        APPVIEWLITE_DNS_SERVER,
        APPVIEWLITE_USE_DNS_OVER_HTTPS,
        APPVIEWLITE_HANDLE_TO_DID_MAX_STALE_HOURS,
        APPVIEWLITE_DID_DOC_MAX_STALE_HOURS,
        APPVIEWLITE_IMAGE_CACHE_DIRECTORY,
        APPVIEWLITE_CACHE_AVATARS,
        APPVIEWLITE_CACHE_FEED_THUMBS,
        APPVIEWLITE_SERVE_IMAGES,
    }
}

