using System;
using System.Globalization;
using System.IO;

namespace AppViewLite
{
    public static class AppViewLiteConfiguration
    {
        public static string? GetString(AppViewLiteParameter parameter)
        {
            if (parameter == default) throw AssertionLiteException.ThrowBadEnumException(parameter);
            return Environment.GetEnvironmentVariable(parameter.ToString());
        }

        public static string[]? GetStringList(AppViewLiteParameter parameter)
        {
            var s = GetString(parameter);
            if (s == null) return null;
            return s.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
        }

        public static int? GetInt32(AppViewLiteParameter parameter)
        {
            var s = GetString(parameter);
            return s != null ? int.Parse(s, CultureInfo.InvariantCulture) : null;
        }
        public static long? GetInt64(AppViewLiteParameter parameter)
        {
            var s = GetString(parameter);
            return s != null ? long.Parse(s, CultureInfo.InvariantCulture) : null;
        }
        public static double? GetDouble(AppViewLiteParameter parameter)
        {
            var s = GetString(parameter);
            return s != null ? double.Parse(s, CultureInfo.InvariantCulture) : null;
        }

        public static bool GetBool(AppViewLiteParameter parameter, bool defaultValue) => GetBool(parameter) ?? false;
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

        public static string GetDataDirectory()
        {
            var path = GetString(AppViewLiteParameter.APPVIEWLITE_DIRECTORY) ?? Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "BskyAppViewLiteData");
            return Path.TrimEndingDirectorySeparator(path);
        }

        public static void ReadEnvAndArgs(string[] args)
        {
            for (int i = 0; i < args.Length; i++)
            {
                var arg = args[i];
                if (arg.StartsWith("--", StringComparison.Ordinal))
                {
                    var name = arg.Substring(2).ToUpperInvariant().Replace('-', '_');
                    if (!name.StartsWith("APPVIEWLITE_", StringComparison.Ordinal))
                        name = "APPVIEWLITE_" + name;
                    if (!Enum.TryParse<AppViewLiteParameter>(name, false, out _))
                        throw new ArgumentException("Unrecognized argument: " + arg);
                    if (i == args.Length - 1 || args[i + 1].StartsWith("--", StringComparison.Ordinal))
                        throw new ArgumentException("Missing value for argument " + arg);

                    Environment.SetEnvironmentVariable(name, args[i + 1]);
                    i++;
                }
                else
                {
                    throw new ArgumentException("Unrecognized argument: " + arg);
                }
            }


            var envpath = GetString(AppViewLiteParameter.APPVIEWLITE_CONFIGURATION);
            if (envpath != null)
            {
                foreach (var line in StringUtils.ReadTextFile(envpath))
                {
                    var eq = line.IndexOf('=');
                    if (eq == -1) throw new Exception("Invalid line in configuration file: " + line);
                    var name = line.Substring(0, eq);
                    var value = line.Substring(eq + 1);
                    if (value.StartsWith("~/", StringComparison.Ordinal))
                        value = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), value.Substring(2));
                    if (Environment.GetEnvironmentVariable(name) is { } envval && envval != value)
                    {
                        // Too early for LoggableBase
                        Console.Error.WriteLine($"Warning: {name} is \"{envval}\" in environment, but \"{value}\" in env file. Using environment.");
                        continue;
                    }
                    Environment.SetEnvironmentVariable(name, value);
                }
            }
        }
    }

    public enum AppViewLiteParameter
    {
        None,
        APPVIEWLITE_DIRECTORY,
        APPVIEWLITE_WIKIDATA_VERIFICATION,
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
        APPVIEWLITE_PLC_DIRECTORY,
        APPVIEWLITE_DID_DOC_OVERRIDES,
        APPVIEWLITE_ALLOW_PUBLIC_READONLY_FAKE_LOGIN,
        APPVIEWLITE_LISTEN_TO_PLC_DIRECTORY,
        APPVIEWLITE_LISTEN_TO_FIREHOSE,
        APPVIEWLITE_FIREHOSES,
        APPVIEWLITE_LABEL_FIREHOSES,
        APPVIEWLITE_LISTEN_ACTIVITYPUB_RELAYS,
        APPVIEWLITE_LISTEN_NOSTR_RELAYS,
        APPVIEWLITE_YOTSUBA_HOSTS,
        APPVIEWLITE_BLOCKLIST_PATH,
        APPVIEWLITE_DISABLE_SLICE_GC,
        APPVIEWLITE_GLOBAL_PERIODIC_FLUSH_SECONDS,
        APPVIEWLITE_TABLE_WRITE_BUFFER_SIZE,
        APPVIEWLITE_NOSTR_IGNORE_REGEX,
        APPVIEWLITE_USE_READONLY_REPLICA,
        APPVIEWLITE_MAX_READONLY_STALENESS_MS_OPPORTUNISTIC,
        APPVIEWLITE_MAX_READONLY_STALENESS_MS_EXPLICIT_READ,
        APPVIEWLITE_WRITE_STACK_TRACES_ON_LOCK_ENTER,
        APPVIEWLITE_EXTERNAL_PREVIEW_SMALL_THUMBNAIL_DOMAINS,
        APPVIEWLITE_BADGE_OVERRIDE_PATH,
        APPVIEWLITE_RECENT_CHECKPOINTS_TO_KEEP,
        APPVIEWLITE_PRUNE_OLD_DAYS,
        APPVIEWLITE_PRUNE_MIN_SIZE,
        APPVIEWLITE_PRUNE_INTERVAL_DAYS,
        APPVIEWLITE_RUN_PRUNING,
        APPVIEWLITE_PRUNE_NEIGHBORHOOD_SIZE,
        APPVIEWLITE_FIREHOSE_PROCESSING_LAG_WARN_THRESHOLD,
        APPVIEWLITE_FIREHOSE_PROCESSING_LAG_ERROR_THRESHOLD,
        APPVIEWLITE_USE_PROBABILISTIC_SETS,
        APPVIEWLITE_FIREHOSE_PROCESSING_LAG_WARN_INTERVAL_MS,
        APPVIEWLITE_FIREHOSE_PROCESSING_LAG_ERROR_DROP_EVENTS,
        APPVIEWLITE_LONG_LOCK_PRIMARY_MS,
        APPVIEWLITE_LONG_LOCK_SECONDARY_MS,
        APPVIEWLITE_CHECK_NUL_FILES,
        APPVIEWLITE_ADMINISTRATIVE_DIDS,
        APPVIEWLITE_CAR_INSERTION_SEMAPHORE_SIZE,
        APPVIEWLITE_EVENT_CHART_HISTORY_DAYS,
        APPVIEWLITE_CAR_SPILL_TO_DISK_BYTES,
        APPVIEWLITE_CAR_DOWNLOAD_SEMAPHORE,
        APPVIEWLITE_DIRECT_IO,
        APPVIEWLITE_DIRECT_IO_SECTOR_SIZE,
        APPVIEWLITE_SET_THREAD_NAMES,
        APPVIEWLITE_FIREHOSE_WATCHDOG_SECONDS,
        APPVIEWLITE_CONFIGURATION,
        APPVIEWLITE_DIRECT_IO_PRINT_READS,
        APPVIEWLITE_ADDITIONAL_DIRECTORIES,
        APPVIEWLITE_ALLOW_NEW_DATABASE,
        APPVIEWLITE_QUICK_REVERSE_BACKFILL_INSTANCE,
        APPVIEWLITE_EXTERNAL_PREVIEW_DISABLE_AUTOMATIC_FOR_DOMAINS,
        APPVIEWLITE_RESET_FIREHOSE_CURSORS,
        APPVIEWLITE_FIREHOSE_THREADPOOL_BACKPRESSURE,
        APPVIEWLITE_IGNORE_SLICES_PATH,
        APPVIEWLITE_ENABLE_HACKERNEWS,
        APPVIEWLITE_ENABLE_RSS,
        APPVIEWLITE_MAX_QPS_BY_HOST,
        APPVIEWLITE_BIND_URLS,
    }
}

