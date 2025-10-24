using Microsoft.Win32.SafeHandles;
using AppViewLite.Storage;
using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace AppViewLite
{
    public static class AppViewLiteInit
    {
        public static BlueskyEnrichedApis Init(string[] args)
        {
            AppViewLiteConfiguration.ReadEnvAndArgs(args);
            LoggableBase.Initialize();
            AppDomain.CurrentDomain.UnhandledException += (_, e) =>
            {
                LoggableBase.Log("AppDomain.CurrentDomain.UnhandledException: " + e.ExceptionObject);
                LoggableBase.FlushLog();
            };
            TaskScheduler.UnobservedTaskException += (_, e) =>
            {
                LoggableBase.Log("TaskScheduler.UnobservedTaskException: " + e.Exception + ", Observed: " + e.Observed);
                LoggableBase.FlushLog();
            };
            CombinedPersistentMultiDictionary.LogOperationCallback = LogOperationCallback;
            CombinedPersistentMultiDictionary.EnsureHasOperationContext = EnsureHasOperationContext;
            CombinedPersistentMultiDictionary.UseDirectIo = AppViewLiteConfiguration.GetBool(AppViewLiteParameter.APPVIEWLITE_DIRECT_IO) ?? true;
            CombinedPersistentMultiDictionary.DiskSectorSize = AppViewLiteConfiguration.GetInt32(AppViewLiteParameter.APPVIEWLITE_DIRECT_IO_SECTOR_SIZE) ?? 512;
            CombinedPersistentMultiDictionary.PrintDirectIoReads = AppViewLiteConfiguration.GetBool(AppViewLiteParameter.APPVIEWLITE_DIRECT_IO_PRINT_READS) ?? false;
            var directIoBlockCacheCapacityBytes = (AppViewLiteConfiguration.GetInt32(AppViewLiteParameter.APPVIEWLITE_DIRECT_IO_BLOCK_CACHE_CAPACITY_MB) ?? 128) * 1024 * 1024;
            var directIoMultiBlockCacheCapacity = AppViewLiteConfiguration.GetInt32(AppViewLiteParameter.APPVIEWLITE_DIRECT_IO_MULTIBLOCK_CACHE_CAPACITY) ?? 128 * 1024;

            if (directIoBlockCacheCapacityBytes != 0)
            {
                var singleBlockCache = new ConcurrentFullEvictionCache<(long, SafeFileHandle), byte[]>(directIoBlockCacheCapacityBytes / CombinedPersistentMultiDictionary.DiskSectorSize);
                var multiBlockCache = new ConcurrentFullEvictionCache<(long, int, SafeFileHandle), byte[]>(directIoMultiBlockCacheCapacity);
                var multiBlockApproxSize = 0L;
                multiBlockCache.AfterReset += () =>
                {
                    multiBlockApproxSize = 0;
                };
                multiBlockCache.ValueAdded += value =>
                {
                    var updated = Interlocked.Add(ref multiBlockApproxSize, value.Length);
                    if (updated > directIoBlockCacheCapacityBytes)
                        multiBlockCache.Reset();
                };
                CombinedPersistentMultiDictionary.DirectIoReadCache = new DirectIoReadCache(singleBlockCache.GetOrAdd, multiBlockCache.GetOrAdd, BlueskyRelationships.AllocUnaligned, () => 
                {
                    return new
                    {
                        SingleBlockCache = singleBlockCache.GetCounters(),
                        MultiBlockCache = multiBlockCache.GetCounters(),
                        MultiBlockCacheBytes = multiBlockCache.Dictionary.Values.Sum(x => x.Length),
                        MultiBlockCacheApproxBytes = multiBlockApproxSize,

                    };
                }, () =>
                {
                    singleBlockCache.Reset();
                    multiBlockCache.Reset();
                });
            }

            GitCommitVersion = TryGetGitCommit();

            var ignoreSlicesPath = AppViewLiteConfiguration.GetString(AppViewLiteParameter.APPVIEWLITE_IGNORE_SLICES_PATH);
            var ignoreSlices = ignoreSlicesPath != null ? StringUtils.ReadTextFile(ignoreSlicesPath).Select(x =>
            {
                var parts = x.Split('/');
                if (parts.Length != 2 || parts[0].Length == 0 || parts[1].Length == 0 || parts[0].AsSpan().ContainsAny('/', '\\', ' ')) throw new ArgumentException("Invalid line in APPVIEWLITE_IGNORE_SLICES_PATH: " + x);
                return (parts[0], SliceName.ParseBaseName(parts[1]));
            }).ToHashSet() : [];
            CombinedPersistentMultiDictionary.TreatMissingSlicesAsPruned = AppViewLiteConfiguration.GetBool(AppViewLiteParameter.APPVIEWLITE_IGNORE_MISSING_SLICES, false);
            CombinedPersistentMultiDictionary.IsPrunedSlice = (directory, sliceName) =>
            {
                if ((sliceName.PruneId % 2) == 1) return true;
                var directoryName = Path.GetFileName(directory);
                if (ignoreSlices.Contains((directoryName, sliceName))) return true;
                return false;
            };

            var additionalDirectories = AppViewLiteConfiguration.GetStringList(AppViewLiteParameter.APPVIEWLITE_ADDITIONAL_DIRECTORIES) ?? [];

            foreach (var additional in additionalDirectories)
            {
                var checkpoints = Path.Combine(additional, "checkpoints");
                if (Directory.Exists(checkpoints))
                {
                    if (Directory.EnumerateFiles(checkpoints, "*.pb").Any())
                        BlueskyRelationships.ThrowFatalError("Checkpoint files must only exist in $APPVIEWLITE_DIRECTORY, not in $APPVIEWLITE_ADDITIONAL_DIRECTORIES.");
                }
            }

            var dataDirectory = AppViewLiteConfiguration.GetDataDirectory();
            CombinedPersistentMultiDictionary.ToPhysicalPath = path =>
            {
                if (Path.DirectorySeparatorChar == '\\')
                    path = path.Replace('/', '\\');

                if (File.Exists(path)) return path;
                if (!path.StartsWith(dataDirectory + Path.DirectorySeparatorChar, StringComparison.Ordinal)) return path;
                var relativePath = Path.GetRelativePath(dataDirectory, path);
                BlueskyRelationships.Assert(!relativePath.StartsWith("../", StringComparison.Ordinal) && !relativePath.StartsWith("..\\", StringComparison.Ordinal));
                BlueskyRelationships.Assert(!relativePath.StartsWith("/", StringComparison.Ordinal) && !relativePath.StartsWith("\\", StringComparison.Ordinal));
                foreach (var additional in additionalDirectories)
                {
                    var newPath = Path.Combine(additional, relativePath);
                    if (File.Exists(newPath)) return newPath;
                }
                return path;
            };

            BlueskyRelationships.CreateTimeSeries();
            var relationships = new BlueskyRelationships(
                dataDirectory,
                AppViewLiteConfiguration.GetBool(AppViewLiteParameter.APPVIEWLITE_READONLY) ?? false,
                [dataDirectory, .. additionalDirectories]);

            relationships.MaybeEnterWriteLockAndPrune();
            var primarySecondaryPair = new PrimarySecondaryPair(relationships);

            var apis = new BlueskyEnrichedApis(primarySecondaryPair);

            foreach (var table in relationships.AllMultidictionaries)
            {
                table.PendingCompactationReadyForCommit += () =>
                {
                    apis.WithRelationshipsWriteLock(rels => 
                    {
                        table.MaybeCommitPendingCompactation();
                    }, RequestContext.CreateForFirehose("CommitCompactation"));
                };
            }


            Indexer.InitializeFirehoseThreadpool(apis);

            apis.RegisterPluggableProtocol(typeof(AppViewLite.PluggableProtocols.ActivityPub.ActivityPubProtocol));
            apis.RegisterPluggableProtocol(typeof(AppViewLite.PluggableProtocols.Nostr.NostrProtocol));
            apis.RegisterPluggableProtocol(typeof(AppViewLite.PluggableProtocols.Yotsuba.YotsubaProtocol));
            apis.RegisterPluggableProtocol(typeof(AppViewLite.PluggableProtocols.HackerNews.HackerNewsProtocol));
            apis.RegisterPluggableProtocol(typeof(AppViewLite.PluggableProtocols.Rss.RssProtocol)); // lowest priority for TryGetDidOrLocalPathFromUrlAsync

            BlueskyEnrichedApis.Instance = apis;
            return apis;
        }

        private static void EnsureHasOperationContext()
        {
            if (hasLogOperationContext == 0 && BlueskyRelationshipsClientBase.CurrentThreadRequestContext != null)
            {
                LoggableBase.Log("Missing LogOperation for " + BlueskyRelationshipsClientBase.CurrentThreadRequestContext);
            }
        }

        [ThreadStatic] private static int hasLogOperationContext;
        private static IDisposable? LogOperationCallback(string tableName, string operation, object? arg)
        {
            var ctx = BlueskyRelationshipsClientBase.CurrentThreadRequestContext!;
            if (ctx == null) return null;
            ctx = ctx.RootOnBehalfOf;

            var begin = PerformanceSnapshot.Capture();

            var threadId = Environment.CurrentManagedThreadId;
            hasLogOperationContext++;
            return new DelegateDisposable(() => 
            {
                hasLogOperationContext--;
                BlueskyRelationships.Assert(Environment.CurrentManagedThreadId == threadId, "LogOperation completed on a thread other than the original thread.");
                var end = PerformanceSnapshot.Capture();

                var delta = end - begin;
                GlobalPerformanceStatsByOperation.AddOrUpdate((ctx.FirehoseReasonOrUrlForBucketing, tableName, operation),
                    addValueFactory: static (_, delta) => (1L, delta),
                    updateValueFactory: static (_, prev, delta) => (prev.Count + 1, prev.Total + delta),
                    delta);
                ctx.OperationLogEntries.Add(new OperationLogEntry(begin, end, tableName, operation, arg));
            });
        }


        public readonly static ConcurrentDictionary<(string RequestContextKind, string TableName, string Operation), (long Count, PerformanceSnapshot Total)> GlobalPerformanceStatsByOperation = new();
        public static string? GitCommitVersion;

        private static string? TryGetGitCommit()
        {
            try
            {
                var processStartInfo = new ProcessStartInfo("git", ["log", "-1", "--pretty=format:%H %ad", "--date=short"]);
                processStartInfo.RedirectStandardOutput = true;
                processStartInfo.RedirectStandardError = true;
                processStartInfo.WorkingDirectory = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
                using var process = Process.Start(processStartInfo)!;
                var version = process.StandardOutput.ReadToEnd().Trim();
                process.WaitForExit();
                if (process.ExitCode != 0)
                {
                    LoggableBase.Log("git log returned exit code " + process.ExitCode);
                    return null;
                }
                return version;
            }
            catch (Exception ex)
            {
                LoggableBase.LogNonCriticalException("git log failed", ex);
                return null;
            }
        }

    }
}

