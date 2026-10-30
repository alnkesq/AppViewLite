using AppViewLite.Models;
using Microsoft.AspNetCore.Mvc;
using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Text;
using System.Text.Json;

namespace AppViewLite.Web.Controllers
{
    /// <summary>
    /// Exposes AppViewLite's existing debug instrumentation in Prometheus text format.
    /// The endpoint is disabled unless APPVIEWLITE_HEALTH_IP contains an IP allowlist.
    /// </summary>
    [ApiController]
    public class HealthController : ControllerBase
    {
        private const int EventChartBucketCount = 120;
        private readonly BlueskyEnrichedApis apis;
        private readonly RequestContext ctx;

        // Read once at startup, like the other AppViewLite configuration values.
        private static readonly HashSet<IPAddress> AllowedAddresses = ParseAllowedAddresses(
            Environment.GetEnvironmentVariable("APPVIEWLITE_HEALTH_IP"));

        public HealthController(BlueskyEnrichedApis apis, RequestContext ctx)
        {
            this.apis = apis;
            this.ctx = ctx;
        }

        [HttpGet("/_health")]
        public IActionResult Get()
        {
            if (AllowedAddresses.Count == 0)
                return NotFound();

            var remoteAddress = NormalizeAddress(HttpContext.Connection.RemoteIpAddress);
            if (remoteAddress == null || !AllowedAddresses.Contains(remoteAddress))
                return StatusCode(StatusCodes.Status403Forbidden);

            var metrics = new PrometheusMetrics();
            metrics.Gauge("appviewlite_build_info", "AppViewLite build information; the version is exposed as a bounded label.", 1, ("version", LimitLabel(AppViewLiteInit.GitCommitVersion, "unknown")));
            AddCounters(metrics);
            AddEventCharts(metrics);
            AddLocks(metrics);
            AddPerformance(metrics);
            AddRequests(metrics);
            AddTables(metrics);

            return Content(metrics.ToString(), "text/plain; version=0.0.4; charset=utf-8", Encoding.UTF8);
        }

        private static HashSet<IPAddress> ParseAllowedAddresses(string? value)
        {
            var result = new HashSet<IPAddress>();
            if (string.IsNullOrWhiteSpace(value)) return result;

            foreach (var item in value.Split([',', ';', ' ', '\t', '\r', '\n'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            {
                if (IPAddress.TryParse(item, out var address))
                    result.Add(NormalizeAddress(address)!);
                else
                    LoggableBase.Log($"Ignoring invalid IP address in APPVIEWLITE_HEALTH_IP: {item}");
            }
            return result;
        }

        private static IPAddress? NormalizeAddress(IPAddress? address)
        {
            return address?.IsIPv4MappedToIPv6 == true ? address.MapToIPv4() : address;
        }

        private void AddCounters(PrometheusMetrics metrics)
        {
            var json = JsonSerializer.SerializeToElement(apis.GetCountersThreadSafe(ctx));
            AddNumericJsonProperties(metrics, "", json);
        }

        // The debug counter object contains a stable hierarchy of named values. Flatten
        // numeric leaves into one gauge family, keeping property names as a label.
        // Arrays are intentionally skipped so their indices cannot create unbounded labels.
        private static void AddNumericJsonProperties(PrometheusMetrics metrics, string prefix, JsonElement element)
        {
            if (element.ValueKind != JsonValueKind.Object) return;
            foreach (var property in element.EnumerateObject())
            {
                var name = string.IsNullOrEmpty(prefix) ? property.Name : prefix + "." + property.Name;
                var value = property.Value;
                if (value.ValueKind == JsonValueKind.Object)
                {
                    AddNumericJsonProperties(metrics, name, value);
                }
                else if (value.ValueKind == JsonValueKind.Number && value.TryGetDouble(out var number) && double.IsFinite(number))
                {
                    metrics.Gauge("appviewlite_debug_counter", "Numeric values from AppViewLite's debug counters.", number, ("name", name));
                }
                else if (value.ValueKind == JsonValueKind.True || value.ValueKind == JsonValueKind.False)
                {
                    metrics.Gauge("appviewlite_debug_counter", "Numeric values from AppViewLite's debug counters.", value.GetBoolean() ? 1 : 0, ("name", name));
                }
            }
        }

        private void AddEventCharts(PrometheusMetrics metrics)
        {
            var scope = BlueskyRelationships.TimeSeriesTotalTime;
            AddTimeSeries(metrics, "appviewlite_firehose_events_received_per_second", "Received firehose events per second by historical bucket.", BlueskyRelationships.FirehoseEventReceivedTimeSeries, scope);
            AddTimeSeries(metrics, "appviewlite_firehose_events_processed_per_second", "Processed firehose events per second by historical bucket.", BlueskyRelationships.FirehoseEventProcessedTimeSeries, scope);
            AddTimeSeries(metrics, "appviewlite_firehose_processing_lag_behind", "Maximum firehose processing lag behind by historical bucket.", BlueskyRelationships.FirehoseProcessingLagBehindTimeSeries, scope, maximum: true);

            var cursor = apis.primarySecondaryPair.relationshipsUnlocked
                .GetCursorsThreadSafe().MaxBy(x => x.ReceivedEvents);
            if (cursor != null)
            {
                metrics.Counter("appviewlite_firehose_received_events_total", "Total events received from the primary firehose.", cursor.ReceivedEvents, ("firehose", "primary"));
                metrics.Gauge("appviewlite_firehose_last_processed_timestamp_seconds", "Unix timestamp of the last processed primary firehose event.", ToUnixSeconds(cursor.SystemTimeLastProcessed), ("firehose", "primary"));
                metrics.Gauge("appviewlite_firehose_event_timestamp_seconds", "Unix timestamp carried by the last processed primary firehose event.", ToUnixSeconds(cursor.FirehoseTimeLastProcessed), ("firehose", "primary"));
                metrics.Gauge("appviewlite_firehose_processing_lag_seconds", "Seconds between the current time and the last processed primary firehose event timestamp.", cursor.LagBehind?.TotalSeconds ?? 0, ("firehose", "primary"));
                metrics.Gauge("appviewlite_firehose_state", "Current primary firehose state, represented by a one-valued state label.", 1, ("firehose", "primary"), ("state", cursor.State.ToString().ToLowerInvariant()));
                metrics.Gauge("appviewlite_firehose_last_raw_received_timestamp_seconds", "Unix timestamp of the last raw primary firehose message.", ToUnixSeconds(cursor.SystemTimeLastRawReceived), ("firehose", "primary"));
                metrics.Gauge("appviewlite_firehose_restart_count", "Number of primary firehose restarts.", cursor.RestartCount, ("firehose", "primary"));
            }
        }

        private static void AddTimeSeries(PrometheusMetrics metrics, string name, string help, TimeSeries series, TimeSpan scope, bool maximum = false)
        {
            var (buckets, duration) = series.GetChart(EventChartBucketCount, scope, maximum);
            for (var i = 0; i < buckets.Length; i++)
                metrics.Gauge(name, help, buckets[i], ("bucket", i.ToString(CultureInfo.InvariantCulture)), ("bucket_seconds", duration.TotalSeconds.ToString("0.###", CultureInfo.InvariantCulture)));
        }

        private void AddLocks(PrometheusMetrics metrics)
        {
            AddLock(metrics, "primary", apis.DangerousUnlockedRelationships);
            if (apis.DangerousReadOnlyReplicaUnlockedRelationships is { } replica)
                AddLock(metrics, "readonly_replica", replica);
        }

        private static void AddLock(PrometheusMetrics metrics, string name, BlueskyRelationships relationships)
        {
            var rwLock = relationships.Lock;
            metrics.Gauge("appviewlite_lock_current_readers", "Number of threads currently holding a read lock.", rwLock.CurrentReadCount, ("lock", name));
            metrics.Gauge("appviewlite_lock_waiting_readers", "Number of threads waiting to acquire a read lock.", rwLock.WaitingReadCount, ("lock", name));
            metrics.Gauge("appviewlite_lock_waiting_writers", "Number of threads waiting to acquire a write lock.", rwLock.WaitingWriteCount, ("lock", name));
            metrics.Gauge("appviewlite_lock_waiting_upgraders", "Number of threads waiting to acquire an upgradeable read lock.", rwLock.WaitingUpgradeCount, ("lock", name));
            metrics.Gauge("appviewlite_lock_recursive_readers", "Number of recursive read lock holds.", rwLock.RecursiveReadCount, ("lock", name));
            if (relationships.LastStackTraceOnWriteLockEnter is { } lastWriter)
                metrics.Gauge("appviewlite_lock_last_writer_thread_id", "Managed thread ID that most recently entered the write lock, when stack-trace instrumentation is enabled.", lastWriter.Value.ManagedThreadId, ("lock", name));
        }

        private static void AddPerformance(PrometheusMetrics metrics)
        {
            foreach (var item in AppViewLiteInit.GlobalPerformanceStatsByOperation)
            {
                var (contextKind, tableName, operation) = item.Key;
                var total = item.Value.Total;
                var labels = new (string Name, string? Value)[]
                {
                    ("context", ClassifyPath(contextKind)),
                    ("table", LimitLabel(tableName, "unknown")),
                    ("operation", LimitLabel(operation, "unknown")),
                };
                metrics.Counter("appviewlite_operation_count_total", "Total storage operations by context, table and operation.", item.Value.Count, labels);
                metrics.Gauge("appviewlite_operation_average_duration_seconds", "Average duration of storage operations.", item.Value.Count == 0 ? 0 : StopwatchTicksToSeconds(total.StopwatchTicks) / item.Value.Count, labels);
                metrics.Counter("appviewlite_operation_duration_seconds_total", "Total time spent in storage operations.", StopwatchTicksToSeconds(total.StopwatchTicks), labels);
                metrics.Counter("appviewlite_operation_seek_duration_seconds_total", "Total time spent seeking during storage operations.", StopwatchTicksToSeconds(total.SeekStopwatchTicks), labels);
                metrics.Counter("appviewlite_operation_io_reads_total", "Total IO reads performed by storage operations.", total.IoReads, labels);
                metrics.Counter("appviewlite_operation_io_read_bytes_total", "Total bytes read by storage operations.", total.IoReadBytes, labels);
                metrics.Counter("appviewlite_operation_mmap_potential_read_bytes_total", "Potential mmap read bytes reported by storage operations.", total.MmapPotentialReadBytes, labels);
                metrics.Counter("appviewlite_operation_seek_count_total", "Total seek operations performed by storage operations.", total.SeekCount, labels);
                metrics.Counter("appviewlite_operation_allocated_bytes_total", "Bytes allocated on threads executing storage operations.", total.AllocatedBytes, labels);
            }
        }

        private void AddRequests(PrometheusMetrics metrics)
        {
            var urgent = RequestContext.RecentRequestContextsUrgent.ToArray()
                .OrderByDescending(x => x.StartDate).Take(200).ToArray();
            var nonUrgent = RequestContext.RecentRequestContextsNonUrgent.ToArray()
                .OrderByDescending(x => x.StartDate).Take(200).ToArray();

            AddRequestGroup(metrics, urgent, "urgent");
            AddRequestGroup(metrics, nonUrgent, "nonurgent");
        }

        private static void AddRequestGroup(PrometheusMetrics metrics, RequestContext[] requests, string urgency)
        {
            for (var slot = 0; slot < requests.Length; slot++)
            {
                var request = requests[slot];
                var readsPrimary = request.ReadsFromPrimaryStolen + request.ReadsFromPrimaryLate;
                var writesPrimary = request.WritesToPrimaryStolen + request.WritesToPrimaryLate;
                if ((request.ReadsFromSecondary == 0 && readsPrimary == 0 && request.WriteOrUpgradeLockEnterCount == 0) || request.RequestUrl == "/debug/requests")
                    continue;

                var labels = new (string Name, string? Value)[]
                {
                    ("urgency", urgency),
                    ("request_slot", slot.ToString(CultureInfo.InvariantCulture)),
                    ("path", ClassifyPath(request.ReasonOrUrlForBucketing)),
                };
                var waitSeconds = request.TimeSpentWaitingForLocks.Elapsed.TotalSeconds;
                var primarySeconds = StopwatchTicksToSeconds(request.StopwatchTicksSpentInsidePrimaryLock);
                var secondarySeconds = StopwatchTicksToSeconds(request.StopwatchTicksSpentInsideSecondaryLock);
                metrics.Gauge("appviewlite_recent_request_age_seconds", "Age of each retained recent request.", Math.Max(0, (DateTime.UtcNow - request.StartDate).TotalSeconds), labels);
                metrics.Gauge("appviewlite_recent_request_reads_secondary", "Secondary database reads performed by each retained request.", request.ReadsFromSecondary, labels);
                metrics.Gauge("appviewlite_recent_request_reads_primary", "Primary database reads performed by each retained request.", readsPrimary, labels);
                metrics.Gauge("appviewlite_recent_request_writes_primary", "Primary database writes performed by each retained request.", writesPrimary, labels);
                metrics.Gauge("appviewlite_recent_request_write_or_upgrade_locks", "Write or upgradeable lock entries for each retained request.", request.WriteOrUpgradeLockEnterCount, labels);
                metrics.Gauge("appviewlite_recent_request_lock_wait_seconds", "Time spent waiting for locks by each retained request.", waitSeconds, labels);
                metrics.Gauge("appviewlite_recent_request_primary_lock_seconds", "Time spent holding the primary database lock by each retained request.", primarySeconds, labels);
                metrics.Gauge("appviewlite_recent_request_secondary_lock_seconds", "Time spent holding the secondary database lock by each retained request.", secondarySeconds, labels);
                metrics.Gauge("appviewlite_recent_request_max_gc_generation_inside_lock", "Maximum GC generation observed while each retained request held a database lock.", request.MaxOccurredGarbageCollectionGenerationInsideLock, labels);
            }
        }

        private void AddTables(PrometheusMetrics metrics)
        {
            var tables = apis.WithRelationshipsUpgradableLock(rels => rels.AllMultidictionaries
                .Select(x => (x.Name, x.InMemorySize, x.LastFlushed, x.OriginalWriteBytes, x.CompactationWriteBytes,
                    x.OnDiskSize, x.KeyCount, x.ValueCount, x.SliceCount, x.Behavior, x.PageIndexSize)).ToArray(), ctx);

            foreach (var table in tables)
            {
                var labels = new (string Name, string? Value)[]
                {
                    ("table", LimitLabel(table.Name, "unknown")),
                    ("behavior", table.Behavior.ToString().ToLowerInvariant()),
                };
                metrics.Gauge("appviewlite_table_disk_bytes", "Table size on disk in bytes.", table.OnDiskSize, labels);
                metrics.Gauge("appviewlite_table_buffered_bytes", "Table buffered in-memory size in bytes.", table.InMemorySize, labels);
                metrics.Gauge("appviewlite_table_page_index_bytes", "Table page key index size in bytes.", table.PageIndexSize, labels);
                metrics.Gauge("appviewlite_table_keys", "Number of keys in the table.", table.KeyCount, labels);
                metrics.Gauge("appviewlite_table_values", "Number of values in the table.", table.ValueCount, labels);
                metrics.Gauge("appviewlite_table_average_values_per_key", "Average values per key in the table.", table.KeyCount == 0 ? 0 : (double)table.ValueCount / table.KeyCount, labels);
                metrics.Gauge("appviewlite_table_slices", "Number of slices in the table.", table.SliceCount, labels);
                metrics.Gauge("appviewlite_table_original_write_bytes", "Original session write bytes for the table.", table.OriginalWriteBytes, labels);
                metrics.Gauge("appviewlite_table_compaction_write_bytes", "Compaction write bytes for the table.", table.CompactationWriteBytes, labels);
                metrics.Gauge("appviewlite_table_last_flushed_timestamp_seconds", "Unix timestamp when the table was last flushed.", ToUnixSeconds(table.LastFlushed), labels);
            }
        }

        private static string ClassifyPath(string? value)
        {
            if (string.IsNullOrEmpty(value)) return "other";
            var path = value;
            if (Uri.TryCreate(value, UriKind.Absolute, out var uri)) path = uri.AbsolutePath;
            var first = path.Split('/', StringSplitOptions.RemoveEmptyEntries).FirstOrDefault()?.ToLowerInvariant();
            if (first?.StartsWith('@') == true) return "profile";
            return first switch
            {
                "api" => "api",
                "xrpc" => "xrpc",
                "@did" => "profile",
                "feed" => "feed",
                "search" => "search",
                "notifications" => "notifications",
                "debug" => "debug",
                "img" or "watch" => "media",
                _ => "other",
            };
        }

        private static string LimitLabel(string? value, string fallback)
        {
            if (string.IsNullOrWhiteSpace(value)) return fallback;
            // Table and operation names are application-defined, not request/user input.
            // Limit length to protect the exposition format from accidental oversized labels.
            return value.Length <= 100 ? value : value[..100];
        }

        private static double StopwatchTicksToSeconds(long ticks) => ticks / (double)Stopwatch.Frequency;
        private static double ToUnixSeconds(DateTime value) => value == default ? 0 : new DateTimeOffset(value.ToUniversalTime()).ToUnixTimeMilliseconds() / 1000d;

        private sealed class PrometheusMetrics
        {
            private readonly StringBuilder output = new();
            private readonly HashSet<string> declared = new(StringComparer.Ordinal);

            public void Gauge(string name, string help, double value, params (string Name, string? Value)[] labels) => Add(name, help, "gauge", value, labels);
            public void Gauge(string name, string help, long value, params (string Name, string? Value)[] labels) => Add(name, help, "gauge", value, labels);
            public void Counter(string name, string help, double value, params (string Name, string? Value)[] labels) => Add(name, help, "counter", value, labels);
            public void Counter(string name, string help, long value, params (string Name, string? Value)[] labels) => Add(name, help, "counter", value, labels);

            private void Add(string name, string help, string type, double value, params (string Name, string? Value)[] labels)
            {
                if (!double.IsFinite(value)) return;
                if (declared.Add(name))
                {
                    output.Append("# HELP ").Append(name).Append(' ').Append(EscapeHelp(help)).Append('\n');
                    output.Append("# TYPE ").Append(name).Append(' ').Append(type).Append('\n');
                }
                output.Append(name);
                if (labels.Length != 0)
                {
                    output.Append('{');
                    for (var i = 0; i < labels.Length; i++)
                    {
                        if (i != 0) output.Append(',');
                        output.Append(labels[i].Name).Append("=\"").Append(EscapeLabel(labels[i].Value ?? "")).Append('"');
                    }
                    output.Append('}');
                }
                output.Append(' ').Append(value.ToString("R", CultureInfo.InvariantCulture)).Append('\n');
            }

            private static string EscapeHelp(string value) => value.Replace("\\", "\\\\").Replace("\n", "\\n");
            private static string EscapeLabel(string value) => value.Replace("\\", "\\\\").Replace("\n", "\\n").Replace("\"", "\\\"");
            public override string ToString() => output.ToString();
        }
    }
}
