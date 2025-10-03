# Configuration

Each of these options can be specified (by descending priority):
* As a command line argument (`--example-setting 1`)
* As an environment variable (`APPVIEWLITE_EXAMPLE_SETTING=1`)
* As a line in an env file (`APPVIEWLITE_EXAMPLE_SETTING=1`, use `#` for comments), whose path can be specified via `--configuration example.env`

## Main settings

* `APPVIEWLITE_BIND_URLS`: Bind IP and ports. Defaults to `https://localhost:61749,http://localhost:61750`. Use `*` instead of `localhost` to listen on all network interfaces.
* `APPVIEWLITE_DIRECTORY`: Where to store the data. Defaults to `~/BskyAppViewLiteData`
* `APPVIEWLITE_ADDITIONAL_DIRECTORIES`: Optional additional data directories (perhaps stored on different volumes) that complement `APPVIEWLITE_DIRECTORY`. When AppViewLite needs to read a file, it will first check if it exists in `APPVIEWLITE_DIRECTORY`, if it doesn't exist there, it will check the corresponding subfolders of each additional directory (if any files are missing altogether, AppViewLite will refuse to start). You can manually move large files or directories from the main `APPVIEWLITE_DIRECTORY` (for example, stored on fast solid storage) to an additional data directory (for example, stored on larger but slower rotating drives). Moving files requires scheduled downtime.
* `APPVIEWLITE_PLC_DIRECTORY_BUNDLE`: Path to an optional parquet file for quick bootstraping of the PLC directory data.
* `APPVIEWLITE_WIKIDATA_VERIFICATION`: Path to an optional parquet file with the official websites of the entities on Wikidata.
* `APPVIEWLITE_BADGE_OVERRIDE_PATH`: Path to an optional text file with badge overrides (format: `didOrHandle,badgeKind[,url[,tooltipDescription]]`) where `badgeKind` can be `verified-generic`, `verified-organization`, `verified-government` or `none`. Will be live-reloaded if it changes.
* `APPVIEWLITE_FIREHOSES`: A comma-separated list of ATProto or JetStream firehoses to listen to. Each entry can provide multiple fallback URLs (separated by `|`), and can be prefixed by `[some-stable-identifier]`. Each fallback URL is in the format `jet:domain.example` (for JetStream), or `domain.example` (for ATProto firehoses). For example, `[jetstream-bsky-network]jet:jetstream1.us-east.bsky.network|jet:jetstream2.us-east.bsky.network`. Defaults to the JetStream firehoses (jetstream*.us-*.bsky.network). Only use firehoses you trust, since DAG-CBOR signatures are not verified. For out-of-network PDSes that are not listened to by the main firehoses, use instead `APPVIEWLITE_DID_DOC_OVERRIDES` (see below).
* `APPVIEWLITE_LISTEN_TO_FIREHOSE`: Whether to listen to the firehose. Defaults to `1`.
* `APPVIEWLITE_LISTEN_TO_PLC_DIRECTORY`: Periodically fetches updates to the PLC directory. Defaults to `1`.
* `APPVIEWLITE_ALLOW_PUBLIC_READONLY_FAKE_LOGIN`: For debugging only, will allow you to "log in" to any account (but obviously you won't be able to perform PDS writes). Defaults to `0`.
* `APPVIEWLITE_READONLY`: Forbids database writes (useful for debugging data issues without the firehose interfering).
* `APPVIEWLITE_EXTERNAL_PREVIEW_SMALL_THUMBNAIL_DOMAINS`: External previews to these domains will be always displayed in compact format (not just when quoted or the post was already seen by the user), because the image doesn't generally contain useful information or is always the same for the whole site (e.g. `x.com,twitter.com,arxiv.org,paypal.me,paypal.com,t.me,cash.app`). Defaults to none.
* `APPVIEWLITE_EXTERNAL_PREVIEW_DISABLE_AUTOMATIC_FOR_DOMAINS`: Disables OpenGraph previews for the specified domains, unless already set by a post record. Use this when the OpenGraph for that domain doesn't generally contain useful information, only branding or wasted space (e.g. `x.com,twitter.com,t.me,redd.it`). Defaults to none.
* `APPVIEWLITE_EXTERNAL_PREVIEW_IGNORE_DESCRIPTION_FOR_DOMAINS`: Ignore OpenGraph descriptions for external links to the specified domains. Defaults to none.
* `APPVIEWLITE_GLOBAL_PERIODIC_FLUSH_SECONDS`: Flushes the database to disk every this number of seconds. Defaults to `600` (10 minutes). If you abruptly close the process without using a proper `CTRL+C` (`SIGINT`), you will lose at most this amount of recent data. However, consistency is still guaranteeded. Abrupt exits during a flush are also consistency-safe. Fast-growing tables might be flushed to disk earlier (but still consistency-safe).
* `APPVIEWLITE_ADMINISTRATIVE_DIDS`: List of DIDs that, when logged in, can perform privileged operations. Defaults to none. You can use `*` for local development.
* `APPVIEWLITE_CONFIGURATION`: Path to an env file that will be loaded into the environment at startup. Prior environment variables take the precendence.
* `APPVIEWLITE_ALLOW_NEW_DATABASE`: Allows AppViewLite to start a new database / empty checkpoint, if no `checkpoints/*.pb` files exist. Defaults to `0`. This is a safer default than `1`, because if no checkpoints were found for whatever reason, AppViewLite would otherwise start from scratch, and it would soon garbage collect all your previous data slices.
* `APPVIEWLITE_QUICK_REVERSE_BACKFILL_INSTANCE`: An appview that can be used to quickly bootstrap the list of followers of a user without first downloading the whole network. Defaults to `https://public.api.bsky.app`, can be disabled with `-`
* `APPVIEWLITE_MAX_QPS_BY_HOST`: Max requests per second that AppViewLite will perform against external servers (PDSes, RSS sites, etc.). Don't include `www.` or other prefixes. Defaults to `*=5` (at most 5 requests per second to the same server).
## Storage (low level configuration)
* `APPVIEWLITE_USE_READONLY_REPLICA`: If enabled, most requests will be served from a readonly snapshot of the database (so that we don't have to wait for any current write lock to complete). Defaults to `1`.
* `APPVIEWLITE_MAX_READONLY_STALENESS_MS_OPPORTUNISTIC`: How many milliseconds can the readonly replica lag behind the primary, ideally. Default: `2000`.
* `APPVIEWLITE_MAX_READONLY_STALENESS_MS_EXPLICIT_READ`: How many milliseconds can the readonly replica lag behind the primary, at most. Default: `4000`.
* `APPVIEWLITE_LONG_LOCK_PRIMARY_MS`, `APPVIEWLITE_LONG_LOCK_SECONDARY_MS`: Log in `/debug/requests` when a lock is taken for more than this amount of time. Defaults to `30` and `50` ms respectively.
* `APPVIEWLITE_TABLE_WRITE_BUFFER_SIZE`: If the buffer of a table grows larger than this amount of bytes, it will be flushed to disk even before the next `APPVIEWLITE_GLOBAL_PERIODIC_FLUSH_SECONDS`. Defaults to `10485760` (10 MB).
* `APPVIEWLITE_DISABLE_SLICE_GC`: If enabled, old slices won't be garbage collected, even after compactation (they will be however ignored and not loaded).
* `APPVIEWLITE_RECENT_CHECKPOINTS_TO_KEEP`: How many old checkpoints to keep before garbage collecting old slices. Defaults to `3`.
* `APPVIEWLITE_USE_PROBABILISTIC_SETS`: Uses a probabilistic cache to reduce disk reads. Defaults to `1`, but while developing you might want to set it to `0` since it increases startup time.
* `APPVIEWLITE_FIREHOSE_PROCESSING_LAG_WARN_THRESHOLD`: Prints a warning if the backlog of pending records to process is above this threshold. Defaults to `100`.
* `APPVIEWLITE_FIREHOSE_PROCESSING_LAG_ERROR_THRESHOLD`: Errors out if the backlog of pending records to process is above this threshold. Defaults to `10000`.
* `APPVIEWLITE_FIREHOSE_PROCESSING_LAG_ERROR_DROP_EVENTS`: Drops firehose events instead of terminating the process if the error threshold is reached. Defaults to `0` (fatal exit).
* `APPVIEWLITE_FIREHOSE_PROCESSING_LAG_WARN_INTERVAL_MS`: How often to print lag behind warning messages, at most. Defaults to `500` ms.
* `APPVIEWLITE_CHECK_NUL_FILES`: On startup, verifies that none of the slice files start or end with long sequences of `NUL` bytes, which could indicate corruption. Defaults to `1`.
* `APPVIEWLITE_CAR_DOWNLOAD_SEMAPHORE`: How many CAR files can be downloaded at once. Defaults to `8`.
* `APPVIEWLITE_CAR_SPILL_TO_DISK_BYTES`: Amount of memory after which we spill CAR entries to a temporary file on disk. This is necessary because reading a CAR file requires random access to previous parts of the file. Shared across all currently running imports. Defaults to `67108864` (64MB)
* `APPVIEWLITE_CAR_INSERTION_SEMAPHORE_SIZE`: How many running CAR imports can perform a database insertion at once. Defaults to `2`.
* `APPVIEWLITE_EVENT_CHART_HISTORY_DAYS`: How many days of statistics (with 1 second granularity) are preserved, for `/debug/event-charts`. Defaults to `2`.
* `APPVIEWLITE_FIREHOSE_WATCHDOG_SECONDS`: If we observe no events from the firehose for this amount of seconds, restart the websocket connection. Defaults to `120`.
* `APPVIEWLITE_SET_THREAD_NAMES`: Sets the name of the current thread to a description of the current context, every time a database lock is taken, for easier debugging. Slow things down quite a bit.
* `APPVIEWLITE_DIRECT_IO`: Uses direct IO (`O_DIRECT`) instead of memory mapping for some reads that are unlikely to be necessary again in the near future. Defaults to `1`.
* `APPVIEWLITE_DIRECT_IO_SECTOR_SIZE`: Sets the sector block size for direct reads. Must be a multiple of the disk sector size. If AppViewLite crashes on startup and you have an enterprise disk with 4KB sectors, try changing this to `4096`. Defaults to `512`.
* `APPVIEWLITE_DIRECT_IO_PRINT_READS`: Prints to stderr every time a direct IO read is performed, with path, offset and length.
* `APPVIEWLITE_FIREHOSE_THREADPOOL_BACKPRESSURE`: how many pending records to process can accumulate before the firehose websocket listener blocks waiting for work to complete. Defaults to `20000`.
* `APPVIEWLITE_RESET_FIREHOSE_CURSORS`: URLs of the firehoses whose cursors should be reset on startup. Defaults to `*` to avoid a known [JetStream issue](https://github.com/bluesky-social/jetstream/issues/27).
* `APPVIEWLITE_DRAIN_FIREHOSE_BEFORE_CURSOR_CAPTURE`: Uses a more precise checkpointing for firehose cursor save and restore (experimental). Defaults to `0`.
* `APPVIEWLITE_DIRECT_IO_BLOCK_CACHE_CAPACITY_MB`: Size of the direct IO read cache for very small reads (≤ 2 sectors), in megabytes. Defaults to `128`.
* `APPVIEWLITE_DIRECT_IO_MULTIBLOCK_CACHE_CAPACITY`: Size of the direct IO read cache for medium-sized reads (≥ 3 sectors, but small enough that mmap is not used), in megabytes. Defaults to `128`.
* `APPVIEWLITE_LOW_DISK_SPACE_WARNING_MB`: Displays a warning on the sidebar when the available disk space is below this threshold, in megabytes. Defaults to `4096` (4 GB), enough for 1-2 days of firehose data.

## Handle and DID doc resolution
* `APPVIEWLITE_DNS_SERVER`: DNS server for TXT resolutions. Defaults to `1.1.1.1`
* `APPVIEWLITE_USE_DNS_OVER_HTTPS`: Whether the DNS server supports DNS Over HTTPS. Defaults to `1`.
* `APPVIEWLITE_HANDLE_TO_DID_MAX_STALE_HOURS`: For how many hours we cache handle resolutions. Defaults to 10 days.
* `APPVIEWLITE_DID_DOC_MAX_STALE_HOURS`: For how many hours we cache DID docs. If the PLC directory is up to date (by this amount), we cache indefinitely. Defaults to 2 days.
* `APPVIEWLITE_PLC_DIRECTORY`: Alternate PLC directory prefix. Defaults to `https://plc.directory`
* `APPVIEWLITE_DID_DOC_OVERRIDES`: Path to an optional text file, where each line is `did:plc:example pds.example handle.example`. Will be reloaded dynamically if it changes. The listed PDSes will be listened from directly, without relying on the main firehose.

## Administrative rules
* `APPVIEWLITE_BLOCKLIST_PATH`: path to an `.ini` file whose sections can be `[noingest]`, `[nodisplay]`, `[nooutboundconnect]` (or combinations, like `[noingest,nodisplay]`) to block specific DIDs or domains (and all their subdomains) of all types (PDSes, Mastodon instances, handles, Mastodon external media...)

   * `[noingest]` ignores all the posts coming from the various firehoses for the specified DIDs or domains. By default, it includes various cross-protocol mirrors (AppViewLite is already multi-protocol)
   * `[nodisplay]` prevents post and profile data for the specified DIDs or domains from being displayed.
   * `[nooutboundconnect]` prevents outbound HTTP traffic to the specified DIDs or domains. Image thumbnails and profile pictures won't be available.
   * `[blockall]` is a shorthand for `[noingest,nodisplay,nooutboundconnect]`
   * `[allowall]` allows you to override the default rules for the specified domains.

You can also use regular expressions (e.g. `regex:^example$`), but for best performance you should minimize the number of such rules (you can use the `|` regex operator to consolidate them).
You can use `#` for comments (whole line, or partial).
It will be live-reloaded if it changes.

## Additional protocols
By default, only ATProto/Bluesky is enabled.<br>
You can however enable additional protocols:

### ActivityPub (Fediverse)
* `APPVIEWLITE_LISTEN_ACTIVITYPUB_RELAYS`: listens to the specified ActivityPub relays. Example: `fedi.buzz`. Defaults to none.

### Yotsuba (Imageboards)
* `APPVIEWLITE_YOTSUBA_HOSTS`: retrieves threads from the specified imageboards. It's a comma-separated list of imageboards, each one composed of `+`-separated parameters: `HOST+CATALOG_URL_TEMPLATE+THREAD_URL_TEMPLATE+THUMB_URL_TEMPLATE+BOARDS_JSON_URL_OR_HARDCODED_BOARDS` where:
   * `HOST` is the domain of the imageboard
   * `CATALOG_URL_TEMPLATE` is the URL (even relative) of `catalog.json` (with `{0}` board placeholder). Defaults to `/{0}/catalog.json`
   * `THREAD_URL_TEMPLATE` is the URL (even relative) of a thread  (with `{0}` board placeholder and `{1}` thread ID placeholder). Defaults to `/{0}/res/{1}.html`
   * `THUMB_URL_TEMPLATE` is the URL (even relative) of thumbnails (with `{0}` board placeholder and `{1}` image ID placeholder). Defaults to `/{0}/thumb/{1}.png`
   * `BOARDS_JSON_URL_OR_HARDCODED_BOARDS` is either the URL (even relative) of `/boards.json`, or a `/`-separated list of boards, optionally specifying a display name within brackets. Defaults to `/boards.json`, but be aware that many ViChan boards don't support this API endpoint, so you'll have to hardcode the list of boards as shown below. Make sure not to add combined virtual boards that aggregate from multiple boards, because images won'work.

Examples:
* 4chan: `boards.4chan.org+https://a.4cdn.org/{0}/catalog.json+https://boards.4chan.org/{0}/thread/{1}+https://i.4cdn.org/{0}/{1}s.jpg+https://a.4cdn.org/boards.json`
* LainChan (ViChan): `lainchan.org++++λ(Programming)/Δ(Do It Yourself)/sec(Security)/r(Random)`

### Nostr
* `APPVIEWLITE_LISTEN_NOSTR_RELAYS`: Listens to the specified Nostr relays. Example: `nos.lol,bostr.bitcointxoko.com`. Defaults to none.
* `APPVIEWLITE_NOSTR_IGNORE_REGEX`: Don't ingest posts matching the specified regex.

### RSS
* `APPVIEWLITE_ENABLE_RSS`: Enables support for RSS feeds. Defaults to `0`.
The RSS protocol provides support for Tumblr, Reddit and YouTube as well. Feeds are refreshed periodically as long as at least one user subscribes to them, or when the corresponding profile page is opened.

### HackerNews
* `APPVIEWLITE_ENABLE_HACKERNEWS`: Enables support for [HackerNews](https://news.ycombinator.com). Defaults to `0`.

## Image proxying and caching
* `APPVIEWLITE_SERVE_IMAGES`: Enables image serving, instead of relying on an external CDN.
* `APPVIEWLITE_CDN`: Image CDN domain. Defaults to `cdn.bsky.app` (unless `APPVIEWLITE_SERVE_IMAGES` is set, or the image is from a non-ATProto protocol)
* `APPVIEWLITE_IMAGE_CACHE_DIRECTORY`: Where to cache the thumbnails. Defaults to `$APPVIEWLITE_DIRECTORY/image-cache`
* `APPVIEWLITE_CACHE_AVATARS`: Whether avatar thumbnails should be cached to disk. Defaults to `1`.
* `APPVIEWLITE_CACHE_FEED_THUMBS`: Whether feed image thumbnails and profile banners should be cached to disk. Defaults to `0`.

You can also add a domain to the `[nooutboundconnect]` section of `APPVIEWLITE_BLOCKLIST_PATH` (see above).

## Pruning
Pruning can be used to free up disk space, by splitting each slice into a *preserved* and a *pruned* part. The *preserved* part will contain all recent Firehose posts, plus all historical posts from users that have ever used the current AppViewLite instance, and the users they frequently interact with or follow. *Pruned* slices can be deleted or moved to offline storage.

Pruning does *not* run in the background, it requires temporary maintanance downtime.

There's currently no way of merging *preserved* and *pruned* slices back together, or to load pruned slices anyways, so make sure you run a [backup](Backup.md) first.

* `APPVIEWLITE_RUN_PRUNING`: Performs any pending pruning on startup (defaults to `0`).
* `APPVIEWLITE_PRUNE_OLD_DAYS`: Content older than this number of days *might* be pruned. Content newer than this is always preserved, no matter who it is from. Defaults to `30` days.
* `APPVIEWLITE_PRUNE_NEIGHBORHOOD_SIZE`: Preserves all the content that involves this number of users with the highest neighborhood score. Pluggable protocol profiles are always preserved and don't count against this threshold. Defaults to `1000000` (one million).
* `APPVIEWLITE_PRUNE_MIN_SIZE`: Only slices larger than this amount of bytes will be considered for pruning. Smaller ones will be preserved in full. Defaults to `1073741824` (1GB)
* `APPVIEWLITE_PRUNE_INTERVAL_DAYS`: Only slices that were last pruned or written this amount of days ago will be considered for pruning. Defaults to `10`.


*Preserved* slices have names in the format

* `startTime-endTime.col*.dat` or
* `startTime-endTime-pruneId.col*.dat` (where `pruneId` is even),

*Pruned* slices have names in the format
* `startTime-endTime-pruneId.col*.dat` (where `pruneId` is odd).

AppViewLite will refuse to start if the file of any `preserved` slice is missing, but will work fine if `pruned` slices are missing (in fact, they wouldn't be loaded even if they existed).

The neighborhood score for a user is the sum of:

* Follows
   * Infinity if they're followed by an AppViewLite user
   * 9 for each follow from a followee of an AppViewLite user
   * Infinity if they follow an AppViewLite user
   * 3 if they follow a followee of an AppViewLite user

* Likes
   * 12 for each like from an AppViewLite user
   * 3 for each like from a followee of an AppViewLite user
   * 3 for each like they send to an AppViewLite user
   * 1 for each like they send to a followee of an AppViewLite user

* `APPVIEWLITE_IGNORE_SLICES_PATH`: Alternatively, instead of pruning as described above, you can manually remove database slices altogether. `APPVIEWLITE_IGNORE_SLICES_PATH` is the path to a text file where each line is a database slice (example: `post-data-time-first-2/638754262903104348-638793205252577381`, without column number or ext) that AppViewLite should ignore (for example, because you manually deleted it or migrated offline). Be careful, not all tables can be safely ignored. Known SAFE tables are `user-to-recent-posts-2`, `post-text-approx-time-32`, `post-data-time-first-2`.
* `APPVIEWLITE_IGNORE_MISSING_SLICES`: If a slice is missing from the file system, ignore that slice instead of failing to start. Defaults to `0`.
