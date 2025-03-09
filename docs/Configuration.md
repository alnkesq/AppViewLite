# Configuration

You can set the following environment variables:

* `APPVIEWLITE_DIRECTORY`: Where to store the data. Defaults to `~/BskyAppViewLiteData`
* `APPVIEWLITE_PLC_DIRECTORY_BUNDLE`: Path to an optional parquet file for quick bootstraping of the PLC directory data.
* `APPVIEWLITE_WIKIDATA_VERIFICATION`: Path to an optional parquet file with the official websites of the entities on Wikidata.
* `APPVIEWLITE_BADGE_OVERRIDE_PATH`: Path to an optional text file with badge overrides (format: `didOrHandle,badgeKind,[url,[tooltipDescription]]`) where `badgeKind` can be `verified-generic`, `verified-organization`, `verified-government` or `none`. Will be live-reloaded if it changes.
* `APPVIEWLITE_FIREHOSES`: A comma-separated list of ATProto or JetStream firehoses to listen to. Defaults to `jet:jetstream.atproto.tools`, alternatively you can use `bsky.network`. Only use trusted firehoses. For custom PDSes, use instead `APPVIEWLITE_DID_DOC_OVERRIDES` (see below).
* `APPVIEWLITE_LISTEN_TO_FIREHOSE`: Whether to listen to the firehose. Defaults to `1`.
* `APPVIEWLITE_LISTEN_TO_PLC_DIRECTORY`: Periodically fetches updates to the PLC directory. Defaults to `1`.
* `APPVIEWLITE_ALLOW_PUBLIC_READONLY_FAKE_LOGIN`: For debugging only, will allow you to "log in" to any account (but obviously you won't be able to perform PDS writes). Defaults to `0`.
* `APPVIEWLITE_READONLY`: Forbids database writes (useful for debugging data issues without the firehose interfering).
* `APPVIEWLITE_EXTERNAL_PREVIEW_SMALL_THUMBNAIL_DOMAINS`: External previews to these domains will be always displayed in compact format (not just when quoted or the post was already seen by the user).
* `APPVIEWLITE_GLOBAL_PERIODIC_FLUSH_SECONDS`: Flushes the database to disk every this number of seconds. Defaults to `600` (10 minutes). If you abruptly close the process without using a proper `CTRL+C` (`SIGINT`), you will lose at most this amount of recent data. However, consistency is still guaranteeded. Abrupt exists during a flush are also consistency-safe. Fast-growing tables might be flushed to disk earlier (but still consistency-safe).

## Storage (low level configuration)
* `APPVIEWLITE_USE_READONLY_REPLICA`: If enabled, most requests will be served from a readonly snapshot of the database (so that we don't have to wait for any current write lock to complete). Defaults to `1`.
* `APPVIEWLITE_MAX_READONLY_STALENESS_MS_OPPORTUNISTIC`: How many milliseconds can the readonly replica lag behind the primary, ideally. Default: `2000`.
* `APPVIEWLITE_MAX_READONLY_STALENESS_MS_EXPLICIT_READ`: How many milliseconds can the readonly replica lag behind the primary, at most. Default: `4000`.
* `APPVIEWLITE_PRINT_LONG_READ_LOCKS_MS`, `APPVIEWLITE_PRINT_LONG_WRITE_LOCKS_MS`, `APPVIEWLITE_PRINT_LONG_UPGRADEABLE_LOCKS_MS`: Print a stacktrace when we spend more than this amount of time while holding a lock.
* `APPVIEWLITE_TABLE_WRITE_BUFFER_SIZE`: If the buffer of a table grows larger than this amount of bytes, it will be flushed to disk even before the next `APPVIEWLITE_GLOBAL_PERIODIC_FLUSH_SECONDS`. Defaults to `10485760` (10 MB).
* `APPVIEWLITE_DISABLE_SLICE_GC`: If enabled, old slices won't be garbage collected, even after compactation (they will be however ignored and not loaded).
* `APPVIEWLITE_RECENT_CHECKPOINTS_TO_KEEP`: How many old checkpoints to keep before garbage collecting old slices. Defaults to `3`.
* `APPVIEWLITE_FATAL_ERROR_LOG_DIRECTORY`: Path to a directory where to dump fatal errors before an emergency exit.

## Handle and DID doc resolution
* `APPVIEWLITE_DNS_SERVER`: DNS server for TXT resolutions. Defaults to `1.1.1.1`
* `APPVIEWLITE_USE_DNS_OVER_HTTPS`: Whether the DNS server supports DNS Over HTTPS. Defaults to `1`.
* `APPVIEWLITE_HANDLE_TO_DID_MAX_STALE_HOURS`: For how many hours we cache handle resolutions. Defaults to 10 days.
* `APPVIEWLITE_DID_DOC_MAX_STALE_HOURS`: For how many hours we cache DID docs. If the PLC directory is up to date (by this amount), we cache indefinitely. Defaults to 2 days.
* `APPVIEWLITE_PLC_DIRECTORY`: Alternate PLC directory prefix. Defaults to `https://plc.directory`
* `APPVIEWLITE_DID_DOC_OVERRIDES`: Path to an optional text file, where each line is `did:plc:example pds.example handle.example`. Will be reloaded dynamically if it changes. The listed PDSes will be listened from directly, without relaying on the main firehose.

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
* `APPVIEWLITE_YOTSUBA_HOSTS`: retrieves threads from the specified imageboards, optionally specifying API and image hosts. Example: `boards.4chan.org/i.4cdn.org/a.4cdn.org`. Defaults to none.

### Nostr
* `APPVIEWLITE_LISTEN_NOSTR_RELAYS`: Listens to the specified Nostr relays. Example: `nos.lol,bostr.bitcointxoko.com`. Defaults to none.
* `APPVIEWLITE_NOSTR_IGNORE_REGEX`: Don't ingest posts matching the specified regex.

## Image proxying and caching
* `APPVIEWLITE_SERVE_IMAGES`: Enables image serving, instead of relying on an external CDN.
* `APPVIEWLITE_CDN`: Image CDN domain. Defaults to `cdn.bsky.app` (unless `APPVIEWLITE_SERVE_IMAGES` is set)
* `APPVIEWLITE_IMAGE_CACHE_DIRECTORY`: Where to cache the thumbnails. Defaults to `$APPVIEWLITE_DIRECTORY/image-cache`
* `APPVIEWLITE_CACHE_AVATARS`: Whether avatar thumbnails should be cached to disk. Defaults to `1`.
* `APPVIEWLITE_CACHE_FEED_THUMBS`: Whether feed image thumbnails and profile banners should be cached to disk. Defaults to `0`.

You can also add a domain to the `[nooutboundconnect]` section of `APPVIEWLITE_BLOCKLIST_PATH` (see above).