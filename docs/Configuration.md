# Configuration

You can set the following environment variables:

* `APPVIEWLITE_DIRECTORY`: Where to store the data. Defaults to `~/BskyAppViewLiteData`
* `APPVIEWLITE_PLC_DIRECTORY_BUNDLE`: Path to an optional parquet file for quick bootstraping of the PLC directory data.
* `APPVIEWLITE_WIKIDATA_VERIFICATION`: Path to an optional parquet file with the official websites of the entities on Wikidata.
* `APPVIEWLITE_FIREHOSES`: A comma-separated list of ATProto or JetStream firehoses to listen to. Defaults to `jet:jetstream.atproto.tools`, alternatively you can use `bsky.network`. Only use trusted firehoses. For custom PDSes, use instead `APPVIEWLITE_DID_DOC_OVERRIDES` (see below).
* `APPVIEWLITE_LISTEN_TO_FIREHOSE`: Whether to listen to the firehose. Defaults to `1`.
* `APPVIEWLITE_LISTEN_TO_PLC_DIRECTORY`: Periodically fetches updates to the PLC directory. Defaults to `1`.
* `APPVIEWLITE_ALLOW_PUBLIC_READONLY_FAKE_LOGIN`: For debugging only, will allow you to "log in" to any account (but obviously you won't be able to perform PDS writes). Defaults to `0`.
* `APPVIEWLITE_PRINT_LONG_READ_LOCKS_MS`, `APPVIEWLITE_PRINT_LONG_WRITE_LOCKS_MS`, `APPVIEWLITE_PRINT_LONG_UPGRADEABLE_LOCKS_MS`: Print a stacktrace when we spend more than this amount of time while holding a lock.
* `APPVIEWLITE_READONLY`: Forbids database writes (useful for debugging data issues without the firehose interfering).

## Handle and DID doc resolution
* `APPVIEWLITE_DNS_SERVER`: DNS server for TXT resolutions. Defaults to `1.1.1.1`
* `APPVIEWLITE_USE_DNS_OVER_HTTPS`: Whether the DNS server supports DNS Over HTTPS. Defaults to `1`.
* `APPVIEWLITE_HANDLE_TO_DID_MAX_STALE_HOURS`: For how many hours we cache handle resolutions. Defaults to 10 days.
* `APPVIEWLITE_DID_DOC_MAX_STALE_HOURS`: For how many hours we cache DID docs. If the PLC directory is up to date (by this amount), we cache indefinitely. Defaults to 2 days.
* `APPVIEWLITE_PLC_DIRECTORY`: Alternate PLC directory prefix. Defaults to `https://plc.directory`
* `APPVIEWLITE_DID_DOC_OVERRIDES`: Path to an optional text file, where each line is `did:plc:example pds.example handle.example`. Will be reloaded dynamically if it changes. The listed PDSes will be listened from directly, without relaying on the main firehose.

## Administrative rules
* `APPVIEWLITE_BLOCKLIST_PATH`: path to an `.ini` file whose sections can be `[noinjest]`, `[nodisplay]`, `[nooutboundconnect]` (or combinations, like `[noinjest,nodisplay]`) to block specific DIDs or domains (and all their subdomains) of all types (PDSes, Mastodon instances, handles, Mastodon external media...)

   * `[noinjest]` ignores all the posts coming from the various firehoses for the specified DIDs or domains. By default, it includes various cross-protocol mirrors (AppViewLite is already multi-protocol)
   * `[nodisplay]` prevents post and profile data for the specified DIDs or domains from being displayed.
   * `[nooutboundconnect]` prevents outbound HTTP traffic to the specified DIDs or domains. Image thumbnails and profile pictures won't be available.
   * `[blockall]` is a shorthand for `[noinjest,nodisplay,nooutboundconnect]`
   * `[allowall]` allows you to override the default rules for the specified domains.

You can also use regular expressions (e.g. `regex:^example$`), but for best performance you should minimize the number of such rules (you can use the `|` regex operator to consolidate them).

## Additional protocols
By default, only ATProto/Bluesky is enabled.<br>
You can however enable additional protocols:

### ActivityPub (Fediverse)
* `APPVIEWLITE_LISTEN_ACTIVITYPUB_RELAYS`: listens to the specified ActivityPub relays. Example: `fedi.buzz`. Defaults to none.

### Yotsuba (Imageboards)
* `APPVIEWLITE_YOTSUBA_HOSTS`: retrieves threads from the specified imageboards, optionally specifying API and image hosts. Example: `boards.4chan.org/i.4cdn.org/a.4cdn.org`. Defaults to none.

### Nostr
* `APPVIEWLITE_LISTEN_NOSTR_RELAYS`: listens to the specified Nostr relays. Example: `nos.lol,bostr.bitcointxoko.com`. Defaults to none.

## Image proxying and caching
* `APPVIEWLITE_SERVE_IMAGES`: Enables image serving, instead of relying on an external CDN.
* `APPVIEWLITE_CDN`: Image CDN domain. Defaults to `cdn.bsky.app` (unless `APPVIEWLITE_SERVE_IMAGES` is set)
* `APPVIEWLITE_IMAGE_CACHE_DIRECTORY`: Where to cache the thumbnails. Defaults to `$APPVIEWLITE_DIRECTORY/image-cache`
* `APPVIEWLITE_CACHE_AVATARS`: Whether avatar thumbnails should be cached to disk. Defaults to `1`.
* `APPVIEWLITE_CACHE_FEED_THUMBS`: Whether feed image thumbnails and profile banners should be cached to disk. Defaults to `0`.

You can also add a domain to the `[nooutboundconnect]` section of `APPVIEWLITE_BLOCKLIST_PATH` (see above).