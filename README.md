# AppViewLite

AppViewLite is an ATProto (Bluesky) appview focused on low resource consumption, able to run independently of the main appview APIs.

It includes:
* A firehose listener and indexer (`AppViewLite`)
* A simple web UI for viewing the indexed data (`AppViewLite.Web`)
* An XRPC interface that allows you to reuse the official TypeScript [client](https://github.com/bluesky-social/social-app/) implementation

<img src="https://raw.githubusercontent.com/alnkesq/AppViewLite/refs/heads/main/images/appviewlite.png" alt="Screenshot of the bsky.app profile on AppViewLite" width="600">

Indexing the firehose (posts, likes, reposts, follows, blocks) takes about 2.2 GB of disk space per day. By contrast, the raw data from the firehose (without inverse indexes) is reported to be around 200 GB per day.
You can optionally [prune](docs/Configuration.md#Pruning) old content that doesn't involve the social graph neighborhood of the users that use your AppViewLite instance.

**Tip**: You can browse to `http://localhost:PORT/profile/...` to easily convert a bsky.app URL into an AppViewLite one, or you can paste a profile URL into the search bar.

## Platform independence

One of the goals is to be as independent as possible from the official Bluesky PBC infrastructure.

This AppView runs independently of the main `bsky.app` APIs.

* **Relays**: you can choose a relay (`bsky.network` or JetStream), and optionally specify individual extra PDSes to listen to, making them uncensorable.
* **PDSes**: AppViewLite connects directly to PDSes to fetch any missing records.
* **PLC directory**: fetched incrementally, can be bootstrapped from a Parquet bundle. You can override individual PLC entries using a configuration file.
* **Image serving**: you can choose whether to proxy/cache images yourself, or to reuse `cdn.bsky.app`

## Implementation status
| Feature                     | AppViewLite (read) | AppViewLite (edit)  | bsky.app 
| --------                    | -------            | -------             | -------  
| Posts                       |  ✅                |✅                  |✅
| Likes, bookmarks, reposts                       |  ✅                |✅                  |⚠️ No bookmarks
| Profile pages               |  ✅                |                  |⚠️ No likes list
| Follows                     |  ✅                |✅                  |⚠️ No private follows
| Search                      |  ✅                |✅                  |⚠️ No media search
| Media grid view             |  ✅                |                   |⛔
| Video                       |  ✅ + download support               |⛔               |✅
| Notifications               |  ✅                |                  |✅
| Feeds                       |  ✅                |                  |✅
| Built-in feed: Recent       |  ✅                |                  |✅
| Built-in feed: Balanced     |  ✅                |                  |⛔
| Live post stat updates      |  ✅                |                  |⛔
| Keyboard navigation (JK)    |  ✅                |                  |⛔
| Recently viewed post history|  ✅                |                  |⛔
| Post interaction settings   |  ✅ Blurred posts  |⛔                  |✅ Nuclear blocks
| Blocks                      |  ✅ Blurred posts  |✅                  |✅ Nuclear blocks
| Labels                      |  ✅                |                    |✅
| Lists                       |  ✅                |⛔                  |✅
| Mutes                       |  ✅                |⚠️ No expiration    |⚠️ No user-specific mute words<br>⚠️ No mute by post type
| Protocol: ATProto           |  ✅                |✅                |✅
| Protocol: Fediverse/Mastodon |  ✅                |⛔                |⛔
| Protocol: RSS                |  ✅                |⛔                |⛔
| Protocol: Nostr              |  ✅                |⛔                |⛔
| Protocol: Imageboards        |  ✅                |⛔                |⛔
| Protocol: Tumblr             |  ✅                |⛔                |⛔
| Appearance settings          |  ✅ Custom accent colors                |                   |✅
| Chat                         |  ⛔                | ⛔               |✅
| Data export                  |  ✅                |                  |⚠️ No images, no private data
| Self-hosting                 |  ✅ Single-process<br>✅ Low-resource focused               |                  |⚠️ Complex, resource intensive



## Building and running
- Install [.NET 9](https://dotnet.microsoft.com/en-us/download)
- `cd src/AppViewLite.Web`
- `dotnet run -c Release -- --allow-new-database`

Optionally, you can set [various configuration settings](https://github.com/alnkesq/AppViewLite/blob/main/docs/Configuration.md), including  `APPVIEWLITE_DIRECTORY` to specify where the data should be stored.

## Storage mechanism
Each "table" is a set of memory-mapped columnar storage files that associates one key, to one or many values.
Both the keys and the values within a key are ordered to enable fast binary search lookups.
All the slices of a table are periodically compacted into larger slices.

A primary / readonly replica mechanism is used (within the same process) with read/write lock semantics to allow for effectively lock-less reads for most HTTP requests.

### Identifiers
Accounts are rekeyed using 32-bit integers. RKeys are converted back into their underlying 64-bit values in order to save space.

### Post text
Post data is compressed by turning it into GPT/Tiktoken tokens, then encoding the 18-bit tokens using a variable-length bit representation, and then serializing everything into a Protobuf message (along with other metadata).
