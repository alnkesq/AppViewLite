# AppViewLite

AppViewLite is an ATProto (Bluesky) appview focused on low resource consumption.

![Screenshot of the bsky.app profile on AppViewLite](https://raw.githubusercontent.com/alnkesq/AppViewLite/refs/heads/main/images/screenshot-bsky.jpg)

It includes:
* A firehose listener and indexer (`AppViewLite`)
* A simple web UI for viewing the indexed data (`AppViewLite.Web`)

Indexing the firehose (posts, likes, reposts, follows, blocks) takes about 2.2 GB of disk space per day. By contrast, the raw data from the firehose (without inverse indexes) is reported to be around 200 GB per day.

If you try to access a post or profile that hasn't been indexed (because it was posted while the indexer wasn't running), it will be fetched from the PDS.

This AppView runs independently of the main bsky.app appview, except for CDN image delivery and handle resolution. Other than that, it only needs a relay.

## Implementation status

- [X] Profile pages (posts, replies, media, following, followers)
- [X] Reverse relationships (post likers/reposts/quotes)
- [X] Custom feeds
- [X] Handle deletions
- [X] Full text search (including date and author filtering)
- [ ] Login support (it's probably easier to reuse the TypeScript from the main appview and add a backend API adapter)
- [ ] Pagination
- [ ] Honor blocks and labels
- [ ] Use user-friendly handles instead of DIDs for URLs
- [ ] CIDs in likes and reposts are not verified.
- [ ] Backfill historical data

## Storage mechanism
Each "table" is a set of memory-mapped columnar storage files that associates one key, to one or many values.
Both the keys and the values within a key are ordered to enable fast binary search lookups.
All the slices of a table are periodically compacted into larger slices.

### Identifiers
Accounts are rekeyed using 32-bit integers. RKeys are converted back into their underlying 64-bit values in order to save space.

### Post text
Post data is compressed by turning it into GPT/Tiktoken tokens, then encoding the 18-bit tokens using a variable-length bit representation, and then serializing everything into a Protobuf message, which is then Brotli-compressed (this was the most compact representation I could find after some experimentation).