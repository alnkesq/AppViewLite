# AppViewLite

AppViewLite is an ATProto (Bluesky) appview focused on low resource consumption, able to run independently of the main appview APIs.

It includes:
* A firehose listener and indexer (`AppViewLite`)
* A simple web UI for viewing the indexed data (`AppViewLite.Web`)
* An XRPC interface that allows you to reuse the official TypeScript [client](https://github.com/bluesky-social/social-app/) implementation

<img src="https://raw.githubusercontent.com/alnkesq/AppViewLite/refs/heads/main/images/appviewlite.png" alt="Screenshot of the bsky.app profile on AppViewLite" width="600">

Indexing the firehose (posts, likes, reposts, follows, blocks) takes about 2.2 GB of disk space per day. By contrast, the raw data from the firehose (without inverse indexes) is reported to be around 200 GB per day.

If you try to access a post or profile that hasn't been indexed (because it was posted while the indexer wasn't running), it will be fetched from the PDS.

This AppView runs independently of the main bsky.app appview, except for CDN image delivery and handle resolution. Other than that, it only needs a relay.

**Tip**: You can browse to `http://localhost:PORT/https://bsky.app/...` to easily convert a bsky.app URL into an AppViewLite one.

## Implementation status

- [X] Profile pages (posts, replies, media, following, followers)
- [X] Reverse relationships (post likers/reposts/quotes)
- [X] Compose posts and like/repost
- [X] Full text search (including date and author filtering)
- [X] Timeline
- [X] Custom feeds
- [X] Notifications
- [ ] [Honor blocks and labels](/../../issues/7)
- [ ] [Show handles instead of DIDs](/../../issues/6)
- [ ] [CID verification](/../../issues/5)
- [ ] [Backfill historical data](/../../issues/8)

## Storage mechanism
Each "table" is a set of memory-mapped columnar storage files that associates one key, to one or many values.
Both the keys and the values within a key are ordered to enable fast binary search lookups.
All the slices of a table are periodically compacted into larger slices.

### Identifiers
Accounts are rekeyed using 32-bit integers. RKeys are converted back into their underlying 64-bit values in order to save space.

### Post text
Post data is compressed by turning it into GPT/Tiktoken tokens, then encoding the 18-bit tokens using a variable-length bit representation, and then serializing everything into a Protobuf message (along with other metadata), which is then Brotli-compressed (this was the most compact representation I could find after some experimentation).
