# MongoDB

Event store implementation for [estoria](https://github.com/go-estoria/estoria) using [MongoDB](https://www.mongodb.com).

MongoDB multi-document transactions require a replica set (a single-node replica set is sufficient).

## Storage Strategies

### Single Collection

All events for all Estoria event streams are stored in a single collection. Streams query this collection, filtering on stream ID.

### Multi Collection

Events are partitioned across collections within a database, with the collection for each stream chosen by a `CollectionSelector`: per stream type (the default), per stream ID, or custom. Useful when a single collection grows too large.

## The Streams Collection

Alongside the event collections, both strategies maintain a streams collection (default name `_streams`) holding one document per stream:

```json
{ "_id": "<type>_<uuid>", "stream_type": "<type>", "stream_id": "<uuid>", "last_offset": 7 }
```

plus a single global offset counter document:

```json
{ "_id": "_global", "last_offset": 42 }
```

Appends reserve stream versions and global positions by incrementing these counters inside the append transaction, so concurrent appends never allocate duplicate offsets, and stream existence is answered by a stream document's presence. The global counter serializes appends store-wide; that is the price of a single global ordering authority.

The leading underscore in `_streams` and `_global` is reserved by construction: typeid type names cannot begin with an underscore, so no stream or selector-derived collection can collide with them. The whole underscore namespace is reserved this way — the multi-collection strategy's event collection enumeration ignores every `_`-prefixed collection, so infrastructure collections (such as the [outbox](../outbox)'s) can share the database without being swept into global reads. A custom `CollectionSelector` that produces the streams collection's name or an underscore-prefixed name is rejected on writes.

## Indexes

The store defines two unique indexes on every event collection:

- `uniq_stream_offset` on `(stream_type, stream_id, offset)` — serves per-stream reads and
  backstops offset reservation: if a stored event already occupies a reserved offset, the
  append fails with a `StreamVersionMismatchError` instead of writing a duplicate.
- `uniq_global_offset` on `global_offset` — serves global reads in order.

MongoDB cannot build indexes inside transactions, so index creation is an explicit
initialization step (the analog of the SQL backends' `Schema()`):

```go
if err := store.EnsureIndexes(ctx); err != nil { ... }
```

`EnsureIndexes` is idempotent. With the multi-collection strategy it covers the event
collections that exist when it is called; a selector that creates collections on the fly
(such as `CollectionPerStreamID`) should instead use the strategy's
`WithAutoEnsureIndexes` option, which ensures each collection's indexes before its first
append, once per collection per process.

The counters make duplicate offsets impossible among the documents they account for, so
the unique indexes matter most as a guard against data they never accounted for — most
notably an un-backfilled legacy dataset, where the restarted counters silently rewrite
history; with the indexes in place those appends fail loudly instead.

## Upgrading From Derived Offsets

Versions of this store before the streams collection derived offsets from `max()` over existing event documents. Databases written by those versions must be backfilled once before use with the current version — without the counter documents, appends restart versions and global positions at 1.

For each event collection, populate stream documents and then the global counter:

```javascript
db.events.aggregate([
  { $group: {
      _id: { $concat: ["$stream_type", "_", "$stream_id"] },
      stream_type: { $first: "$stream_type" },
      stream_id: { $first: "$stream_id" },
      last_offset: { $max: "$offset" },
  } },
  { $merge: { into: "_streams" } },
]);

db.getCollection("_streams").insertOne({
  _id: "_global",
  last_offset: db.events.aggregate([
    { $group: { _id: null, max: { $max: "$global_offset" } } },
  ]).next().max,
});
```

Under a multi-collection strategy, run the first aggregation once per event collection, and set `_global` to the maximum `global_offset` across all of them.
