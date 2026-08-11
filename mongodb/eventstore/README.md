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

The leading underscore in `_streams` and `_global` is reserved by construction: typeid type names cannot begin with an underscore, so no stream or selector-derived collection can collide with them. A custom `CollectionSelector` must not produce the streams collection's name.

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
