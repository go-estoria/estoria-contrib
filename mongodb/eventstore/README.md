# MongoDB

Event store implementation for [estoria](https://github.com/go-estoria/estoria) using
[MongoDB](https://www.mongodb.com).

## Requirements

- **MongoDB must run as a replica set (or mongos).** The append path uses multi-document
  transactions, which require a replica set. A single-node replica set is sufficient for local
  development and testing.
- The driver is [`go.mongodb.org/mongo-driver/v2`](https://pkg.go.dev/go.mongodb.org/mongo-driver/v2).

## Data model

The store uses three collections in a single database (default `estoria`):

| Collection | Role | `_id` |
|---|---|---|
| `events` | Source of truth — one document per event. | opaque `ObjectID` |
| `streams` | Per-stream metadata cache (last offset, last global offset). | compound natural key `{t, s}` |
| `counters` | A single document holding the global sequence. | `"global_offset"` |

The `streams` and `counters` collections are derived caches: both are reconstructable from `events`
(though no rebuild helper ships yet).

### Sequencing and ordering

- **Per-stream offsets** are reserved with an atomic `findOneAndUpdate` on the `streams` document —
  no scan for `max(offset)`.
- **Global offsets** (`Event.GlobalPosition`) come from `$inc`-ing the single `counters` document
  inside the append transaction. This makes the global offset **dense, gap-free, and
  commit-ordered**.
- The shared global counter **serializes all appends across all streams**. This is the deliberate
  cost of strict global ordering. Under high cross-stream write concurrency, expect transaction
  retries on the counter document; sharded/batched counters are possible future mitigations.

## Setup

Create the required indexes once at deploy time (the analog of the Postgres `Schema()` step):

```go
store, err := eventstore.New(client)
if err != nil { /* ... */ }

if err := store.EnsureIndexes(ctx); err != nil { /* ... */ }
```

`EnsureIndexes` is idempotent. Alternatively pass `eventstore.WithAutoEnsureIndexes()` to `New` to
have it called automatically (off by default, for parity with the explicit Postgres flow).

## Usage

```go
client, _ := mongo.Connect(options.Client().ApplyURI(uri).SetReplicaSet("rs0"))

store, _ := eventstore.New(client,
    eventstore.WithDatabaseName("myapp"),         // optional
    eventstore.WithTransactionHook(outboxHook),   // optional; runs in the append transaction
)
_ = store.EnsureIndexes(ctx)

streamID := typeid.NewV4("user")
_ = store.AppendStream(ctx, streamID, events, eventstore.AppendStreamOptions{})
```

### Options

- `WithDatabaseName`, `WithEventsCollectionName`, `WithStreamsCollectionName`,
  `WithCountersCollectionName` — override collection/database names.
- `WithDocumentMarshaler` — customize event document encoding.
- `WithTransactionHook` — run a hook inside the append transaction (the basis for the
  [outbox](../outbox)).
- `WithSessionOptions`, `WithTransactionOptions` — override session/transaction concerns. The
  defaults are snapshot read concern, majority write concern, and primary read preference.
- `WithAutoEnsureIndexes` — call `EnsureIndexes` from `New`.

## Concurrency semantics

- Concurrent appends to the **same stream** conflict on the `streams` document. The loser is retried
  by the driver, re-reads the bumped version, and fails its optimistic-concurrency check with a
  `StreamVersionMismatchError`. The unique `{stream_type, stream_id, offset}` index is the backstop.
- Concurrent appends to **different streams** all succeed; their global offsets are unique, dense,
  and gap-free.
- A retried/duplicated append is rejected by the unique index (translated to
  `StreamVersionMismatchError`), never silently duplicated.

## Breaking changes

This package was rewritten for production readiness. The previous pluggable `Strategy` abstraction
(`Strategy`, `WithStrategy`, the `strategy/` package, and the multi-collection strategy) has been
**removed**, and the on-disk schema is new and incompatible with the previous layout. See
[`CHANGELOG.md`](../CHANGELOG.md).
