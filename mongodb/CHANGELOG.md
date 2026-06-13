# Changelog — MongoDB contrib

## Unreleased — production-readiness rewrite (breaking)

This is a **breaking, greenfield rewrite** of `mongodb/eventstore` plus a new `mongodb/outbox`
package. There is **no data migration and no backward compatibility**: the on-disk schema is new.

### Removed

- The exported `Strategy` interface, `WithStrategy`, the entire `strategy/` package, and the
  multi-collection strategy. Single-collection logic is now folded directly into `EventStore`.
- The multi-cursor `ReadAll` merge (and its `multiStreamIterator`).
- `StreamInfo.UnmarshalBSON` (replaced by typed decoding).

### Changed

- **Sequencing** no longer scans for `max(offset)`. Per-stream and global offsets are assigned by
  atomic in-transaction counters in dedicated `streams` and `counters` collections.
- **Global ordering is now strict**: `Event.GlobalPosition` is a dense, gap-free, commit-ordered
  `int64` from an in-transaction global counter. This serializes appends across all streams (the
  cost of strict global ordering).
- **Appends are fully transactional.** Event inserts now genuinely run inside the append
  transaction; optimistic-concurrency checks and the unique-index backstop prevent duplicate or
  out-of-order writes under concurrency.
- **Transaction hooks run in-transaction** on the session context, committing or rolling back
  atomically with the events.
- Transaction/session defaults are now snapshot read concern, majority write concern, and primary
  read preference.

### Added

- `EnsureIndexes(ctx)` (idempotent) and `WithAutoEnsureIndexes()` — the analog of Postgres
  `Schema()`. Creates the unique `uniq_stream_offset` and `uniq_global_offset` indexes.
- Collection/database name and session/transaction options: `WithDatabaseName`,
  `WithEventsCollectionName`, `WithStreamsCollectionName`, `WithCountersCollectionName`,
  `WithSessionOptions`, `WithTransactionOptions`.
- `TransactionHookFunc` functional adapter.
- New `mongodb/outbox` package: a transactional outbox with strict per-stream FIFO via per-stream
  leasing, claim → handle-outside-txn → ack, delete-on-ack, and at-least-once delivery.

### Requirements

- MongoDB must run as a **replica set** (or mongos); transactions require it. A single-node replica
  set is fine for tests.
