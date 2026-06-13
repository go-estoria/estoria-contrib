# MongoDB Outbox

A transactional [outbox](https://microservices.io/patterns/data/transactional-outbox.html) for the
MongoDB [event store](../eventstore), mirroring the API of the Postgres outbox but using
MongoDB-native mechanics.

## How it works

- **Producer.** `Outbox` implements the event store's `TransactionHook`. On each append it inserts
  one outbox document per event **inside the append transaction**, so outbox rows commit or roll
  back atomically with the events.
- **Consumer.** A polling loop claims work and processes it with **strict per-stream FIFO**:
  1. **Claim a stream** via an atomic `findOneAndUpdate` that grants a time-bounded *lease* on the
     stream's cursor (in the `outbox_streams` collection). The lease guarantees **at most one active
     worker per stream**, while different streams are processed in parallel.
  2. **Load the head item** — the pending item whose `stream_version` equals the stream cursor.
  3. **Run the handler OUTSIDE any transaction** (no session is held across user code).
  4. **Acknowledge:** on success the item is deleted (delete-on-ack) and the cursor advances; on
     failure the retry count/error are recorded, and once `WithMaxRetries` is exceeded the item is
     marked `failed` and **the stream halts** (its later events are not delivered until an operator
     intervenes). The lease is released either way.

## Delivery contract

**At-least-once.** A crash (or a lease expiry while a handler is still running) re-delivers the item
after the lease window. **Handlers must be idempotent** — the same contract as the Postgres outbox.

## Usage

```go
db := client.Database("myapp")
outboxColl := db.Collection("outbox")
streamColl := db.Collection("outbox_streams")

ob, _ := outbox.New(outboxColl, streamColl, func(ctx context.Context, item *outbox.Item) error {
    return publish(ctx, item) // your delivery logic; must be idempotent
})

// Register the producer with the event store and create indexes once.
store, _ := eventstore.New(client,
    eventstore.WithDatabaseName("myapp"),
    eventstore.WithTransactionHook(ob),
)
_ = store.EnsureIndexes(ctx)
_ = ob.EnsureIndexes(ctx)

// Run the consumer (typically in its own goroutine / process).
go ob.Run(ctx)
```

Multiple processes may each run their own `Outbox` over the same collections; per-stream leases keep
delivery FIFO per stream while allowing cross-stream parallelism. A single process enforces one
active `Run` at a time.

## Options

- `WithPollInterval(d)` — how often `Run` polls (default 1s).
- `WithMaxRetries(n)` — retries before an item is permanently failed and its stream halts
  (default 10; 0 = unlimited).
- `WithLeaseDuration(d)` — per-stream lease duration (default 30s). A handler slower than the lease
  risks duplicate delivery.
- `WithLogger(l)` — logger.

## Cleanup

Delete-on-ack keeps the working set small with no cron. Failed items are retained for operator
inspection. A TTL-based retention option for failed items is a possible future addition.
