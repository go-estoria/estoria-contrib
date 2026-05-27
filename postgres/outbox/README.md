# Postgres Outbox

Transactional outbox implementation for [estoria](https://github.com/go-estoria/estoria) using [Postgres](https://www.postgresql.org).

## Overview

The outbox writes events into a dedicated outbox table within the same database transaction as the event append. A background polling loop then reads and processes those rows asynchronously, marking each one as processed after the handler succeeds. This guarantees that events are never lost between the write and the downstream handler, even if the process crashes between the two.

## Installation

```sh
go get github.com/go-estoria/estoria-contrib
```

## Usage

```go
import (
    "github.com/jackc/pgx/v5/pgxpool"

    "github.com/go-estoria/estoria-contrib/postgres/outbox"
    pgeventstore "github.com/go-estoria/estoria-contrib/postgres/eventstore"
)

// Define a handler that processes each outbox item.
handler := func(ctx context.Context, item *outbox.Item) error {
    // publish item to a message broker, call a downstream service, etc.
    return nil
}

// Create the outbox.
ob, err := outbox.New(pool, handler,
    outbox.WithTableName("outbox"),
    outbox.WithPollInterval(500*time.Millisecond),
)
if err != nil {
    log.Fatal(err)
}

// Create the outbox table (safe to call on every startup).
if _, err := pool.Exec(ctx, ob.Schema()); err != nil {
    log.Fatal(err)
}

// Create the event store, registering the outbox as a transaction hook.
store, err := pgeventstore.New(pool,
    pgeventstore.WithAppendTransactionHooks(ob),
)
if err != nil {
    log.Fatal(err)
}

// Process items one at a time (returns outbox.ErrNoItems when the queue is empty).
if err := ob.ProcessNext(ctx); err != nil && !errors.Is(err, outbox.ErrNoItems) {
    log.Println("outbox error:", err)
}

// Or run the polling loop in the background until the context is canceled.
go ob.Run(ctx)
```

## Configuration

| Option | Default | Description |
|---|---|---|
| `WithTableName(name string)` | `"outbox"` | Database table used to store outbox rows. Must be a valid SQL identifier. |
| `WithPollInterval(d time.Duration)` | `1s` | How often the polling loop checks for unprocessed items. Must be positive. |
| `WithLogger(logger estoria.Logger)` | estoria default logger | Logger used for internal diagnostic messages. |

## How It Works

When the event store appends events, each registered `TransactionHook` is called with the open transaction before it is committed. The outbox inserts one row per event into the outbox table inside that same transaction, so the event rows and outbox rows are always written atomically — either both land or neither does.

The polling loop calls `ProcessNext` on each tick. `ProcessNext` opens its own transaction and selects the next eligible row using `SELECT FOR UPDATE SKIP LOCKED`. The configured handler is called with the locked row. If the handler returns without error, the row is marked `processed_at = now()` and the transaction is committed. If the handler returns an error, the retry count and last error are persisted in the same transaction and the row remains available for the next poll cycle.

## Ordering and Concurrency

`ProcessNext` enforces **strict per-stream FIFO**: an outbox row is only eligible once every earlier row on the same stream has been successfully processed. Concurrent processors (multiple service instances, or multiple goroutines on the same instance) may handle different streams in parallel, but no two callers will ever deliver events for the same stream out of order.

This is implemented in the selection query: in addition to the row-level `FOR UPDATE SKIP LOCKED` claim, each candidate row must have no earlier row on its stream with `processed_at IS NULL`. The partial index `(stream_id, id) WHERE processed_at IS NULL` keeps that lookup cheap.

### Halt on permanent failure

When a row exceeds its configured `WithMaxRetries` budget, it is marked `failed_at = now()` (a "dead letter") and the handler is no longer invoked for it. Because a dead-lettered row still has `processed_at IS NULL`, it continues to act as an unprocessed predecessor and **halts its stream**: no later events on the same stream will be delivered until an operator resolves the failure (typically by setting `processed_at` manually after replaying the event downstream, or by setting `processed_at` and explicitly skipping it). Other streams are unaffected and continue to drain in parallel.

This is the safer default for event-sourced projections, where skipping a single event in a stream's history can leave downstream consumers in a permanently inconsistent state. Operators should monitor `failed_at IS NOT NULL` rows and alert on them.
