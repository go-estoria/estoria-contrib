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

The polling loop calls `ProcessNext` on each tick. `ProcessNext` opens its own transaction and selects the oldest unprocessed row using `SELECT FOR UPDATE SKIP LOCKED`, which allows multiple concurrent processors to run without contention. The configured handler is called with the locked row. If the handler returns without error, the row is marked `processed_at = now()` and the transaction is committed. If the handler returns an error, the transaction is rolled back and the row remains available for the next poll cycle.
