package outbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/go-estoria/estoria"
	pgeventstore "github.com/go-estoria/estoria-contrib/postgres/eventstore"
	"github.com/go-estoria/estoria/eventstore"
	"github.com/lib/pq"
)

// ItemHandler is a function that processes a single outbox item.
//
// Handlers must be idempotent: due to the at-least-once delivery guarantee,
// a handler may be called more than once for the same item if a failure occurs
// after the handler succeeds but before the transaction commits.
//
// Handlers must be safe for concurrent use if ProcessNext or Run is called
// from multiple goroutines.
//
// Handlers are called while holding a Postgres row-level lock on the outbox item.
// To avoid connection pool exhaustion, handlers should complete within a bounded
// time and should not perform unbounded I/O without a timeout.
type ItemHandler func(ctx context.Context, item *Item) error

// Outbox inserts events into an outbox table within the same database transaction as
// the event append, and processes them asynchronously via a polling loop.
type Outbox struct {
	db           *sql.DB
	tableName    string
	handler      ItemHandler
	pollInterval time.Duration
	maxRetries   int
	log          estoria.Logger
	running      atomic.Bool
}

var _ pgeventstore.TransactionHook = (*Outbox)(nil)

// New creates a new Outbox using the provided database connection and item handler.
func New(db *sql.DB, handler ItemHandler, opts ...Option) (*Outbox, error) {
	if db == nil {
		return nil, fmt.Errorf("database is required")
	}

	if handler == nil {
		return nil, fmt.Errorf("handler is required")
	}

	o := &Outbox{
		db:           db,
		tableName:    "outbox",
		handler:      handler,
		pollInterval: 1 * time.Second,
		maxRetries:   10,
		log:          estoria.GetLogger().WithGroup("outbox"),
	}

	for _, opt := range opts {
		if err := opt(o); err != nil {
			return nil, fmt.Errorf("applying option: %w", err)
		}
	}

	return o, nil
}

// Schema returns the SQL statements required to create the outbox table and its indexes.
//
// Note: processed and failed items are retained in the table indefinitely. In high-throughput
// systems, periodic cleanup of old items is recommended:
//
//	DELETE FROM outbox WHERE processed_at < now() - interval '7 days';
//	DELETE FROM outbox WHERE failed_at < now() - interval '30 days';
func (o *Outbox) Schema() string {
	quotedTable := pq.QuoteIdentifier(o.tableName)
	quotedIndex := pq.QuoteIdentifier("idx_" + o.tableName + "_unprocessed")
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    id              bigserial    PRIMARY KEY,
    event_id        uuid         NOT NULL,
    event_type      varchar(255) NOT NULL,
    stream_id       uuid         NOT NULL,
    stream_type     varchar(255) NOT NULL,
    stream_version  bigint       NOT NULL,
    timestamp       timestamptz  NOT NULL,
    data            jsonb,
    metadata        jsonb,
    created_at      timestamptz  NOT NULL DEFAULT now(),
    processed_at    timestamptz,
    retry_count     integer      NOT NULL DEFAULT 0,
    last_error      text,
    failed_at       timestamptz
);

CREATE INDEX IF NOT EXISTS %s
    ON %s (id)
    WHERE processed_at IS NULL AND failed_at IS NULL;
`, quotedTable, quotedIndex, quotedTable)
}

// HandleEvents implements pgeventstore.TransactionHook. It inserts one outbox row per event
// into the outbox table using the provided transaction. If any insert fails the error is
// returned and the caller's transaction will be rolled back.
func (o *Outbox) HandleEvents(ctx context.Context, tx *sql.Tx, events []*eventstore.Event) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (event_id, event_type, stream_id, stream_type, stream_version, timestamp, data, metadata)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		pq.QuoteIdentifier(o.tableName),
	)

	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return fmt.Errorf("preparing statement: %w", err)
	}
	defer func() {
		if closeErr := stmt.Close(); closeErr != nil {
			o.log.Error("closing statement", "error", closeErr)
		}
	}()

	for _, event := range events {
		var metadataArg any
		if event.Metadata != nil {
			// json.Marshal cannot fail for map[string]string — all keys and values are valid JSON strings.
			metadataArg, _ = json.Marshal(event.Metadata)
		}
		if _, err := stmt.ExecContext(ctx,
			event.ID.UUID,
			event.ID.Type,
			event.StreamID.UUID,
			event.StreamID.Type,
			event.StreamVersion,
			event.Timestamp,
			event.Data,
			metadataArg,
		); err != nil {
			return fmt.Errorf("inserting outbox item for event %s: %w", event.ID.String(), err)
		}
	}

	return nil
}
