package outbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/go-estoria/estoria/typeid"
	"github.com/lib/pq"
)

// ErrNoItems is returned by ProcessNext when there are no unprocessed outbox items.
var ErrNoItems = errors.New("no unprocessed outbox items")

// ProcessNext claims and processes the next unprocessed outbox item. It uses
// SELECT FOR UPDATE SKIP LOCKED so that concurrent processors never handle the
// same item. If there are no unprocessed items, ErrNoItems is returned. If the
// handler fails the transaction is rolled back and the item remains available
// for a subsequent call.
func (o *Outbox) ProcessNext(ctx context.Context) (retErr error) {
	tx, err := o.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}

	defer func() {
		if retErr != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
				o.log.Error("rolling back transaction", "error", rollbackErr)
			}
		}
	}()

	query := fmt.Sprintf(
		`SELECT id, event_id, event_type, stream_id, stream_type, stream_version, timestamp, data, created_at
		FROM %s
		WHERE processed_at IS NULL
		ORDER BY id ASC
		LIMIT 1
		FOR UPDATE SKIP LOCKED`,
		pq.QuoteIdentifier(o.tableName),
	)

	var item Item
	var eventUUID, streamUUID uuid.UUID
	var eventType, streamType string

	row := tx.QueryRowContext(ctx, query)
	if err := row.Scan(
		&item.ID,
		&eventUUID,
		&eventType,
		&streamUUID,
		&streamType,
		&item.StreamVersion,
		&item.Timestamp,
		&item.Data,
		&item.CreatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			_ = tx.Rollback()
			return ErrNoItems
		}
		return fmt.Errorf("scanning outbox item: %w", err)
	}

	item.EventID = typeid.New(eventType, eventUUID)
	item.StreamID = typeid.New(streamType, streamUUID)

	if err := o.handler(ctx, &item); err != nil {
		return fmt.Errorf("handling outbox item %d: %w", item.ID, err)
	}

	updateQuery := fmt.Sprintf(
		`UPDATE %s SET processed_at = now() WHERE id = $1`,
		pq.QuoteIdentifier(o.tableName),
	)

	if _, err := tx.ExecContext(ctx, updateQuery, item.ID); err != nil {
		return fmt.Errorf("marking outbox item %d as processed: %w", item.ID, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	return nil
}

// Run starts the outbox polling loop. On each tick it drains all available items by
// calling ProcessNext repeatedly until ErrNoItems is returned. If the handler returns
// an error, the failing item remains unprocessed and the processor waits for the next
// tick before retrying. The loop runs until the context is canceled.
func (o *Outbox) Run(ctx context.Context) error {
	o.log.Info("outbox processor starting", "poll_interval", o.pollInterval)

	ticker := time.NewTicker(o.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			o.log.Info("outbox processor stopped")
			return nil

		case <-ticker.C:
			for {
				if err := o.ProcessNext(ctx); err != nil {
					if errors.Is(err, ErrNoItems) {
						break
					}
					if ctx.Err() != nil {
						return nil
					}
					o.log.Error("processing outbox item", "error", err)
					break
				}
			}
		}
	}
}
