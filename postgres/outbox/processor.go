package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/go-estoria/estoria/typeid"
	"github.com/gofrs/uuid/v5"
	"github.com/jackc/pgx/v5"
)

// ErrNoItems is returned by ProcessNext when there are no unprocessed outbox items.
var ErrNoItems = errors.New("no unprocessed outbox items")

// ProcessNext claims and processes the next eligible outbox item.
//
// Selection enforces strict per-stream FIFO: a stream's next event is only eligible
// once every earlier event on that stream has been successfully processed. Concurrent
// processors may handle different streams in parallel via SELECT FOR UPDATE SKIP LOCKED.
// If there are no eligible items, ErrNoItems is returned.
//
// On handler failure, the item's retry count and last error are persisted within
// the same transaction (which is then committed), so the failure is recorded even
// though the item was not successfully processed. If the retry count reaches the
// configured maximum, the item is permanently marked as failed. Because a failed
// item is still considered an unprocessed predecessor, its stream halts at that
// point and no later events on that stream will be delivered until an operator
// resolves the failure.
//
// ProcessNext is safe to call concurrently; Postgres row-level locking ensures each
// item is delivered to at most one caller at a time, and the head-of-stream predicate
// preserves within-stream order across concurrent callers.
func (o *Outbox) ProcessNext(ctx context.Context) (retErr error) {
	tx, err := o.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}

	defer func() {
		if retErr != nil {
			if rollbackErr := tx.Rollback(ctx); rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
				o.log.Error("rolling back transaction", "error", rollbackErr)
			}
		}
	}()

	// Only the head of each stream (the lowest-id row with no earlier unprocessed
	// predecessor on the same stream) is eligible. A permanently-failed predecessor
	// still has processed_at IS NULL, so it blocks the stream until resolved.
	table := pgx.Identifier{o.tableName}.Sanitize()
	query := fmt.Sprintf(
		`SELECT id, event_id, event_type, stream_id, stream_type, stream_version, timestamp, data, metadata, created_at, retry_count, last_error
		FROM %[1]s o
		WHERE o.processed_at IS NULL
		  AND o.failed_at IS NULL
		  AND NOT EXISTS (
		    SELECT 1 FROM %[1]s e
		    WHERE e.stream_id = o.stream_id
		      AND e.id < o.id
		      AND e.processed_at IS NULL
		  )
		ORDER BY o.id ASC
		LIMIT 1
		FOR UPDATE SKIP LOCKED`,
		table,
	)

	var item Item
	var eventUUID, streamUUID uuid.UUID
	var eventType, streamType string
	var metadataJSON []byte

	row := tx.QueryRow(ctx, query)
	if err := row.Scan(
		&item.ID,
		&eventUUID,
		&eventType,
		&streamUUID,
		&streamType,
		&item.StreamVersion,
		&item.Timestamp,
		&item.Data,
		&metadataJSON,
		&item.CreatedAt,
		&item.RetryCount,
		&item.LastError,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNoItems
		}
		return fmt.Errorf("scanning outbox item: %w", err)
	}

	item.EventID = typeid.New(eventType, eventUUID)
	item.StreamID = typeid.New(streamType, streamUUID)

	if metadataJSON != nil {
		if err := json.Unmarshal(metadataJSON, &item.Metadata); err != nil {
			return fmt.Errorf("unmarshaling outbox item metadata: %w", err)
		}
	}

	if handlerErr := o.handler(ctx, &item); handlerErr != nil {
		if err := o.recordHandlerFailure(ctx, tx, item, handlerErr); err != nil {
			return err
		}

		return fmt.Errorf("handling outbox item %d: %w", item.ID, handlerErr)
	}

	updateQuery := fmt.Sprintf(
		`UPDATE %s SET processed_at = now() WHERE id = $1`,
		pgx.Identifier{o.tableName}.Sanitize(),
	)

	if _, err := tx.Exec(ctx, updateQuery, item.ID); err != nil {
		return fmt.Errorf("marking outbox item %d as processed: %w", item.ID, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	return nil
}

// Run starts the outbox polling loop. On each tick it drains all available items by
// calling ProcessNext repeatedly until ErrNoItems is returned. If the handler returns
// an error, the failing item remains unprocessed and the processor waits for the next
// tick before retrying. The loop runs until the context is canceled.
//
// Run returns an error if it is called while another Run is already active on the same Outbox.
func (o *Outbox) Run(ctx context.Context) error {
	if !o.running.CompareAndSwap(false, true) {
		return errors.New("outbox processor is already running")
	}
	defer o.running.Store(false)

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

// recordHandlerFailure persists the outcome of a failed handler call: either a retry
// with an incremented count, or a permanent failure once the retry budget is spent.
// It commits, since the caller's transaction is otherwise rolled back with the error.
func (o *Outbox) recordHandlerFailure(ctx context.Context, tx pgx.Tx, item Item, handlerErr error) error {
	newRetryCount := item.RetryCount + 1
	errMsg := handlerErr.Error()

	if o.maxRetries > 0 && newRetryCount > o.maxRetries {
		// Permanently fail the item — it has exhausted its retry budget.
		failQuery := fmt.Sprintf(
			`UPDATE %s SET retry_count = $2, last_error = $3, failed_at = now() WHERE id = $1`,
			pgx.Identifier{o.tableName}.Sanitize(),
		)
		if _, execErr := tx.Exec(ctx, failQuery, item.ID, newRetryCount, errMsg); execErr != nil {
			o.log.Error("marking outbox item as failed", "error", execErr)
			return fmt.Errorf("handling outbox item %d: %w", item.ID, handlerErr)
		}

		o.log.Error("outbox item permanently failed",
			"item_id", item.ID,
			"retry_count", newRetryCount,
			"max_retries", o.maxRetries,
			"error", errMsg,
		)
	} else {
		// Increment the retry count and record the error so the next poll picks it up.
		retryQuery := fmt.Sprintf(
			`UPDATE %s SET retry_count = $2, last_error = $3 WHERE id = $1`,
			pgx.Identifier{o.tableName}.Sanitize(),
		)
		if _, execErr := tx.Exec(ctx, retryQuery, item.ID, newRetryCount, errMsg); execErr != nil {
			o.log.Error("updating retry count", "error", execErr)
			return fmt.Errorf("handling outbox item %d: %w", item.ID, handlerErr)
		}
	}

	// Commit the retry/fail update. The deferred rollback will see pgx.ErrTxClosed
	// (which is filtered) because the transaction is already done after this commit.
	if commitErr := tx.Commit(ctx); commitErr != nil {
		o.log.Error("committing retry update", "error", commitErr)
		return fmt.Errorf("handling outbox item %d: %w", item.ID, handlerErr)
	}

	return nil
}
