package outbox

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/go-estoria/estoria"
	"github.com/go-estoria/estoria/eventstore"
)

// Transaction is an interface for executing queries within a transactional context.
// It abstracts *sql.Tx by exposing prepare, query, and exec methods, plus commit/rollback.
type Transaction interface {
	Prepare(query string) (*sql.Stmt, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	Commit() error
	Rollback() error
}

type Outbox struct {
	table string
	log   estoria.Logger
}

func New(table string) (*Outbox, error) {
	outbox := &Outbox{
		table: table,
		log:   estoria.GetLogger().WithGroup("outbox"),
	}

	return outbox, nil
}

// HandleEvents inserts emitted events into the outbox table using the given transactional context.
func (o *Outbox) HandleEvents(tx Transaction, events []*eventstore.Event) error {
	o.log.Debug("inserting events into outbox", "tx", "inherited", "events", len(events))

	statement := fmt.Sprintf(`
		INSERT INTO "%s" (stream_id, stream_type, event_id, event_type, timestamp, stream_offset, data)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		o.table)
	stmt, err := tx.Prepare(statement)
	if err != nil {
		return fmt.Errorf("preparing statement: %w", err)
	}
	defer stmt.Close()

	for _, event := range events {
		if _, err := stmt.Exec(
			event.StreamID.UUID().String(),
			event.StreamID.TypeName(),
			event.ID.UUID().String(),
			event.ID.TypeName(),
			event.Timestamp,
			event.StreamVersion,
			event.Data,
		); err != nil {
			return fmt.Errorf("executing statement: %w", err)
		}
	}

	return nil
}
