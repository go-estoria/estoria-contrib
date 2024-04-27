package outbox

import (
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/go-estoria/estoria"
	"go.jetpack.io/typeid"
)

type Outbox struct {
	db              *sql.DB
	table           string
	includeFullData bool
}

func New(db *sql.DB, table string, opts ...OutboxOption) (*Outbox, error) {
	outbox := &Outbox{
		db:              db,
		table:           table,
		includeFullData: false,
	}

	for _, opt := range opts {
		if err := opt(outbox); err != nil {
			return nil, fmt.Errorf("applying option: %w", err)
		}
	}

	return outbox, nil
}

func (o *Outbox) HandleEventsInTransaction(tx *sql.Tx, events []estoria.Event) error {
	slog.Debug("inserting events into outbox", "tx", "inherited", "events", len(events))

	documents := make([]any, len(events))
	for i, event := range events {
		documents[i] = &outboxRow{
			StreamID:  event.StreamID(),
			Timestamp: time.Now(),
			Event:     event,
		}
	}

	statement := fmt.Sprintf("INSERT INTO %s (timestamp, stream_id, event_id, event_data) VALUES ($1, $2, $3, $4)", o.table)
	stmt, err := tx.Prepare(statement)
	if err != nil {
		return fmt.Errorf("preparing statement: %w", err)
	}
	defer stmt.Close()

	for _, event := range events {
		var data any
		if o.includeFullData {
			data = event.Data()
		}

		_, err = stmt.Exec(event.Timestamp(), event.StreamID(), event.ID(), data)
		if err != nil {
			return fmt.Errorf("executing statement: %w", err)
		}
	}

	return nil
}

type outboxRow struct {
	StreamID  typeid.AnyID
	Timestamp time.Time
	Event     estoria.Event
}
