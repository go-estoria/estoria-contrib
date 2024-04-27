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
	db    *sql.DB
	table string
}

func New(db *sql.DB, table string) *Outbox {
	return &Outbox{
		db:    db,
		table: table,
	}
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

	stmt, err := tx.Prepare(fmt.Sprintf("INSERT INTO %s (stream_id, timestamp, event) VALUES ($1, $2, $3)", o.table))
	if err != nil {
		return fmt.Errorf("preparing statement: %w", err)
	}
	defer stmt.Close()

	for _, event := range events {
		_, err = stmt.Exec(event.StreamID(), event.Timestamp(), event.Data())
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
