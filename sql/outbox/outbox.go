package outbox

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/go-estoria/estoria"
	"github.com/go-estoria/estoria/eventstore"
)

type Outbox struct {
	db              *sql.DB
	table           string
	includeFullData bool
	log             estoria.Logger
}

func New(db *sql.DB, table string, opts ...OutboxOption) (*Outbox, error) {
	outbox := &Outbox{
		db:              db,
		table:           table,
		includeFullData: false,
		log:             estoria.DefaultLogger().WithGroup("outbox"),
	}

	for _, opt := range opts {
		if err := opt(outbox); err != nil {
			return nil, fmt.Errorf("applying option: %w", err)
		}
	}

	return outbox, nil
}

func (o *Outbox) HandleEvents(tx *sql.Tx, events []*eventstore.Event) error {
	o.log.Debug("inserting events into outbox", "tx", "inherited", "events", len(events))

	documents := make([]any, len(events))
	for i, event := range events {
		documents[i] = &outboxRow{
			StreamID:  event.StreamID.String(),
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
		var data []byte
		if o.includeFullData {
			data = event.Data
		}

		if _, err := stmt.Exec(event.Timestamp, event.StreamID.String(), event.ID.String(), data); err != nil {
			return fmt.Errorf("executing statement: %w", err)
		}
	}

	return nil
}

type outboxRow struct {
	StreamID  string
	Timestamp time.Time
	Event     *eventstore.Event
}
