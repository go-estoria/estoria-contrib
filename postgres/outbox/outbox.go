package outbox

import (
	"database/sql"
	"fmt"

	"github.com/go-estoria/estoria"
	"github.com/go-estoria/estoria/eventstore"
)

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

func (o *Outbox) HandleEvents(tx *sql.Tx, events []*eventstore.Event) error {
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
