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

// HandleEvents inserts emitted events into the outbox table using the given transactional context.
func (o *Outbox) HandleEvents(tx *sql.Tx, events []*eventstore.Event) error {
	o.log.Debug("inserting events into outbox", "events", len(events))

	stmt, err := tx.Prepare(fmt.Sprintf(`
		INSERT INTO "%s" (
			stream_id,
			stream_type,
			event_id,
			event_type, 
			timestamp,
			stream_offset,
			data,
			added_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		o.table))
	if err != nil {
		return fmt.Errorf("preparing statement: %w", err)
	}

	defer func() { _ = stmt.Close() }()

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

func (o *Outbox) Schema() string {
	return fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS "%s" (
			stream_id     text        NOT NULL,
			stream_type   text        NOT NULL,
			event_id      text        NOT NULL,
			event_type    text        NOT NULL,
			timestamp     timestamptz NOT NULL,
			stream_offset bigint      NOT NULL,
			data          jsonb       NOT NULL,

			added_at	  timestamptz NOT NULL DEFAULT timezone('utc' now()),
			handled_at	  timestamptz NULL
		);

		PRIMARY KEY (event_id, event_type);

		CREATE INDEX IF NOT EXISTS "%s_stream" ON "%s" (stream_type, stream_id);
		CREATE INDEX IF NOT EXISTS "%s_handled_at" ON "%s" (stream_id, stream_type, handled_at);
	`, o.table, o.table, o.table, o.table, o.table)
}
