package strategy

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"

	"github.com/go-estoria/estoria"
	"github.com/go-estoria/estoria/typeid"
	"github.com/gofrs/uuid/v5"
)

type streamIterator struct {
	streamID typeid.UUID
	rows     *sql.Rows
}

func (i *streamIterator) Next(ctx context.Context) (*estoria.EventStoreEvent, error) {
	if !i.rows.Next() {
		return nil, io.EOF
	}

	if err := i.rows.Err(); errors.Is(err, sql.ErrNoRows) {
		return nil, io.EOF
	} else if err != nil {
		return nil, fmt.Errorf("iterating rows: %w", err)
	}

	var e estoria.EventStoreEvent
	var eventID uuid.UUID
	var eventType string
	if err := i.rows.Scan(&eventID, &eventType, &e.Timestamp, &e.Data); err != nil {
		return nil, fmt.Errorf("scanning row: %w", err)
	}

	e.ID = typeid.FromUUID(eventType, eventID)

	return &e, nil
}
