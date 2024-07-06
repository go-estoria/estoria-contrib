package strategy

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/go-estoria/estoria"
	"github.com/go-estoria/estoria/typeid"
	"github.com/gofrs/uuid/v5"
)

type streamIterator struct {
	streamID typeid.UUID
	rows     *sql.Rows
}

func (i *streamIterator) Next(ctx context.Context) (estoria.EventStoreEvent, error) {
	if !i.rows.Next() {
		return nil, io.EOF
	}

	if err := i.rows.Err(); errors.Is(err, sql.ErrNoRows) {
		return nil, io.EOF
	} else if err != nil {
		return nil, fmt.Errorf("iterating rows: %w", err)
	}

	var e event
	var eventID uuid.UUID
	var eventType string
	if err := i.rows.Scan(&eventID, &eventType, &e.timestamp, &e.data); err != nil {
		return nil, fmt.Errorf("scanning row: %w", err)
	}

	e.id = typeid.FromUUID(eventType, eventID)

	return &e, nil
}

type event struct {
	id            typeid.UUID
	streamID      typeid.UUID
	streamVersion int64
	timestamp     time.Time
	data          []byte
}

var _ estoria.EventStoreEvent = (*event)(nil)

func (e *event) ID() typeid.UUID {
	return e.id
}

func (e *event) StreamID() typeid.UUID {
	return e.streamID
}

func (e *event) StreamVersion() int64 {
	return e.streamVersion
}

func (e *event) Timestamp() time.Time {
	return e.timestamp
}

func (e *event) Data() []byte {
	return e.data
}
