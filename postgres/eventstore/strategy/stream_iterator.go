package strategy

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/go-estoria/estoria"
	"go.jetpack.io/typeid"
)

type streamIterator struct {
	streamID typeid.AnyID
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
	var eventID string
	var eventType string
	if err := i.rows.Scan(&eventID, &eventType, &e.timestamp, &e.data); err != nil {
		return nil, fmt.Errorf("scanning row: %w", err)
	}

	eventTID, err := typeid.From(eventType, eventID)
	if err != nil {
		return nil, fmt.Errorf("parsing event type and ID into typeid: %w", err)
	}

	e.id = eventTID

	return &e, nil
}

type event struct {
	id        typeid.AnyID
	streamID  typeid.AnyID
	timestamp time.Time
	data      []byte
}

var _ estoria.EventStoreEvent = (*event)(nil)

func (e *event) ID() typeid.AnyID {
	return e.id
}

func (e *event) StreamID() typeid.AnyID {
	return e.streamID
}

func (e *event) Timestamp() time.Time {
	return e.timestamp
}

func (e *event) Data() []byte {
	return e.data
}
