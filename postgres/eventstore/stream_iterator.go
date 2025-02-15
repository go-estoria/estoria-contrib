package eventstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
	"github.com/gofrs/uuid/v5"
)

type streamIterator struct {
	rows *sql.Rows
}

func (i *streamIterator) Next(ctx context.Context) (*eventstore.Event, error) {
	if !i.rows.Next() {
		return nil, eventstore.ErrEndOfEventStream
	}

	if err := i.rows.Err(); errors.Is(err, sql.ErrNoRows) {
		return nil, eventstore.ErrEndOfEventStream
	} else if err != nil {
		return nil, fmt.Errorf("iterating rows: %w", err)
	}

	var (
		e          Event
		streamID   uuid.UUID
		streamType string
		eventID    uuid.UUID
		eventType  string
	)
	if err := i.rows.Scan(&streamID, &streamType, &eventID, &eventType, &e.Timestamp, &e.StreamVersion, &e.GlobalOffset, &e.Data); err != nil {
		return nil, fmt.Errorf("scanning row: %w", err)
	}

	e.ID = typeid.FromUUID(eventType, eventID)
	e.StreamID = typeid.FromUUID(streamType, streamID)

	return &e.Event, nil
}

func (i *streamIterator) Close(_ context.Context) error {
	return i.rows.Close()
}
