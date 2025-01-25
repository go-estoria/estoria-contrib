package strategy

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
	streamID typeid.UUID
	rows     *sql.Rows
}

func (i *streamIterator) All(ctx context.Context) ([]*eventstore.Event, error) {
	var events []*eventstore.Event
	for {
		e, err := i.Next(ctx)
		if errors.Is(err, eventstore.ErrEndOfEventStream) {
			break
		} else if err != nil {
			return nil, fmt.Errorf("iterating events: %w", err)
		}

		events = append(events, e)
	}

	return events, nil
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

	var e eventstore.Event
	var eventID uuid.UUID
	var eventType string
	if err := i.rows.Scan(&eventID, &eventType, &e.Timestamp, &e.Data); err != nil {
		return nil, fmt.Errorf("scanning row: %w", err)
	}

	e.ID = typeid.FromUUID(eventType, eventID)

	return &e, nil
}

func (i *streamIterator) Close(_ context.Context) error {
	return i.rows.Close()
}
