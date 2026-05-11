package eventstore

import (
	"context"
	"fmt"

	"github.com/go-estoria/estoria/eventstore"
	"github.com/jackc/pgx/v5"
)

type streamIterator struct {
	strategy Strategy
	rows     pgx.Rows
	first    *eventstore.Event
	closed   bool
}

// Next returns the next event from the iterator.
// ctx is accepted for interface compatibility but is not used by this implementation.
func (i *streamIterator) Next(ctx context.Context) (*eventstore.Event, error) {
	if i.closed {
		return nil, eventstore.ErrStreamIteratorClosed
	}

	if i.first != nil {
		event := i.first
		i.first = nil
		return event, nil
	}

	if !i.rows.Next() {
		if err := i.rows.Err(); err != nil {
			return nil, fmt.Errorf("iterating rows: %w", err)
		}
		return nil, eventstore.ErrEndOfEventStream
	}

	event, err := i.strategy.ScanEventRow(i.rows)
	if err != nil {
		return nil, fmt.Errorf("scanning event row: %w", err)
	}

	return event, nil
}

func (i *streamIterator) Close(_ context.Context) error {
	i.closed = true
	i.rows.Close()
	return nil
}

// emptyStreamIterator is a StreamIterator that immediately returns ErrEndOfEventStream.
type emptyStreamIterator struct{}

func (emptyStreamIterator) Next(_ context.Context) (*eventstore.Event, error) {
	return nil, eventstore.ErrEndOfEventStream
}

func (emptyStreamIterator) Close(_ context.Context) error {
	return nil
}
