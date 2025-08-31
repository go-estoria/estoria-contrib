package eventstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/go-estoria/estoria/eventstore"
)

type streamIterator struct {
	strategy Strategy
	rows     *sql.Rows
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

	event, err := i.strategy.ScanEventRow(i.rows)
	if err != nil {
		return nil, fmt.Errorf("scanning event row: %w", err)
	}

	return event, nil
}

func (i *streamIterator) Close(_ context.Context) error {
	return i.rows.Close()
}
