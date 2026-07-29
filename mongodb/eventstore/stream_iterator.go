package eventstore

import (
	"context"
	"fmt"

	"github.com/go-estoria/estoria/eventstore"
)

// MongoCursor provides an API for iterating over a set of documents returned by a query.
type MongoCursor interface {
	Next(ctx context.Context) bool
	Decode(v any) error
	Err() error
	Close(ctx context.Context) error
}

type streamIterator struct {
	cursor    MongoCursor
	marshaler DocumentMarshaler
}

func (i *streamIterator) Next(ctx context.Context) (*eventstore.Event, error) {
	if i.cursor.Next(ctx) {
		evt, err := i.marshaler.UnmarshalDocument(i.cursor.Decode)
		if err != nil {
			return nil, fmt.Errorf("parsing event document: %w", err)
		}

		return &evt.Event, nil
	}

	if err := i.cursor.Err(); err != nil {
		return nil, fmt.Errorf("fetching document: %w", err)
	}

	return nil, eventstore.ErrEndOfEventStream
}

func (i *streamIterator) Close(ctx context.Context) error {
	return i.cursor.Close(ctx)
}
