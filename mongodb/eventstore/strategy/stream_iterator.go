package strategy

import (
	"context"
	"fmt"

	"github.com/go-estoria/estoria/eventstore"
)

type (
	// DecodeDocumentFunc is a function that decodes a MongoDB document into a destination type.
	DecodeDocumentFunc func(dest any) error

	// A DocumentMarshaler is responsible for marshaling and unmarshaling MongoDB documents to and from event store events.
	DocumentMarshaler interface {
		MarshalDocument(event *eventstore.Event) (any, error)
		UnmarshalDocument(decode DecodeDocumentFunc) (*eventstore.Event, error)
	}
)

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

		return evt, nil
	}

	if err := i.cursor.Err(); err != nil {
		return nil, fmt.Errorf("fetching document: %w", err)
	}

	return nil, eventstore.ErrEndOfEventStream
}

func (i *streamIterator) Close(ctx context.Context) error {
	return i.cursor.Close(ctx)
}
