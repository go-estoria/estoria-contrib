package strategy

import (
	"context"
	"fmt"
	"io"

	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
)

type streamIterator struct {
	streamID  typeid.UUID
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

	return nil, io.EOF
}

func (i *streamIterator) Close(ctx context.Context) error {
	return i.cursor.Close(ctx)
}
