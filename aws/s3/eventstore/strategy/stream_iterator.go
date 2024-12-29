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
	cursor    BucketCursor
	marshaler ObjectMarshaler
}

func (i *streamIterator) Next(ctx context.Context) (*eventstore.Event, error) {
	if i.cursor.HasNext(ctx) {
		reader, err := i.cursor.Next(ctx)
		if err != nil {
			return nil, fmt.Errorf("getting next object: %w", err)
		}

		defer reader.Close()

		evt, err := i.marshaler.UnmarshalObject(reader)
		if err != nil {
			return nil, fmt.Errorf("parsing event document: %w", err)
		}

		return evt, nil
	}

	return nil, io.EOF
}

func (i *streamIterator) Close(ctx context.Context) error {
	return nil
}
