package strategy

import (
	"context"
	"fmt"
	"io"

	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
	"go.mongodb.org/mongo-driver/mongo"
)

type streamIterator struct {
	streamID  typeid.UUID
	cursor    *mongo.Cursor
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
