package strategy

import (
	"context"
	"fmt"
	"io"

	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
	"go.mongodb.org/mongo-driver/mongo"
)

type eventDocument interface {
	ToEvent(streamID typeid.UUID) (*eventstore.Event, error)
}

type streamIterator[D eventDocument] struct {
	streamID    typeid.UUID
	docTemplate D
	cursor      *mongo.Cursor
}

func (i *streamIterator[D]) Next(ctx context.Context) (*eventstore.Event, error) {
	if i.cursor.Next(ctx) {
		doc := i.docTemplate
		if err := i.cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decoding event document: %w", err)
		}

		evt, err := doc.ToEvent(i.streamID)
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
