package strategy

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/go-estoria/estoria"
	"github.com/go-estoria/estoria/typeid"
	"go.mongodb.org/mongo-driver/mongo"
)

type eventDocument interface {
	ToEvent(streamID typeid.UUID) (estoria.EventStoreEvent, error)
}

type streamIterator[D eventDocument] struct {
	streamID    typeid.UUID
	docTemplate D
	cursor      *mongo.Cursor
}

func (i *streamIterator[D]) Next(ctx context.Context) (estoria.EventStoreEvent, error) {
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

type event struct {
	id            typeid.UUID
	streamID      typeid.UUID
	streamVersion int64
	timestamp     time.Time
	data          []byte
}

var _ estoria.EventStoreEvent = (*event)(nil)

func (e *event) ID() typeid.UUID {
	return e.id
}

func (e *event) StreamID() typeid.UUID {
	return e.streamID
}

func (e *event) StreamVersion() int64 {
	return e.streamVersion
}

func (e *event) Timestamp() time.Time {
	return e.timestamp
}

func (e *event) Data() []byte {
	return e.data
}
