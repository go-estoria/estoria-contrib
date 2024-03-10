package strategy

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/go-estoria/estoria"
	"go.jetpack.io/typeid"
	"go.mongodb.org/mongo-driver/mongo"
)

type eventDocument interface {
	ToEvent(streamID typeid.AnyID) (estoria.Event, error)
}

type streamIterator[D eventDocument] struct {
	streamID    typeid.AnyID
	docTemplate D
	cursor      *mongo.Cursor
}

func (i *streamIterator[D]) Next(ctx context.Context) (estoria.Event, error) {
	if i.cursor.Next(ctx) {
		doc := i.docTemplate
		if err := i.cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decoding event document: %w", err)
		}

		evt, err := doc.ToEvent(i.streamID)
		if err != nil {
			return nil, fmt.Errorf("parsing event document: %w", err)
		}

		slog.Default().WithGroup("eventstore").Debug("read event", "event_id", evt.ID().String(), "stream_id", evt.StreamID().String())
		return evt, nil
	}

	if err := i.cursor.Err(); err != nil {
		return nil, fmt.Errorf("fetching document: %w", err)
	}

	return nil, io.EOF
}

type event struct {
	id        typeid.AnyID
	streamID  typeid.AnyID
	timestamp time.Time
	data      []byte
}

var _ estoria.Event = (*event)(nil)

func (e *event) ID() typeid.AnyID {
	return e.id
}

func (e *event) StreamID() typeid.AnyID {
	return e.streamID
}

func (e *event) Timestamp() time.Time {
	return e.timestamp
}

func (e *event) Data() []byte {
	return e.data
}
