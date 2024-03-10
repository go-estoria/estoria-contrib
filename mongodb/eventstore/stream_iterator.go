package eventstore

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/go-estoria/estoria"
	"go.mongodb.org/mongo-driver/mongo"
)

type StreamIterator struct {
	cursor *mongo.Cursor
}

func (i *StreamIterator) Next(ctx context.Context) (estoria.Event, error) {
	if i.cursor.Next(ctx) {
		doc := eventDocument{}
		if err := i.cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decoding event document: %w", err)
		}

		evt, err := eventFromDocument(&doc)
		if err != nil {
			return nil, fmt.Errorf("parsing event document: %w", err)
		}

		slog.Default().WithGroup("eventstore").Debug("read event", "event_id", evt.id.String(), "stream_id", evt.streamID.String())
		return evt, nil
	}

	if err := i.cursor.Err(); err != nil {
		return nil, fmt.Errorf("fetching document: %w", err)
	}

	return nil, io.EOF
}
