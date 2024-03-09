package eventstore

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/go-estoria/estoria"
	"github.com/redis/go-redis/v9"
	"go.jetpack.io/typeid"
)

type StreamIterator struct {
	streamID    typeid.AnyID
	redis       *redis.Client
	batch       []redis.XMessage
	batchCursor int
	pageSize    int64
}

func (i *StreamIterator) Next(ctx context.Context) (estoria.Event, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if i.batch == nil {
		messages, err := i.redis.XRange(ctx, i.streamID.String(), "-", "+").Result()
		if err != nil {
			return nil, fmt.Errorf("reading stream: %w", err)
		} else if len(messages) == 0 {
			return nil, io.EOF
		}

		i.batch = messages
		i.batchCursor = 0
	}

	if i.batchCursor >= len(i.batch) {
		i.batch = nil
		return i.Next(ctx)
	}

	message := i.batch[i.batchCursor]
	i.batchCursor++

	eventData := message.Values

	eventIDStr, ok := eventData["event_id"].(string)
	if !ok {
		return nil, fmt.Errorf("event ID is not string")
	}

	eventID, err := typeid.FromString(eventIDStr)
	if err != nil {
		return nil, fmt.Errorf("parsing event ID: %w", err)
	}

	timestampStr, ok := eventData["timestamp"].(string)
	if !ok {
		return nil, fmt.Errorf("timestamp is not string")
	}

	timestamp, err := time.Parse(time.RFC3339, timestampStr)
	if err != nil {
		return nil, fmt.Errorf("parsing timestamp: %w", err)
	}

	data, ok := eventData["data"].(string)
	if !ok {
		return nil, fmt.Errorf("event data (%T) is not string", eventData["data"])
	}

	return &event{
		streamID:  i.streamID,
		id:        eventID,
		timestamp: timestamp,
		data:      []byte(data),
	}, nil
}
