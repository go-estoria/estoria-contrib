package eventstore

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/go-estoria/estoria"
	"github.com/redis/go-redis/v9"
	"go.jetpack.io/typeid"
)

type StreamIterator struct {
	streamID      typeid.AnyID
	redis         *redis.Client
	batch         []redis.XMessage
	lastMessageID string
	batchCursor   int
}

func (i *StreamIterator) Next(ctx context.Context) (estoria.Event, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	log := slog.Default().WithGroup("eventstore")

	if i.batch == nil {
		if i.lastMessageID == "" {
			i.lastMessageID = "-"
		}

		messages, err := i.redis.XRange(ctx, i.streamID.String(), i.lastMessageID, "+").Result()
		if err != nil {
			return nil, fmt.Errorf("reading stream: %w", err)
		} else if len(messages) == 0 {
			return nil, io.EOF
		}

		log.Debug("read message batch", "count", len(messages), "begin", messages[0].ID, "end", messages[len(messages)-1].ID)

		i.batch = messages
		i.batchCursor = 0
	}

	// if we've reached the end of the batch, time to fetch again
	if i.batchCursor >= len(i.batch) {
		i.batch = nil
		return i.Next(ctx)
	}

	// grab the next message from the batch
	message := i.batch[i.batchCursor]

	i.lastMessageID = fmt.Sprintf("(%s", message.ID)
	i.batchCursor++

	event, err := eventFromRedisMessage(i.streamID, message)
	if err != nil {
		return nil, fmt.Errorf("parsing redis message: %w", err)
	}

	return event, nil
}

func eventFromRedisMessage(streamID typeid.AnyID, message redis.XMessage) (estoria.Event, error) {
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
		streamID:  streamID,
		id:        eventID,
		timestamp: timestamp,
		data:      []byte(data),
	}, nil
}
