package eventstore

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/go-estoria/estoria"
	"github.com/go-estoria/estoria/typeid"
	"github.com/redis/go-redis/v9"
)

type StreamIterator struct {
	streamID      typeid.TypeID
	redis         *redis.Client
	batch         []redis.XMessage
	lastMessageID string
	batchCursor   int
	pageSize      int64
}

func (i *StreamIterator) Next(ctx context.Context) (estoria.EventStoreEvent, error) {
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

		messages, err := i.redis.XRangeN(ctx, i.streamID.String(), i.lastMessageID, "+", i.pageSize).Result()
		if err != nil {
			return nil, fmt.Errorf("reading stream: %w", err)
		} else if len(messages) == 0 {
			return nil, io.EOF
		}

		log.Debug("read message batch", "page_size", i.pageSize, "count", len(messages), "begin", messages[0].ID, "end", messages[len(messages)-1].ID)

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
