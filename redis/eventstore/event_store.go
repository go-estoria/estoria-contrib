package eventstore

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/go-estoria/estoria"
	"github.com/redis/go-redis/v9"
	"go.jetpack.io/typeid"
)

const (
	defaultPageSize = 100
)

type EventStore struct {
	redisClient *redis.Client
	log         *slog.Logger
}

// NewEventStore creates a new event store using the given ESDB client.
func NewEventStore(redisClient *redis.Client, opts ...EventStoreOption) (*EventStore, error) {
	eventStore := &EventStore{
		redisClient: redisClient,
		log:         slog.Default(),
	}

	for _, opt := range opts {
		if err := opt(eventStore); err != nil {
			return nil, fmt.Errorf("applying option: %w", err)
		}
	}

	return eventStore, nil
}

func (s *EventStore) ReadStream(ctx context.Context, streamID typeid.AnyID, opts estoria.ReadStreamOptions) (estoria.EventStreamIterator, error) {
	log := slog.Default().WithGroup("eventstore")
	log.Debug("reading stream", "stream_id", streamID.String())

	return &StreamIterator{
		streamID: streamID,
		redis:    s.redisClient,
		pageSize: defaultPageSize,
	}, nil
}

// AppendStream saves the given events to the event store.
func (s *EventStore) AppendStream(ctx context.Context, streamID typeid.AnyID, opts estoria.AppendStreamOptions, events ...estoria.Event) error {
	log := slog.Default().WithGroup("eventstore")
	log.Debug("appending events to stream", "stream_id", streamID.String(), "events", len(events))

	pipeline := s.redisClient.TxPipeline()

	for _, e := range events {
		pipeline.XAdd(ctx, &redis.XAddArgs{
			Stream: streamID.String(),
			Values: map[string]any{
				"event_id":  e.ID().String(),
				"timestamp": e.Timestamp().Format(time.RFC3339),
				"data":      e.Data(),
			},
		})
	}

	if _, err := pipeline.Exec(ctx); err != nil {
		return fmt.Errorf("appending events: %w", err)
	}

	log.Debug("appended events to stream", "stream_id", streamID.String(), "events", len(events))

	return nil
}
