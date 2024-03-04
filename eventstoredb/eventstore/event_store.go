package eventstore

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/EventStore/EventStore-Client-Go/v3/esdb"
	"github.com/go-estoria/estoria"
	"github.com/gofrs/uuid"
	"go.jetpack.io/typeid"
)

type EventStore struct {
	esdbClient *esdb.Client
	log        *slog.Logger
}

// NewEventStore creates a new event store using the given ESDB client.
func NewEventStore(esdbClient *esdb.Client, opts ...EventStoreOption) (*EventStore, error) {
	eventStore := &EventStore{
		esdbClient: esdbClient,
		log:        slog.Default(),
	}

	for _, opt := range opts {
		if err := opt(eventStore); err != nil {
			return nil, fmt.Errorf("applying option: %w", err)
		}
	}

	return eventStore, nil
}

func (s *EventStore) ReadStream(ctx context.Context, streamID typeid.AnyID, opts estoria.ReadStreamOptions) (estoria.EventStreamIterator, error) {
	result, err := s.esdbClient.ReadStream(ctx, streamID.String(), esdb.ReadStreamOptions{}, 10)
	if err != nil {
		return nil, fmt.Errorf("reading stream: %w", err)
	}

	return &StreamIterator{
		streamID: streamID,
		client:   s.esdbClient,
		stream:   result,
	}, nil
}

// AppendStream saves the given events to the event store.
func (s *EventStore) AppendStream(ctx context.Context, streamID typeid.AnyID, opts estoria.AppendStreamOptions, events ...estoria.Event) error {
	log := slog.Default().WithGroup("eventstore")
	log.Debug("appending events to stream", "stream_id", streamID.String(), "events", len(events))

	streamEvents := make([]esdb.EventData, len(events))
	for i, e := range events {
		streamEvents[i] = esdb.EventData{
			EventID:     uuid.UUID(e.ID().UUIDBytes()),
			ContentType: esdb.ContentTypeJson,
			EventType:   e.ID().Prefix(),
			Data:        e.Data(),
		}
	}

	if _, err := s.esdbClient.AppendToStream(ctx, streamID.String(), esdb.AppendToStreamOptions{}, streamEvents...); err != nil {
		return fmt.Errorf("appending to stream: %w", err)
	}

	return nil
}
