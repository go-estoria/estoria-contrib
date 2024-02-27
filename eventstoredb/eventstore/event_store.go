package eventstore

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/EventStore/EventStore-Client-Go/v3/esdb"
	"github.com/go-estoria/estoria"
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

func (s *EventStore) FindStream(ctx context.Context, aggregateID estoria.TypedID) (estoria.EventStream, error) {
	result, err := s.esdbClient.ReadStream(ctx, aggregateID.ID.String(), esdb.ReadStreamOptions{}, 10)
	if err != nil {
		return nil, fmt.Errorf("reading stream: %w", err)
	}

	return &EventStream{
		id:     aggregateID.ID,
		client: s.esdbClient,
		stream: result,
	}, nil
}

// SaveEvents saves the given events to the event store.
func (s *EventStore) SaveEvents(ctx context.Context, events ...estoria.Event) error {
	log := slog.Default().WithGroup("eventstore")
	log.Debug("saving events", "count", len(events))

	for _, evt := range events {
		data := esdb.EventData{
			ContentType: esdb.ContentTypeJson,
			EventType:   evt.ID().Type,
			Data:        evt.Data(),
		}

		if _, err := s.esdbClient.AppendToStream(
			ctx,
			evt.AggregateID().ID.String(),
			esdb.AppendToStreamOptions{},
			data,
		); err != nil {
			return fmt.Errorf("appending to stream: %w", err)
		}
	}

	return nil
}
