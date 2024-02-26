package eventstore

import (
	"context"
	"errors"
	"fmt"
	"io"
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

// LoadEvents loads the events for the given aggregate ID from the event store.
func (s *EventStore) LoadEvents(ctx context.Context, aggregateID estoria.TypedID) ([]estoria.Event, error) {
	log := slog.Default().WithGroup("eventstore")
	log.Debug("loading events", "aggregate_id", aggregateID)

	stream, err := s.esdbClient.ReadStream(ctx, aggregateID.ID.String(), esdb.ReadStreamOptions{}, 10)
	if err != nil {
		return nil, fmt.Errorf("reading stream: %w", err)
	}
	defer stream.Close()

	events := make([]estoria.Event, 0)
	for {
		resolvedEvent, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			var esdbErr *esdb.Error
			if errors.As(err, &esdbErr) {
				log.Error("ESDB error", "code", esdbErr.Code(), "message", esdbErr.Err())
			} else {
				log.Error("unknown error receiving event", "error", err)
			}

			return nil, fmt.Errorf("receiving event: %w", err)
		}

		events = append(events, &eventDocument{
			EventAggregateID:   aggregateID.ID.String(),
			EventAggregateType: aggregateID.Type,
			EventType:          resolvedEvent.Event.EventType,
			EventID:            resolvedEvent.Event.EventID.String(),
			EventTimestamp:     resolvedEvent.Event.CreatedDate,
			EventData:          resolvedEvent.Event.Data,
		})
	}

	log.Debug("loaded events", "events", len(events))

	return events, nil
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
