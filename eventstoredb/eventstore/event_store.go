package eventstore

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/EventStore/EventStore-Client-Go/v3/esdb"
	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
	"github.com/gofrs/uuid"
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

func (s *EventStore) ReadStream(ctx context.Context, streamID typeid.UUID, opts eventstore.ReadStreamOptions) (eventstore.StreamIterator, error) {
	readOpts := esdb.ReadStreamOptions{
		Direction: esdb.Forwards,
		From:      esdb.Start{},
	}

	if opts.Direction == eventstore.Reverse {
		slog.Debug("reading stream in reverse", "stream_id", streamID.String())
		readOpts.Direction = esdb.Backwards
		readOpts.From = esdb.End{}
	}

	if opts.Offset > 0 {
		readOpts.From = esdb.StreamRevision{Value: uint64(opts.Offset)}
	}

	count := uint64(opts.Count)
	if count == 0 {
		// HACK: large value to read all events
		count = 1_000_000
	}

	result, err := s.esdbClient.ReadStream(ctx, streamID.String(), readOpts, count)
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
func (s *EventStore) AppendStream(ctx context.Context, streamID typeid.UUID, opts eventstore.AppendStreamOptions, events []*eventstore.EventStoreEvent) error {
	log := slog.Default().WithGroup("eventstore")
	log.Debug("appending events to stream", "stream_id", streamID.String(), "events", len(events))

	appendOpts := esdb.AppendToStreamOptions{
		ExpectedRevision: esdb.Any{},
	}

	if opts.ExpectVersion > 0 {
		appendOpts.ExpectedRevision = esdb.StreamRevision{Value: uint64(opts.ExpectVersion) + 1}
	}

	streamEvents := make([]esdb.EventData, len(events))
	for i, e := range events {
		eventID, err := uuid.FromString(e.ID.Value())
		if err != nil {
			return fmt.Errorf("parsing event ID: %w", err)
		}

		streamEvents[i] = esdb.EventData{
			EventID:     eventID,
			ContentType: esdb.ContentTypeJson,
			EventType:   e.ID.TypeName(),
			Data:        e.Data,
		}
	}

	if _, err := s.esdbClient.AppendToStream(ctx, streamID.String(), esdb.AppendToStreamOptions{}, streamEvents...); err != nil {
		return fmt.Errorf("appending to stream: %w", err)
	}

	return nil
}
