package eventstore

import (
	"context"
	"fmt"

	"github.com/EventStore/EventStore-Client-Go/v3/esdb"
	"github.com/go-estoria/estoria"
	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
	gofrsv1 "github.com/gofrs/uuid"
	"github.com/gofrs/uuid/v5"
)

type ESDBClient interface {
	ReadStream(context context.Context, streamID string, opts esdb.ReadStreamOptions, count uint64) (*esdb.ReadStream, error)
	AppendToStream(context context.Context, streamID string, opts esdb.AppendToStreamOptions, events ...esdb.EventData) (*esdb.WriteResult, error)
}

type EventStore struct {
	esdbClient ESDBClient
	log        estoria.Logger
}

var _ eventstore.StreamReader = (*EventStore)(nil)
var _ eventstore.StreamWriter = (*EventStore)(nil)

// NewEventStore creates a new event store using the given ESDB client.
func NewEventStore(esdbClient ESDBClient, opts ...EventStoreOption) (*EventStore, error) {
	eventStore := &EventStore{
		esdbClient: esdbClient,
		log:        estoria.GetLogger().WithGroup("eventstore"),
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
		s.log.Debug("reading stream in reverse", "stream_id", streamID.String())
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

	return &streamIterator{
		streamID: streamID,
		stream:   result,
	}, nil
}

// AppendStream saves the given events to the event store.
func (s *EventStore) AppendStream(ctx context.Context, streamID typeid.UUID, events []*eventstore.WritableEvent, opts eventstore.AppendStreamOptions) error {
	s.log.Debug("appending events to stream", "stream_id", streamID.String(), "events", len(events))

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
			EventID:     gofrsv1.UUID(eventID),
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
