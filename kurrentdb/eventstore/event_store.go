package eventstore

import (
	"context"
	"fmt"

	"github.com/go-estoria/estoria"
	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
	"github.com/gofrs/uuid/v5"
	guuid "github.com/google/uuid"
	"github.com/kurrent-io/KurrentDB-Client-Go/kurrentdb"
)

type KurrentClient interface {
	ReadStream(context context.Context, streamID string, opts kurrentdb.ReadStreamOptions, count uint64) (*kurrentdb.ReadStream, error)
	AppendToStream(context context.Context, streamID string, opts kurrentdb.AppendToStreamOptions, events ...kurrentdb.EventData) (*kurrentdb.WriteResult, error)
}

type EventStore struct {
	kurrentDB KurrentClient
	log       estoria.Logger
}

var _ eventstore.StreamReader = (*EventStore)(nil)
var _ eventstore.StreamWriter = (*EventStore)(nil)

// NewEventStore creates a new event store using the given ESDB client.
func NewEventStore(kurrentDB KurrentClient, opts ...EventStoreOption) (*EventStore, error) {
	eventStore := &EventStore{
		kurrentDB: kurrentDB,
		log:       estoria.GetLogger().WithGroup("eventstore"),
	}

	for _, opt := range opts {
		if err := opt(eventStore); err != nil {
			return nil, fmt.Errorf("applying option: %w", err)
		}
	}

	return eventStore, nil
}

func (s *EventStore) ReadStream(ctx context.Context, streamID typeid.ID, opts eventstore.ReadStreamOptions) (eventstore.StreamIterator, error) {
	readOpts := kurrentdb.ReadStreamOptions{
		Direction: kurrentdb.Forwards,
		From:      kurrentdb.Start{},
	}

	if opts.Offset > 0 {
		readOpts.From = kurrentdb.StreamRevision{Value: uint64(opts.Offset - 1)}
	}

	count := uint64(opts.Count)
	if count == 0 {
		// HACK: large value to read all events
		count = 1_000_000
	}

	result, err := s.kurrentDB.ReadStream(ctx, streamID.String(), readOpts, count)
	if err != nil {
		return nil, fmt.Errorf("reading stream: %w", err)
	}

	return &streamIterator{
		streamID: streamID,
		stream:   result,
	}, nil
}

// AppendStream saves the given events to the event store.
func (s *EventStore) AppendStream(ctx context.Context, streamID typeid.ID, events []*eventstore.WritableEvent, opts eventstore.AppendStreamOptions) error {
	s.log.Debug("appending events to stream", "stream_id", streamID.String(), "events", len(events))

	appendOpts := kurrentdb.AppendToStreamOptions{}

	if opts.ExpectVersion > 0 {
		appendOpts.StreamState = kurrentdb.StreamRevision{Value: uint64(opts.ExpectVersion)}
	}

	streamEvents := make([]kurrentdb.EventData, len(events))
	for i, e := range events {
		eventID, err := uuid.NewV4()
		if err != nil {
			return fmt.Errorf("generating event ID: %w", err)
		}

		streamEvents[i] = kurrentdb.EventData{
			EventID:     guuid.UUID(eventID),
			ContentType: kurrentdb.ContentTypeJson,
			EventType:   e.Type,
			Data:        e.Data,
		}
	}

	if _, err := s.kurrentDB.AppendToStream(ctx, streamID.String(), appendOpts, streamEvents...); err != nil {
		return fmt.Errorf("appending to stream: %w", err)
	}

	return nil
}
