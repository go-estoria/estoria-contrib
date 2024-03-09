package eventstore

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/go-estoria/estoria"
	"github.com/go-estoria/estoria-contrib/mongodb/eventstore/strategy"
	"go.jetpack.io/typeid"
	"go.mongodb.org/mongo-driver/mongo"
)

const (
	defaultDatabaseName         = "eventstore"
	defaultEventsCollectionName = "events"
)

type EventStore struct {
	mongoClient *mongo.Client
	strategy    Strategy

	databaseName string
	database     *mongo.Database

	eventsCollectionName string
	events               *mongo.Collection

	log *slog.Logger
}

var _ estoria.EventStreamReader = (*EventStore)(nil)
var _ estoria.EventStreamWriter = (*EventStore)(nil)

type Strategy interface {
	Initialize(client *mongo.Client) error
	ReadStream(ctx context.Context, streamID typeid.AnyID, opts estoria.ReadStreamOptions) (estoria.EventStreamIterator, error)
	AppendStream(ctx context.Context, streamID typeid.AnyID, opts estoria.AppendStreamOptions, events ...estoria.Event) error
}

// NewEventStore creates a new event store using the given MongoDB client.
func NewEventStore(mongoClient *mongo.Client, opts ...EventStoreOption) (*EventStore, error) {
	eventStore := &EventStore{
		log:                  slog.Default().WithGroup("eventstore"),
		mongoClient:          mongoClient,
		databaseName:         defaultDatabaseName,
		eventsCollectionName: defaultEventsCollectionName,
	}

	for _, opt := range opts {
		if err := opt(eventStore); err != nil {
			return nil, fmt.Errorf("applying option: %w", err)
		}
	}

	if eventStore.strategy == nil {
		eventStore.strategy = &strategy.SingleCollectionStrategy{
			DatabaseName:   eventStore.databaseName,
			CollectionName: eventStore.eventsCollectionName,
		}
	}

	if err := eventStore.strategy.Initialize(mongoClient); err != nil {
		return nil, fmt.Errorf("initializing strategy: %w", err)
	}

	return eventStore, nil
}

// ReadStream returns an iterator for reading events from the specified stream.
func (s *EventStore) ReadStream(ctx context.Context, streamID typeid.AnyID, opts estoria.ReadStreamOptions) (estoria.EventStreamIterator, error) {
	return s.strategy.ReadStream(ctx, streamID, opts)
}

// AppendStream appends events to the specified stream.
func (s *EventStore) AppendStream(ctx context.Context, streamID typeid.AnyID, opts estoria.AppendStreamOptions, events ...estoria.Event) error {
	return s.strategy.AppendStream(ctx, streamID, opts, events...)
}
