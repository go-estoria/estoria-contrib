package eventstore

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/go-estoria/estoria"
	"github.com/go-estoria/estoria-contrib/mongodb/eventstore/strategy"
	"go.jetpack.io/typeid"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type EventStore struct {
	mongoClient *mongo.Client
	strategy    Strategy
	log         *slog.Logger
}

var _ estoria.EventStreamReader = (*EventStore)(nil)
var _ estoria.EventStreamWriter = (*EventStore)(nil)

type Strategy interface {
	GetStreamIterator(
		ctx context.Context,
		streamID typeid.AnyID,
		opts estoria.ReadStreamOptions,
	) (estoria.EventStreamIterator, error)
	InsertStreamEvents(
		ctx mongo.SessionContext,
		streamID typeid.AnyID,
		events []estoria.Event,
		opts estoria.AppendStreamOptions,
	) (*mongo.InsertManyResult, error)
}

// NewEventStore creates a new event store using the given MongoDB client.
func NewEventStore(mongoClient *mongo.Client, opts ...EventStoreOption) (*EventStore, error) {
	eventStore := &EventStore{
		mongoClient: mongoClient,
	}

	for _, opt := range opts {
		if err := opt(eventStore); err != nil {
			return nil, fmt.Errorf("applying option: %w", err)
		}
	}

	if eventStore.log == nil {
		eventStore.log = slog.Default().WithGroup("eventstore")
	}

	if eventStore.strategy == nil {
		strat, err := strategy.NewCollectionPerStreamStrategy(mongoClient, "streams")
		// strat, err := strategy.NewDatabasePerStreamStrategy(mongoClient, "events")
		// strat, err := strategy.NewSingleCollectionStrategy(mongoClient, "example-app", "events")
		if err != nil {
			return nil, fmt.Errorf("creating default strategy: %w", err)
		}

		eventStore.strategy = strat
	}

	return eventStore, nil
}

// ReadStream returns an iterator for reading events from the specified stream.
func (s *EventStore) ReadStream(ctx context.Context, streamID typeid.AnyID, opts estoria.ReadStreamOptions) (estoria.EventStreamIterator, error) {
	s.log.Debug("reading events from stream", "stream_id", streamID.String())

	iter, err := s.strategy.GetStreamIterator(ctx, streamID, opts)
	if err != nil {
		return nil, fmt.Errorf("getting stream cursor: %w", err)
	}

	return iter, nil
}

// AppendStream appends events to the specified stream.
func (s *EventStore) AppendStream(ctx context.Context, streamID typeid.AnyID, opts estoria.AppendStreamOptions, events ...estoria.Event) error {
	s.log.Debug("appending events to Mongo stream", "stream_id", streamID.String(), "events", len(events))

	sessionOpts := options.Session().
		SetDefaultReadConcern(readconcern.Majority()).
		SetDefaultReadPreference(readpref.Primary())
	session, err := s.mongoClient.StartSession(sessionOpts)
	if err != nil {
		return fmt.Errorf("starting MongoDB session: %w", err)
	}
	defer session.EndSession(ctx)

	transactionFn := func(sessCtx mongo.SessionContext) (any, error) {
		s.log.Debug("inserting events", "events", len(events))
		result, err := s.strategy.InsertStreamEvents(sessCtx, streamID, events, opts)
		if err != nil {
			return nil, fmt.Errorf("inserting events: %w", err)
		}

		return result, nil
	}

	txOpts := options.Transaction().SetReadPreference(readpref.Primary())
	_, err = session.WithTransaction(ctx, transactionFn, txOpts)
	if err != nil {
		return fmt.Errorf("executing transaction: %w", err)
	}

	return nil
}
