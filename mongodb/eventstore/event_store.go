package eventstore

import (
	"context"
	"fmt"
	"time"

	"github.com/go-estoria/estoria"
	"github.com/go-estoria/estoria-contrib/mongodb/eventstore/strategy"
	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type EventStore struct {
	mongoClient *mongo.Client
	strategy    Strategy
	txHooks     []TransactionHook
	log         estoria.Logger
}

var _ eventstore.StreamReader = (*EventStore)(nil)
var _ eventstore.StreamWriter = (*EventStore)(nil)

type TransactionHook interface {
	HandleEvents(sessCtx mongo.SessionContext, events []*eventstore.Event) error
}

type Strategy interface {
	GetStreamIterator(
		ctx context.Context,
		streamID typeid.UUID,
		opts eventstore.ReadStreamOptions,
	) (eventstore.StreamIterator, error)
	InsertStreamEvents(
		ctx mongo.SessionContext,
		streamID typeid.UUID,
		events []*eventstore.WritableEvent,
		opts eventstore.AppendStreamOptions,
	) (*strategy.InsertStreamEventsResult, error)
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
		eventStore.log = estoria.GetLogger().WithGroup("eventstore")
	}

	if eventStore.strategy == nil {
		db := mongoClient.Database("estoria")
		collection := db.Collection("events")
		strat, err := strategy.NewSingleCollectionStrategy(collection)
		if err != nil {
			return nil, fmt.Errorf("creating default strategy: %w", err)
		}

		eventStore.strategy = strat
	}

	return eventStore, nil
}

// ReadStream returns an iterator for reading events from the specified stream.
func (s *EventStore) ReadStream(ctx context.Context, streamID typeid.UUID, opts eventstore.ReadStreamOptions) (eventstore.StreamIterator, error) {
	s.log.Debug("reading events from stream", "stream_id", streamID.String())

	iter, err := s.strategy.GetStreamIterator(ctx, streamID, opts)
	if err != nil {
		return nil, fmt.Errorf("getting stream cursor: %w", err)
	}

	return iter, nil
}

// AppendStream appends events to the specified stream.
func (s *EventStore) AppendStream(ctx context.Context, streamID typeid.UUID, events []*eventstore.WritableEvent, opts eventstore.AppendStreamOptions) error {
	s.log.Debug("appending events to Mongo stream", "stream_id", streamID.String(), "events", len(events))

	fullEvents := make([]*eventstore.Event, len(events))
	for i, event := range events {
		fullEvents[i] = &eventstore.Event{
			ID:        event.ID,
			StreamID:  streamID,
			Timestamp: time.Now(),
			Data:      event.Data,
			// StreamVersion: 0, // assigned by the strategy
		}
	}

	result, txErr := s.doInTransaction(ctx, func(sessCtx mongo.SessionContext) (any, error) {
		s.log.Debug("inserting events", "events", len(events))
		insertResult, err := s.strategy.InsertStreamEvents(sessCtx, streamID, events, opts)
		if err != nil {
			s.log.Debug("inserting events failed", "err", err)
			return nil, fmt.Errorf("inserting events: %w", err)
		}

		for i, hook := range s.txHooks {
			s.log.Debug("executing transaction hook", "hook", i)
			if err := hook.HandleEvents(sessCtx, insertResult.InsertedEvents); err != nil {
				s.log.Debug("transaction hook failed", "hook", i, "err", err)
				return nil, fmt.Errorf("executing transaction hook: %w", err)
			}
		}

		return insertResult, nil
	})
	if txErr != nil {
		s.log.Debug("transaction failed", "err", txErr)
		return fmt.Errorf("executing transaction: %w", txErr)
	} else if result == nil {
		s.log.Debug("transaction failed", "err", "no result")
		return fmt.Errorf("executing transaction: no result")
	}

	return nil
}

// Executes the given function within a session transaction.
// The function passed to this method must be idempotent, as the MongoDB transaction may be retried
// in the event of a transient error.
func (s *EventStore) doInTransaction(ctx context.Context, f func(sessCtx mongo.SessionContext) (any, error)) (any, error) {
	sessionOpts := options.Session().
		SetDefaultReadConcern(readconcern.Majority()).
		SetDefaultReadPreference(readpref.Primary())
	session, err := s.mongoClient.StartSession(sessionOpts)
	if err != nil {
		return nil, fmt.Errorf("starting MongoDB session: %w", err)
	}
	defer session.EndSession(ctx)

	txOpts := options.Transaction().SetReadPreference(readpref.Primary())
	result, err := session.WithTransaction(ctx, func(sessCtx mongo.SessionContext) (any, error) {
		return f(sessCtx)
	}, txOpts)
	if err != nil {
		return nil, fmt.Errorf("executing transaction: %w", err)
	}

	return result, nil
}
