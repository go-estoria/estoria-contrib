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
	mongoClient   *mongo.Client
	strategy      Strategy
	appendTxHooks []TransactionHook
	log           *slog.Logger
}

var _ estoria.EventStreamReader = (*EventStore)(nil)
var _ estoria.EventStreamWriter = (*EventStore)(nil)

type TransactionHook interface {
	HandleEvents(sessCtx mongo.SessionContext, events []estoria.Event) error
}

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

	result, txErr := s.doInTransaction(ctx, func(sessCtx mongo.SessionContext) (any, error) {
		s.log.Debug("inserting events", "events", len(events))
		var err error
		insertResult, err := s.strategy.InsertStreamEvents(sessCtx, streamID, events, opts)
		if err != nil {
			slog.Debug("inserting events failed", "err", err)
			return nil, fmt.Errorf("inserting events: %w", err)
		}

		for i, hook := range s.appendTxHooks {
			slog.Debug("executing transaction hook", "hook", i)
			if err := hook.HandleEvents(sessCtx, events); err != nil {
				slog.Debug("transaction hook failed", "hook", i, "err", err)
				return nil, fmt.Errorf("executing transaction hook: %w", err)
			}
		}

		return insertResult, nil
	})
	if txErr != nil {
		slog.Debug("transaction failed", "err", txErr)
		return fmt.Errorf("executing transaction: %w", txErr)
	} else if result == nil {
		slog.Debug("transaction failed", "err", "no result")
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
