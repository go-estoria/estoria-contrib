package eventstore

import (
	"context"
	"fmt"

	"github.com/go-estoria/estoria"
	"github.com/go-estoria/estoria-contrib/mongodb/eventstore/strategy"
	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

const (
	DefaultDatabaseName   string = "estoria"
	DefaultCollectionName string = "events"
)

type (
	// MongoClient provides APIs for obtaining database handles and starting sessions.
	MongoClient interface {
		Database(name string) *mongo.Database
		StartSession(opts ...*options.SessionOptions) (mongo.Session, error)
	}

	// Strategy provides APIs for reading and writing events to an event store.
	Strategy interface {
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

	// A TransactionHook is a function that is executed within the transaction used for appending events.
	// If a hook returns an error, the transaction is aborted and the error is returned to the caller.
	TransactionHook interface {
		HandleEvents(sessCtx mongo.SessionContext, events []*eventstore.Event) error
	}
)

// An EventStore stores and retrieves events using MongoDB as the underlying storage.
type EventStore struct {
	mongoClient    MongoClient
	strategy       Strategy
	sessionOptions *options.SessionOptions
	txOptions      *options.TransactionOptions
	txHooks        []TransactionHook
	log            estoria.Logger
}

var _ eventstore.StreamReader = (*EventStore)(nil)
var _ eventstore.StreamWriter = (*EventStore)(nil)

// New creates a new EventStore using the given MongoDB client.
func New(client MongoClient, opts ...EventStoreOption) (*EventStore, error) {
	if client == nil {
		return nil, fmt.Errorf("mongodb client is required")
	}

	eventStore := &EventStore{
		mongoClient:    client,
		sessionOptions: DefaultSessionOptions(),
		txOptions:      DefaultTransactionOptions(),
		log:            estoria.GetLogger().WithGroup("eventstore"),
	}
	for _, opt := range opts {
		if err := opt(eventStore); err != nil {
			return nil, fmt.Errorf("applying option: %w", err)
		}
	}

	// use a single collection strategy by default
	if eventStore.strategy == nil {
		strat, err := strategy.NewSingleCollectionStrategy(
			client.Database(DefaultDatabaseName).Collection(DefaultCollectionName),
		)
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

// DefaultSessionOptions returns the default session options used by the event store
// when starting a new MongoDB session.
func DefaultSessionOptions() *options.SessionOptions {
	return options.Session().
		SetDefaultReadConcern(readconcern.Majority()).
		SetDefaultReadPreference(readpref.Primary())
}

// DefaultTransactionOptions returns the default transaction options used by the event store
// when starting a new MongoDB transaction on a session.
func DefaultTransactionOptions() *options.TransactionOptions {
	return options.Transaction().SetReadPreference(readpref.Primary())
}

// Executes the given function within a session transaction.
// The function passed to this method must be idempotent, as the MongoDB transaction
// may be retried in the event of a transient error.
func (s *EventStore) doInTransaction(ctx context.Context, f func(sessCtx mongo.SessionContext) (any, error)) (any, error) {
	session, err := s.mongoClient.StartSession(s.sessionOptions)
	if err != nil {
		return nil, fmt.Errorf("starting MongoDB session: %w", err)
	}

	defer session.EndSession(ctx)

	result, err := session.WithTransaction(ctx, func(sessCtx mongo.SessionContext) (any, error) {
		return f(sessCtx)
	}, s.txOptions)
	if err != nil {
		return nil, fmt.Errorf("executing transaction: %w", err)
	}

	return result, nil
}
