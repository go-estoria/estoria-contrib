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
	GetStreamCursor(ctx context.Context, streamID typeid.AnyID) (*mongo.Cursor, error)
	InsertStreamDocuments(ctx context.Context, docs []any) (*mongo.InsertManyResult, error)
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
		strat, err := strategy.NewSingleCollectionStrategy(mongoClient, eventStore.databaseName, eventStore.eventsCollectionName)
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

	cursor, err := s.strategy.GetStreamCursor(ctx, streamID)
	if err != nil {
		return nil, fmt.Errorf("getting stream cursor: %w", err)
	}

	iter := &StreamIterator{
		cursor: cursor,
	}

	return iter, nil
}

// AppendStream appends events to the specified stream.
func (s *EventStore) AppendStream(ctx context.Context, streamID typeid.AnyID, opts estoria.AppendStreamOptions, events ...estoria.Event) error {
	s.log.Debug("appending events to stream", "stream_id", streamID.String(), "events", len(events))

	docs := make([]any, len(events))
	for i, e := range events {
		docs[i] = documentFromEvent(e)
	}

	s.log.Debug("starting MongoDB session")
	sessionOpts := options.Session().SetDefaultReadConcern(readconcern.Majority())
	session, err := s.mongoClient.StartSession(sessionOpts)
	if err != nil {
		return fmt.Errorf("starting MongoDB session: %w", err)
	}
	defer session.EndSession(ctx)

	transactionFn := func(sessCtx mongo.SessionContext) (any, error) {
		s.log.Debug("inserting events", "events", len(docs))
		result, err := s.strategy.InsertStreamDocuments(sessCtx, docs)
		if err != nil {
			return nil, fmt.Errorf("inserting events: %w", err)
		}

		return result, nil
	}

	s.log.Debug("executing transaction")
	txOpts := options.Transaction().SetReadPreference(readpref.PrimaryPreferred())
	_, err = session.WithTransaction(ctx, transactionFn, txOpts)
	if err != nil {
		return fmt.Errorf("executing transaction: %w", err)
	}

	return nil
}
