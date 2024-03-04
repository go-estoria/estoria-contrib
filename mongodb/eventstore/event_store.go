package eventstore

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/go-estoria/estoria"
	"go.jetpack.io/typeid"
	"go.mongodb.org/mongo-driver/bson"
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

	databaseName string
	database     *mongo.Database

	eventsCollectionName string
	events               *mongo.Collection

	log *slog.Logger
}

var _ estoria.EventStreamReader = (*EventStore)(nil)
var _ estoria.EventStreamWriter = (*EventStore)(nil)

// NewEventStore creates a new event store using the given MongoDB client.
func NewEventStore(mongoClient *mongo.Client, opts ...EventStoreOption) (*EventStore, error) {
	eventStore := &EventStore{
		mongoClient:          mongoClient,
		databaseName:         defaultDatabaseName,
		eventsCollectionName: defaultEventsCollectionName,
		log:                  slog.Default().WithGroup("eventstore"),
	}

	for _, opt := range opts {
		if err := opt(eventStore); err != nil {
			return nil, fmt.Errorf("applying option: %w", err)
		}
	}

	eventStore.database = mongoClient.Database(eventStore.databaseName)
	eventStore.events = eventStore.database.Collection(eventStore.eventsCollectionName)

	return eventStore, nil
}

// ReadStream returns an iterator for reading events from the specified stream.
func (s *EventStore) ReadStream(ctx context.Context, streamID typeid.AnyID, opts estoria.ReadStreamOptions) (estoria.EventStreamIterator, error) {
	s.log.Debug("reading events from stream", "stream_id", streamID.String())

	findOpts := options.Find().SetSort(bson.D{{Key: "timestamp", Value: 1}})
	cursor, err := s.events.Find(ctx, bson.M{"stream_id": streamID.Suffix()}, findOpts)
	if err != nil {
		s.log.Error("MongoDB error while finding events", "error", err)
		return nil, fmt.Errorf("finding events: %w", err)
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
		result, err := s.events.InsertMany(sessCtx, docs)
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
