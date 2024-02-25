package eventstore

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/go-estoria/estoria"
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

// NewEventStore creates a new event store using the given MongoDB client.
func NewEventStore(mongoClient *mongo.Client, opts ...EventStoreOption) (*EventStore, error) {
	eventStore := &EventStore{
		mongoClient:          mongoClient,
		databaseName:         defaultDatabaseName,
		eventsCollectionName: defaultEventsCollectionName,
		log:                  slog.Default(),
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

// LoadEvents loads the events for the given aggregate ID from the event store.
func (s *EventStore) LoadEvents(ctx context.Context, aggregateID estoria.TypedID) ([]estoria.Event, error) {
	log := slog.Default().WithGroup("eventstore")
	log.Debug("loading events", "aggregate_id", aggregateID)

	opts := options.Find().SetSort(bson.D{{Key: "timestamp", Value: 1}})
	cursor, err := s.events.Find(ctx, bson.M{"aggregate_id": aggregateID.ID.String()}, opts)
	if err != nil {
		log.Error("finding events", "error", err)
		return nil, fmt.Errorf("finding events: %w", err)
	}

	docs := []*eventDocument{}
	if err := cursor.All(ctx, &docs); err != nil {
		log.Error("iterating events", "error", err)
		return nil, fmt.Errorf("iterating events: %w", err)
	}

	events := make([]estoria.Event, len(docs))
	for i, doc := range docs {
		events[i] = doc
	}

	log.Debug("loaded events", "events", len(docs))

	return events, nil
}

// SaveEvents saves the given events to the event store.
func (s *EventStore) SaveEvents(ctx context.Context, events ...estoria.Event) error {
	log := slog.Default().WithGroup("eventstore")
	log.Debug("saving events", "count", len(events), "events", fmt.Sprintf("%#v", events))

	docs := make([]any, len(events))
	for i, e := range events {
		docs[i] = documentFromEvent(e)
	}

	log.Debug("starting MongoDB session")
	opts := options.Session().SetDefaultReadConcern(readconcern.Majority())
	session, err := s.mongoClient.StartSession(opts)
	if err != nil {
		return fmt.Errorf("starting MongoDB session: %w", err)
	}
	defer session.EndSession(ctx)

	transactionFn := func(sessCtx mongo.SessionContext) (any, error) {
		log.Debug("inserting events", "events", len(docs))
		result, err := s.events.InsertMany(sessCtx, docs)
		if err != nil {
			return nil, fmt.Errorf("inserting events: %w", err)
		}

		return result, nil
	}

	log.Debug("executing transaction")
	txOpts := options.Transaction().SetReadPreference(readpref.PrimaryPreferred())
	_, err = session.WithTransaction(ctx, transactionFn, txOpts)
	if err != nil {
		return fmt.Errorf("executing transaction: %w", err)
	}

	return nil
}
