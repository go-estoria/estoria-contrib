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

type EventStore struct {
	mongoClient *mongo.Client
	database    *mongo.Database
	events      *mongo.Collection
}

func NewEventStore(mongoClient *mongo.Client, database, eventsCollection string) (*EventStore, error) {
	db := mongoClient.Database(database)
	events := db.Collection(eventsCollection)

	eventStore := &EventStore{
		mongoClient: mongoClient,
		database:    db,
		events:      events,
	}

	return eventStore, nil
}

func (s *EventStore) LoadEvents(ctx context.Context, aggregateID estoria.AggregateID) ([]estoria.Event, error) {
	log := slog.Default().WithGroup("eventstore")
	log.Debug("loading events", "aggregate_id", aggregateID)

	opts := options.Find().SetSort(bson.D{{"timestamp", 1}})
	cursor, err := s.events.Find(ctx, bson.M{"aggregate_id": aggregateID.String()}, opts)
	if err != nil {
		log.Error("finding events", "error", err)
		return nil, fmt.Errorf("finding events: %w", err)
	}

	docs := []eventDocument{}
	if err := cursor.All(ctx, &docs); err != nil {
		log.Error("iterating events", "error", err)
		return nil, fmt.Errorf("iterating events: %w", err)
	}

	log.Debug("found events", "events", len(docs))

	events := make([]estoria.Event, len(docs))
	for i, doc := range docs {
		events[i] = doc
	}

	return events, nil
}

// SaveEvents saves the given events to the event store.
func (s *EventStore) SaveEvents(ctx context.Context, events ...estoria.Event) error {
	log := slog.Default().WithGroup("eventstore")
	log.Debug("saving events", "events", len(events))

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
		// db := s.mongoClient.Database("mmmm")
		// events := db.Collection("events")
		result, err := s.events.InsertMany(sessCtx, docs)
		if err != nil {
			log.Error("inserting events", "error", err)
			return nil, fmt.Errorf("inserting events: %w", err)
		}

		log.Debug("InsertMany result", "result", result)
		return result, nil
	}

	log.Debug("executing transaction")
	txOpts := options.Transaction().SetReadPreference(readpref.PrimaryPreferred())
	result, err := session.WithTransaction(ctx, transactionFn, txOpts)
	if err != nil {
		return fmt.Errorf("executing transaction: %w", err)
	}

	slog.Debug("InsertMany result", "result", result)
	return nil
}

type EventStoreOption func(*EventStore) error

func WithClient(client *mongo.Client) EventStoreOption {
	return func(s *EventStore) error {
		s.mongoClient = client
		return nil
	}
}

// ErrEventExists is returned when attempting to write an event that already exists.
type ErrEventExists struct {
	EventID estoria.EventID
}

// Error returns the error message.
func (e ErrEventExists) Error() string {
	return fmt.Sprintf("event already exists: %s", e.EventID)
}
