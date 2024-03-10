package strategy

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/go-estoria/estoria"
	"go.jetpack.io/typeid"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type DatabasePerStreamStrategy struct {
	client         *mongo.Client
	collectionName string
	log            *slog.Logger
}

func NewDatabasePerStreamStrategy(client *mongo.Client, collection string) (*DatabasePerStreamStrategy, error) {
	if client == nil {
		return nil, fmt.Errorf("client is required")
	} else if collection == "" {
		return nil, fmt.Errorf("collection is required")
	}

	return &DatabasePerStreamStrategy{
		client:         client,
		collectionName: collection,
		log:            slog.Default().WithGroup("eventstore"),
	}, nil
}

func (s *DatabasePerStreamStrategy) GetStreamIterator(ctx context.Context, streamID typeid.AnyID) (estoria.EventStreamIterator, error) {
	database := s.client.Database(streamID.String())
	collection := database.Collection(s.collectionName)
	findOpts := options.Find().SetSort(bson.D{{Key: "timestamp", Value: 1}})
	cursor, err := collection.Find(ctx, bson.D{}, findOpts)
	if err != nil {
		return nil, fmt.Errorf("finding events: %w", err)
	}

	return &streamIterator[databasePerStreamEventDocument]{
		streamID: streamID,
		cursor:   cursor,
	}, nil
}

func (s *DatabasePerStreamStrategy) InsertStreamEvents(ctx context.Context, streamID typeid.AnyID, events []estoria.Event) (*mongo.InsertManyResult, error) {
	docs := make([]any, len(events))
	for i, event := range events {
		docs[i] = databasePerStreamEventDocumentFromEvent(event)
	}

	database := s.client.Database(streamID.String())
	collection := database.Collection(s.collectionName)
	result, err := collection.InsertMany(ctx, docs)
	if err != nil {
		return nil, fmt.Errorf("inserting events: %w", err)
	}

	return result, nil
}

type databasePerStreamEventDocument struct {
	EventID   string    `bson:"event_id"`
	EventType string    `bson:"event_type"`
	Timestamp time.Time `bson:"timestamp"`
	Data      []byte    `bson:"data"`
}

func databasePerStreamEventDocumentFromEvent(evt estoria.Event) databasePerStreamEventDocument {
	return databasePerStreamEventDocument{
		EventID:   evt.ID().Suffix(),
		EventType: evt.ID().Prefix(),
		Timestamp: evt.Timestamp(),
		Data:      evt.Data(),
	}
}

func (d databasePerStreamEventDocument) ToEvent(streamID typeid.AnyID) (estoria.Event, error) {
	eventID, err := typeid.From(d.EventType, d.EventID)
	if err != nil {
		return nil, fmt.Errorf("parsing event ID: %w", err)
	}

	return &event{
		id:        eventID,
		streamID:  streamID,
		timestamp: d.Timestamp,
		data:      d.Data,
	}, nil
}
