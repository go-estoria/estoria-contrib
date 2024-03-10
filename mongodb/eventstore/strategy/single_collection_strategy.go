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

type SingleCollectionStrategy struct {
	client     *mongo.Client
	database   *mongo.Database
	collection *mongo.Collection
	log        *slog.Logger
}

func NewSingleCollectionStrategy(client *mongo.Client, database, collection string) (*SingleCollectionStrategy, error) {
	if client == nil {
		return nil, fmt.Errorf("client is required")
	} else if database == "" {
		return nil, fmt.Errorf("database is required")
	} else if collection == "" {
		return nil, fmt.Errorf("collection is required")
	}

	db := client.Database(database)
	coll := db.Collection(collection)

	return &SingleCollectionStrategy{
		client:     client,
		database:   db,
		collection: coll,
		log:        slog.Default().WithGroup("eventstore"),
	}, nil
}

func (s *SingleCollectionStrategy) GetStreamIterator(ctx context.Context, streamID typeid.AnyID) (estoria.EventStreamIterator, error) {
	findOpts := options.Find().SetSort(bson.D{{Key: "timestamp", Value: 1}})
	cursor, err := s.collection.Find(ctx, bson.M{"stream_id": streamID.Suffix()}, findOpts)
	if err != nil {
		return nil, fmt.Errorf("finding events: %w", err)
	}

	return &streamIterator[singleCollectionEventDocument]{
		streamID: streamID,
		cursor:   cursor,
	}, nil
}

func (s *SingleCollectionStrategy) InsertStreamEvents(ctx context.Context, streamID typeid.AnyID, events []estoria.Event) (*mongo.InsertManyResult, error) {
	docs := make([]any, len(events))
	for i, event := range events {
		docs[i] = singleCollectionEventDocumentFromEvent(event)
	}

	result, err := s.collection.InsertMany(ctx, docs)
	if err != nil {
		return nil, fmt.Errorf("inserting events: %w", err)
	}

	return result, nil
}

type singleCollectionEventDocument struct {
	StreamType string    `bson:"stream_type"`
	StreamID   string    `bson:"stream_id"`
	EventID    string    `bson:"event_id"`
	EventType  string    `bson:"event_type"`
	Timestamp  time.Time `bson:"timestamp"`
	Data       []byte    `bson:"data"`
}

func singleCollectionEventDocumentFromEvent(evt estoria.Event) singleCollectionEventDocument {
	return singleCollectionEventDocument{
		StreamType: evt.StreamID().Prefix(),
		StreamID:   evt.StreamID().Suffix(),
		EventID:    evt.ID().Suffix(),
		EventType:  evt.ID().Prefix(),
		Timestamp:  evt.Timestamp(),
		Data:       evt.Data(),
	}
}

func (d singleCollectionEventDocument) ToEvent(_ typeid.AnyID) (estoria.Event, error) {
	eventID, err := typeid.From(d.EventType, d.EventID)
	if err != nil {
		return nil, fmt.Errorf("parsing event ID: %w", err)
	}

	streamID, err := typeid.From(d.StreamType, d.StreamID)
	if err != nil {
		return nil, fmt.Errorf("parsing stream ID: %w", err)
	}

	return &event{
		id:        eventID,
		streamID:  streamID,
		timestamp: d.Timestamp,
		data:      d.Data,
	}, nil
}
