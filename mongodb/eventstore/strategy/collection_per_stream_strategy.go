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

type CollectionPerStreamStrategy struct {
	client   *mongo.Client
	database *mongo.Database
	log      *slog.Logger
}

func NewCollectionPerStreamStrategy(client *mongo.Client, database string) (*CollectionPerStreamStrategy, error) {
	if client == nil {
		return nil, fmt.Errorf("client is required")
	} else if database == "" {
		return nil, fmt.Errorf("database is required")
	}

	db := client.Database(database)

	return &CollectionPerStreamStrategy{
		client:   client,
		database: db,
		log:      slog.Default().WithGroup("eventstore"),
	}, nil
}

func (s *CollectionPerStreamStrategy) GetStreamIterator(
	ctx context.Context,
	streamID typeid.AnyID,
	opts estoria.ReadStreamOptions,
) (estoria.EventStreamIterator, error) {
	collection := s.database.Collection(streamID.String())

	offset := opts.Offset
	count := opts.Count
	sortDirection := 1
	versionFilterKey := "$gt"
	if opts.Direction == estoria.Reverse {
		sortDirection = -1
		versionFilterKey = "$lt"
	}

	findOpts := options.Find().SetSort(bson.D{{Key: "aggregate_version", Value: sortDirection}}).SetLimit(count)
	cursor, err := collection.Find(ctx, bson.D{
		{Key: "aggregate_version", Value: bson.D{{Key: versionFilterKey, Value: offset}}},
	}, findOpts)
	if err != nil {
		return nil, fmt.Errorf("finding events: %w", err)
	}

	return &streamIterator[collectionPerStreamEventDocument]{
		streamID: streamID,
		cursor:   cursor,
	}, nil
}

func (s *CollectionPerStreamStrategy) InsertStreamEvents(
	ctx mongo.SessionContext,
	streamID typeid.AnyID,
	events []estoria.Event,
	_ estoria.AppendStreamOptions,
) (*mongo.InsertManyResult, error) {
	docs := make([]any, len(events))
	for i, event := range events {
		docs[i] = collectionPerStreamEventDocumentFromEvent(event)
	}

	// TODO: utilize the append stream options

	collection := s.database.Collection(streamID.String())
	result, err := collection.InsertMany(ctx, docs)
	if err != nil {
		return nil, fmt.Errorf("inserting events: %w", err)
	}

	return result, nil
}

type collectionPerStreamEventDocument struct {
	EventID   string    `bson:"event_id"`
	EventType string    `bson:"event_type"`
	Timestamp time.Time `bson:"timestamp"`
	Data      []byte    `bson:"data"`
}

func collectionPerStreamEventDocumentFromEvent(evt estoria.Event) collectionPerStreamEventDocument {
	return collectionPerStreamEventDocument{
		EventID:   evt.ID().Suffix(),
		EventType: evt.ID().Prefix(),
		Timestamp: evt.Timestamp(),
		Data:      evt.Data(),
	}
}

func (d collectionPerStreamEventDocument) ToEvent(streamID typeid.AnyID) (estoria.Event, error) {
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
