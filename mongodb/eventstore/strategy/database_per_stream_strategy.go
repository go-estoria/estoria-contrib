package strategy

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/go-estoria/estoria"
	"github.com/go-estoria/estoria/typeid"
	"github.com/gofrs/uuid/v5"
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

func (s *DatabasePerStreamStrategy) GetStreamIterator(
	ctx context.Context,
	streamID typeid.UUID,
	opts estoria.ReadStreamOptions,
) (estoria.EventStreamIterator, error) {
	database := s.client.Database(streamID.String())
	collection := database.Collection(s.collectionName)

	offset := opts.Offset
	count := opts.Count
	sortDirection := 1
	versionFilterKey := "$gt"
	if opts.Direction == estoria.Reverse {
		sortDirection = -1
		// versionFilterKey = "$lt"
	}

	findOpts := options.Find().SetSort(bson.D{{Key: "event_id", Value: sortDirection}})
	if count > 0 {
		findOpts = findOpts.SetLimit(count)
	}

	cursor, err := collection.Find(ctx, bson.D{
		{Key: "version", Value: bson.D{{Key: versionFilterKey, Value: offset}}},
	}, findOpts)
	if err != nil {
		return nil, fmt.Errorf("finding events: %w", err)
	}

	return &streamIterator[databasePerStreamEventDocument]{
		streamID: streamID,
		cursor:   cursor,
	}, nil
}

func (s *DatabasePerStreamStrategy) InsertStreamEvents(
	ctx mongo.SessionContext,
	streamID typeid.UUID,
	events []estoria.EventStoreEvent,
	opts estoria.AppendStreamOptions,
) (*mongo.InsertManyResult, error) {
	latestVersion, err := s.getLatestVersion(ctx, streamID)
	if err != nil {
		return nil, fmt.Errorf("getting latest version: %w", err)
	}

	if opts.ExpectVersion > 0 && latestVersion != opts.ExpectVersion {
		return nil, fmt.Errorf("expected version %d, but stream has version %d", opts.ExpectVersion, latestVersion)
	}

	docs := make([]any, len(events))
	for i, event := range events {
		docs[i] = databasePerStreamEventDocumentFromEvent(event, latestVersion+int64(i+1))
	}

	database := s.client.Database(streamID.String())
	collection := database.Collection(s.collectionName)
	result, err := collection.InsertMany(ctx, docs)
	if err != nil {
		return nil, fmt.Errorf("inserting events: %w", err)
	}

	return result, nil
}

func (s *DatabasePerStreamStrategy) getLatestVersion(ctx context.Context, streamID typeid.UUID) (int64, error) {
	database := s.client.Database(streamID.String())
	collection := database.Collection(s.collectionName)

	opts := options.FindOne().SetSort(bson.D{{Key: "version", Value: -1}})
	var doc databasePerStreamEventDocument
	if err := collection.FindOne(ctx, bson.D{}, opts).Decode(&doc); err != nil {
		if err == mongo.ErrNoDocuments {
			return 0, nil
		}

		return 0, fmt.Errorf("finding latest version: %w", err)
	}

	return doc.Version, nil
}

type databasePerStreamEventDocument struct {
	EventID   uuid.UUID `bson:"event_id"`
	EventType string    `bson:"event_type"`
	Timestamp time.Time `bson:"timestamp"`
	Version   int64     `bson:"version"`
	Data      []byte    `bson:"data"`
}

func databasePerStreamEventDocumentFromEvent(evt estoria.EventStoreEvent, version int64) databasePerStreamEventDocument {
	return databasePerStreamEventDocument{
		EventID:   evt.ID().UUID(),
		EventType: evt.ID().TypeName(),
		Timestamp: evt.Timestamp(),
		Version:   version,
		Data:      evt.Data(),
	}
}

func (d databasePerStreamEventDocument) ToEvent(streamID typeid.UUID) (estoria.EventStoreEvent, error) {
	return &event{
		id:        typeid.FromUUID(d.EventType, d.EventID),
		streamID:  streamID,
		timestamp: d.Timestamp,
		data:      d.Data,
	}, nil
}
