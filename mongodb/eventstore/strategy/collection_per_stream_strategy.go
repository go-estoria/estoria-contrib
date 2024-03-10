package strategy

import (
	"context"
	"fmt"
	"log/slog"

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

func (s *CollectionPerStreamStrategy) GetStreamCursor(ctx context.Context, streamID typeid.AnyID) (*mongo.Cursor, error) {
	collection := s.database.Collection(streamID.String())
	findOpts := options.Find().SetSort(bson.D{{Key: "timestamp", Value: 1}})
	cursor, err := collection.Find(ctx, bson.D{}, findOpts)
	if err != nil {
		return nil, fmt.Errorf("finding events: %w", err)
	}

	return cursor, nil
}

func (s *CollectionPerStreamStrategy) InsertStreamDocuments(ctx context.Context, streamID typeid.AnyID, docs []any) (*mongo.InsertManyResult, error) {
	collection := s.database.Collection(streamID.String())
	result, err := collection.InsertMany(ctx, docs)
	if err != nil {
		return nil, fmt.Errorf("inserting events: %w", err)
	}

	return result, nil
}
