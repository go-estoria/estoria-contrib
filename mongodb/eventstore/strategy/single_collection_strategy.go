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

func (s *SingleCollectionStrategy) GetStreamCursor(ctx context.Context, streamID typeid.AnyID) (*mongo.Cursor, error) {
	findOpts := options.Find().SetSort(bson.D{{Key: "timestamp", Value: 1}})
	cursor, err := s.collection.Find(ctx, bson.M{"stream_id": streamID.Suffix()}, findOpts)
	if err != nil {
		return nil, fmt.Errorf("finding events: %w", err)
	}

	return cursor, nil
}

func (s *SingleCollectionStrategy) InsertStreamDocuments(ctx context.Context, docs []any) (*mongo.InsertManyResult, error) {
	result, err := s.collection.InsertMany(ctx, docs)
	if err != nil {
		return nil, fmt.Errorf("inserting events: %w", err)
	}

	return result, nil
}
