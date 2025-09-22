package strategy

import (
	"context"
	"fmt"

	"github.com/go-estoria/estoria/eventstore"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

type (
	// MongoDatabase provides an API for obtaining a collection handle.
	MongoDatabase interface {
		Collection(string, ...options.Lister[options.CollectionOptions]) *mongo.Collection
		ListCollectionNames(ctx context.Context, filter any, opts ...options.Lister[options.ListCollectionsOptions]) ([]string, error)
	}

	// MongoCollection provides an API for querying and inserting documents into a MongoDB collection.
	MongoCollection interface {
		Aggregate(context.Context, any, ...options.Lister[options.AggregateOptions]) (*mongo.Cursor, error)
		Find(context.Context, any, ...options.Lister[options.FindOptions]) (*mongo.Cursor, error)
		FindOne(context.Context, any, ...options.Lister[options.FindOneOptions]) *mongo.SingleResult
		InsertMany(context.Context, any, ...options.Lister[options.InsertManyOptions]) (*mongo.InsertManyResult, error)
	}

	MongoSessionStarter interface {
		StartSession(opts ...options.Lister[options.SessionOptions]) (*mongo.Session, error)
	}
)

type Offsets struct {
	Offset       int64 `bson:"offset"`
	GlobalOffset int64 `bson:"global_offset"`
}

// An InsertStreamEventsResult contains the result of inserting events into a stream.
type InsertStreamEventsResult struct {
	MongoResult *mongo.InsertManyResult
}

// DefaultSessionOptions returns the default session options used by the event store
// when starting a new MongoDB session.
func DefaultSessionOptions() *options.SessionOptionsBuilder {
	return options.Session()
}

// DefaultTransactionOptions returns the default transaction options used by the event store
// when starting a new MongoDB transaction on a session.
func DefaultTransactionOptions() *options.TransactionOptionsBuilder {
	return options.Transaction().SetReadPreference(readpref.Primary())
}

func findOptsFromReadStreamOptions(opts eventstore.ReadStreamOptions, offsetKey string) options.Lister[options.FindOptions] {
	findOpts := options.Find()
	if opts.Direction == eventstore.Reverse {
		findOpts.SetSort(bson.D{{Key: offsetKey, Value: -1}})
	} else {
		findOpts.SetSort(bson.D{{Key: offsetKey, Value: 1}})
	}

	if opts.Offset > 0 {
		findOpts.SetSkip(opts.Offset)
	}

	if opts.Count > 0 {
		findOpts.SetLimit(opts.Count)
	}

	return findOpts
}

func getListStreamsCursor(ctx context.Context, collection MongoCollection) (*mongo.Cursor, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$sort", Value: bson.D{
			{Key: "stream_id", Value: 1}, // Group documents together by stream_id.
			{Key: "offset", Value: -1},   // Highest offset comes first within each stream.
		}}},
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$stream_id"}, // Group key is stream_id.
			{Key: "stream_type", Value: bson.D{{Key: "$first", Value: "$stream_type"}}},
			{Key: "offset", Value: bson.D{{Key: "$first", Value: "$offset"}}},
			{Key: "global_offset", Value: bson.D{{Key: "$first", Value: "$global_offset"}}},
		}}},
	}

	cursor, err := collection.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("aggregating collection: %w", err)
	}

	return cursor, nil
}
