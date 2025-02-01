package strategy

import (
	"context"

	"github.com/go-estoria/estoria/eventstore"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type (
	// MongoClient provides an API for obtaining a database handle.
	MongoClient interface {
		Database(name string, opts ...*options.DatabaseOptions) *mongo.Database
	}

	// MongoDatabase provides an API for obtaining a collection handle.
	MongoDatabase interface {
		Collection(name string, opts ...*options.CollectionOptions) *mongo.Collection
	}

	// MongoCollection provides an API for querying and inserting documents into a MongoDB collection.
	MongoCollection interface {
		Aggregate(ctx context.Context, pipeline any, opts ...*options.AggregateOptions) (*mongo.Cursor, error)
		Find(ctx context.Context, filter any, opts ...*options.FindOptions) (cur *mongo.Cursor, err error)
		FindOne(ctx context.Context, filter any, opts ...*options.FindOneOptions) *mongo.SingleResult
		InsertMany(ctx context.Context, documents []any, opts ...*options.InsertManyOptions) (*mongo.InsertManyResult, error)
	}

	// MongoCursor provides an API for iterating over a set of documents returned by a query.
	MongoCursor interface {
		Next(ctx context.Context) bool
		Decode(v any) error
		Err() error
		Close(ctx context.Context) error
	}
)

// An InsertStreamEventsResult contains the result of inserting events into a stream.
type InsertStreamEventsResult struct {
	MongoResult    *mongo.InsertManyResult
	InsertedEvents []*Event
}

func findOptsFromReadStreamOptions(opts eventstore.ReadStreamOptions, offsetKey string) *options.FindOptions {
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
