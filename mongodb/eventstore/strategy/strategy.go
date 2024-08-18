package strategy

import (
	"context"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoClient interface {
	Database(name string, opts ...*options.DatabaseOptions) *mongo.Database
}

type MongoDatabase interface {
	Collection(name string, opts ...*options.CollectionOptions) *mongo.Collection
}

type MongoCollection interface {
	Find(ctx context.Context, filter any, opts ...*options.FindOptions) (cur *mongo.Cursor, err error)
	FindOne(ctx context.Context, filter any, opts ...*options.FindOneOptions) *mongo.SingleResult
	InsertMany(ctx context.Context, documents []any, opts ...*options.InsertManyOptions) (*mongo.InsertManyResult, error)
}

type MongoCursor interface {
	Next(ctx context.Context) bool
	Decode(v any) error
	Err() error
	Close(ctx context.Context) error
}
