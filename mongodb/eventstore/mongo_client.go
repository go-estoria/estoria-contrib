package eventstore

import (
	"context"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

func NewDefaultMongoDBClient(ctx context.Context, appName, uri string) (*mongo.Client, error) {
	options := options.Client()
	options.ApplyURI(uri)
	options.SetAppName(appName)
	options.SetReadConcern(readconcern.Majority())
	options.SetReadPreference(readpref.Primary())
	options.SetWriteConcern(writeconcern.Majority())
	options.SetRetryWrites(true)

	return mongo.Connect(ctx, options)
}
