package eventstore

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

func NewDefaultMongoDBClient(ctx context.Context, uri string) (*mongo.Client, error) {
	options := options.Client()
	options.ApplyURI(uri)
	options.ReadConcern = readconcern.Majority()
	options.WriteConcern = writeconcern.Majority()

	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	return mongo.Connect(ctx, options)
}
