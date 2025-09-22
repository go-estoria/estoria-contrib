package strategy_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func createMongoDBContainer(t *testing.T, ctx context.Context) (*mongo.Client, error) {
	t.Helper()

	mongodbContainer, err := mongodb.Run(ctx, "mongo:7", mongodb.WithReplicaSet("rs0"))
	if err != nil {
		return nil, fmt.Errorf("starting MongoDB container: %w", err)
	}

	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(mongodbContainer); err != nil {
			t.Fatalf("failed to terminate MongoDB container: %v", err)
		}
	})

	connStr, err := mongodbContainer.ConnectionString(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get MongoDB connection string: %w", err)
	}

	t.Log("MongoDB container connection string:", connStr)

	mongoClient, err := mongo.Connect(options.Client().
		ApplyURI(connStr).
		SetReplicaSet("rs0").
		SetDirect(true),
	)
	if err != nil {
		t.Fatalf("failed to create MongoDB client: %v", err)
	}

	t.Log("Created MongoDB client")

	if err := mongoClient.Ping(ctx, nil); err != nil {
		return nil, fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	t.Log("Successfully pinged MongoDB")

	return mongoClient, nil
}
