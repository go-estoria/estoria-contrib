package eventstore_test

import (
	"fmt"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func createMongoDBContainer(t *testing.T) (*mongo.Client, error) {
	t.Helper()

	client, _, err := createMongoDBContainerWithConnStr(t)
	return client, err
}

// createMongoDBContainerWithConnStr also returns the container's connection string, for
// tests that need a second, differently-configured client against the same server.
func createMongoDBContainerWithConnStr(t *testing.T) (*mongo.Client, string, error) {
	t.Helper()

	ctx := t.Context()

	mongodbContainer, err := mongodb.Run(ctx, "mongo:7", mongodb.WithReplicaSet("rs0"), waitForWritablePrimary)
	if err != nil {
		return nil, "", fmt.Errorf("starting MongoDB container: %w", err)
	}

	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(mongodbContainer); err != nil {
			t.Fatalf("failed to terminate MongoDB container: %v", err)
		}
	})

	connStr, err := mongodbContainer.ConnectionString(ctx)
	if err != nil {
		return nil, "", fmt.Errorf("failed to get MongoDB connection string: %w", err)
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
		return nil, "", fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	t.Log("Successfully pinged MongoDB")

	return mongoClient, connStr, nil
}

// waitForWritablePrimary holds the container until the single-node replica set has
// elected its primary. The module's own readiness check passes as soon as rs.initiate is
// acknowledged, which precedes the election, and a direct connection does not wait for one.
var waitForWritablePrimary = testcontainers.WithAdditionalWaitStrategy(
	wait.ForExec([]string{"mongosh", "--quiet", "--eval", "quit(db.hello().isWritablePrimary ? 0 : 1)"}),
)
