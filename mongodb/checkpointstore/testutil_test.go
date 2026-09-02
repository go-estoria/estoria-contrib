package checkpointstore_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func createMongoDBContainer(t *testing.T) (*mongo.Client, error) {
	t.Helper()

	connStr, err := startMongoDBContainer(t)
	if err != nil {
		return nil, err
	}

	return connectMongoDBClient(t, connStr, nil)
}

func startMongoDBContainer(t *testing.T) (string, error) {
	t.Helper()

	ctx := t.Context()

	mongodbContainer, err := mongodb.Run(ctx, "mongo:7", mongodb.WithReplicaSet("rs0"), waitForWritablePrimary)
	if err != nil {
		return "", fmt.Errorf("starting MongoDB container: %w", err)
	}

	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(mongodbContainer); err != nil {
			t.Fatalf("failed to terminate MongoDB container: %v", err)
		}
	})

	connStr, err := mongodbContainer.ConnectionString(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get MongoDB connection string: %w", err)
	}

	return connStr, nil
}

func connectMongoDBClient(t *testing.T, connStr string, monitor *event.CommandMonitor) (*mongo.Client, error) {
	t.Helper()

	opts := options.Client().
		ApplyURI(connStr).
		SetReplicaSet("rs0").
		SetDirect(true)
	if monitor != nil {
		opts = opts.SetMonitor(monitor)
	}

	mongoClient, err := mongo.Connect(opts)
	if err != nil {
		t.Fatalf("failed to create MongoDB client: %v", err)
	}

	if err := mongoClient.Ping(t.Context(), nil); err != nil {
		return nil, fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	return mongoClient, nil
}

func testDatabaseName(t *testing.T) string {
	t.Helper()
	name := strings.NewReplacer("/", "_", " ", "_", ".", "_", "$", "_").Replace(t.Name())
	if len(name) > 60 {
		name = name[:60]
	}
	return name
}

// waitForWritablePrimary holds the container until the single-node replica set has
// elected its primary; see the eventstore package's harness for the rationale.
var waitForWritablePrimary = testcontainers.WithAdditionalWaitStrategy(
	wait.ForExec([]string{"mongosh", "--quiet", "--eval", "quit(db.hello().isWritablePrimary ? 0 : 1)"}),
)
