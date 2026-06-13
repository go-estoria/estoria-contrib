package eventstore_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/go-estoria/estoria-contrib/mongodb/eventstore"
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

// testDatabaseName derives a MongoDB-legal database name unique to the given test, so that
// parallel tests sharing one container do not collide.
func testDatabaseName(t *testing.T) string {
	t.Helper()
	name := strings.NewReplacer("/", "_", " ", "_", ".", "_", "$", "_").Replace(t.Name())
	if len(name) > 60 {
		name = name[:60]
	}
	return name
}

// newTestStore creates an EventStore backed by a database unique to the test, ensures its indexes,
// and registers cleanup that drops the database.
func newTestStore(t *testing.T, ctx context.Context, client *mongo.Client, opts ...eventstore.EventStoreOption) *eventstore.EventStore {
	t.Helper()

	dbName := testDatabaseName(t)
	t.Cleanup(func() {
		if err := client.Database(dbName).Drop(ctx); err != nil {
			t.Fatalf("tc cleanup: failed to drop database %q: %v", dbName, err)
		}
	})

	opts = append([]eventstore.EventStoreOption{eventstore.WithDatabaseName(dbName)}, opts...)
	store, err := eventstore.New(client, opts...)
	if err != nil {
		t.Fatalf("tc setup: failed to create EventStore: %v", err)
	}

	if err := store.EnsureIndexes(ctx); err != nil {
		t.Fatalf("tc setup: failed to ensure indexes: %v", err)
	}

	return store
}
