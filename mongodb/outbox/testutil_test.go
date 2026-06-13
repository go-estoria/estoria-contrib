package outbox_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	mongoeventstore "github.com/go-estoria/estoria-contrib/mongodb/eventstore"
	mongooutbox "github.com/go-estoria/estoria-contrib/mongodb/outbox"
	es "github.com/go-estoria/estoria/eventstore"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const (
	outboxCollName    = "outbox"
	outboxStreamsName = "outbox_streams"
)

func createMongoDBContainer(t *testing.T, ctx context.Context) *mongo.Client {
	t.Helper()

	mongodbContainer, err := mongodb.Run(ctx, "mongo:7", mongodb.WithReplicaSet("rs0"))
	if err != nil {
		t.Fatalf("starting MongoDB container: %v", err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(mongodbContainer); err != nil {
			t.Fatalf("failed to terminate MongoDB container: %v", err)
		}
	})

	connStr, err := mongodbContainer.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("failed to get MongoDB connection string: %v", err)
	}

	client, err := mongo.Connect(options.Client().ApplyURI(connStr).SetReplicaSet("rs0").SetDirect(true))
	if err != nil {
		t.Fatalf("failed to create MongoDB client: %v", err)
	}
	if err := client.Ping(ctx, nil); err != nil {
		t.Fatalf("failed to ping MongoDB: %v", err)
	}

	return client
}

func testDatabaseName(t *testing.T) string {
	t.Helper()
	name := strings.NewReplacer("/", "_", " ", "_", ".", "_", "$", "_").Replace(t.Name())
	if len(name) > 60 {
		name = name[:60]
	}
	return name
}

// harness wires an event store and an outbox over the same uniquely-named database.
type harness struct {
	db         *mongo.Database
	store      *mongoeventstore.EventStore
	outbox     *mongooutbox.Outbox
	outboxColl *mongo.Collection
	streamColl *mongo.Collection
}

// newHarness builds an event store with the outbox registered as a transaction hook. extraHooks are
// registered AFTER the outbox (use a failing hook to exercise producer rollback).
func newHarness(t *testing.T, ctx context.Context, client *mongo.Client, handler mongooutbox.ItemHandler, outboxOpts []mongooutbox.Option, extraHooks ...mongoeventstore.TransactionHook) *harness {
	t.Helper()

	dbName := testDatabaseName(t)
	db := client.Database(dbName)
	t.Cleanup(func() {
		if err := db.Drop(ctx); err != nil {
			t.Fatalf("tc cleanup: failed to drop database %q: %v", dbName, err)
		}
	})

	outboxColl := db.Collection(outboxCollName)
	streamColl := db.Collection(outboxStreamsName)

	ob, err := mongooutbox.New(outboxColl, streamColl, handler, outboxOpts...)
	if err != nil {
		t.Fatalf("creating outbox: %v", err)
	}

	storeOpts := []mongoeventstore.EventStoreOption{
		mongoeventstore.WithDatabaseName(dbName),
		mongoeventstore.WithTransactionHook(ob),
	}
	for _, h := range extraHooks {
		storeOpts = append(storeOpts, mongoeventstore.WithTransactionHook(h))
	}

	store, err := mongoeventstore.New(client, storeOpts...)
	if err != nil {
		t.Fatalf("creating event store: %v", err)
	}

	if err := store.EnsureIndexes(ctx); err != nil {
		t.Fatalf("ensuring event store indexes: %v", err)
	}
	if err := ob.EnsureIndexes(ctx); err != nil {
		t.Fatalf("ensuring outbox indexes: %v", err)
	}

	return &harness{
		db:         db,
		store:      store,
		outbox:     ob,
		outboxColl: outboxColl,
		streamColl: streamColl,
	}
}

func (h *harness) countOutbox(t *testing.T, ctx context.Context) int64 {
	t.Helper()
	count, err := h.outboxColl.CountDocuments(ctx, bson.D{})
	if err != nil {
		t.Fatalf("counting outbox items: %v", err)
	}
	return count
}

func writableEvents(eventType string, n int) []*es.WritableEvent {
	events := make([]*es.WritableEvent, n)
	for i := range events {
		events[i] = &es.WritableEvent{
			Type: eventType,
			Data: fmt.Appendf(nil, `{"index":%d}`, i+1),
		}
	}
	return events
}

// collectingHandler appends each received item to a shared slice under mu.
func collectingHandler(mu *sync.Mutex, items *[]*mongooutbox.Item) mongooutbox.ItemHandler {
	return func(_ context.Context, item *mongooutbox.Item) error {
		mu.Lock()
		defer mu.Unlock()
		*items = append(*items, item)
		return nil
	}
}
