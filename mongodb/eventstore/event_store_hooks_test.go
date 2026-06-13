package eventstore_test

import (
	"context"
	"errors"
	"testing"

	"github.com/go-estoria/estoria-contrib/mongodb/eventstore"
	es "github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// newHookTestStore builds a store on a unique database and returns it together with the database
// handle (so a hook can write marker documents in the append transaction).
func newHookTestStore(t *testing.T, ctx context.Context, client *mongo.Client, hooks ...eventstore.TransactionHook) (*eventstore.EventStore, *mongo.Database) {
	t.Helper()

	dbName := testDatabaseName(t)
	t.Cleanup(func() {
		if err := client.Database(dbName).Drop(ctx); err != nil {
			t.Fatalf("tc cleanup: failed to drop database %q: %v", dbName, err)
		}
	})

	opts := []eventstore.EventStoreOption{eventstore.WithDatabaseName(dbName)}
	for _, h := range hooks {
		opts = append(opts, eventstore.WithTransactionHook(h))
	}

	store, err := eventstore.New(client, opts...)
	if err != nil {
		t.Fatalf("tc setup: failed to create EventStore: %v", err)
	}
	if err := store.EnsureIndexes(ctx); err != nil {
		t.Fatalf("tc setup: failed to ensure indexes: %v", err)
	}

	return store, client.Database(dbName)
}

func countMarkers(t *testing.T, ctx context.Context, db *mongo.Database) int64 {
	t.Helper()
	count, err := db.Collection("markers").CountDocuments(ctx, bson.D{})
	if err != nil {
		t.Fatalf("counting markers: %v", err)
	}
	return count
}

func TestEventStore_Integration_HookCommitsWithAppend(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Parallel()

	ctx := context.Background()

	mongoClient, err := createMongoDBContainer(t, ctx)
	if err != nil {
		t.Fatalf("failed to create MongoDB container: %v", err)
	}

	var db *mongo.Database
	marker := eventstore.TransactionHookFunc(func(sessCtx context.Context, events []*es.Event) error {
		_, err := db.Collection("markers").InsertOne(sessCtx, bson.D{{Key: "count", Value: len(events)}})
		return err
	})

	store, database := newHookTestStore(t, ctx, mongoClient, marker)
	db = database

	streamID := typeid.NewV4("hooktype")
	if err := store.AppendStream(ctx, streamID, writableEvents("evt", 3), es.AppendStreamOptions{}); err != nil {
		t.Fatalf("appending: %v", err)
	}

	if got := countMarkers(t, ctx, db); got != 1 {
		t.Fatalf("expected 1 marker after a committed append, got %d", got)
	}
}

func TestEventStore_Integration_HookErrorAbortsAppend(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Parallel()

	ctx := context.Background()

	mongoClient, err := createMongoDBContainer(t, ctx)
	if err != nil {
		t.Fatalf("failed to create MongoDB container: %v", err)
	}

	var db *mongo.Database
	// The first hook writes a marker; the second fails. Because hooks run in-transaction, the
	// failure must roll back both the events and the marker written by the first hook.
	writeMarker := eventstore.TransactionHookFunc(func(sessCtx context.Context, events []*es.Event) error {
		_, err := db.Collection("markers").InsertOne(sessCtx, bson.D{{Key: "count", Value: len(events)}})
		return err
	})
	wantErr := errors.New("boom")
	failHook := eventstore.TransactionHookFunc(func(sessCtx context.Context, events []*es.Event) error {
		return wantErr
	})

	store, database := newHookTestStore(t, ctx, mongoClient, writeMarker, failHook)
	db = database

	streamID := typeid.NewV4("hooktype")
	err = store.AppendStream(ctx, streamID, writableEvents("evt", 3), es.AppendStreamOptions{})
	if err == nil {
		t.Fatalf("expected an error, got nil")
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected error to wrap %v, got %v", wantErr, err)
	}

	// No marker should survive the rollback.
	if got := countMarkers(t, ctx, db); got != 0 {
		t.Errorf("expected 0 markers after an aborted append, got %d", got)
	}

	// No events should survive the rollback.
	iter, err := store.ReadStream(ctx, streamID, es.ReadStreamOptions{})
	if err != nil {
		t.Fatalf("reading: %v", err)
	}
	events := drain(t, ctx, iter)
	if len(events) != 0 {
		t.Errorf("expected 0 events after an aborted append, got %d", len(events))
	}
}

func TestEventStore_Integration_HookReceivesPositions(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Parallel()

	ctx := context.Background()

	mongoClient, err := createMongoDBContainer(t, ctx)
	if err != nil {
		t.Fatalf("failed to create MongoDB container: %v", err)
	}

	type received struct {
		version int64
		global  int64
	}
	var got []received
	capture := eventstore.TransactionHookFunc(func(sessCtx context.Context, events []*es.Event) error {
		for _, e := range events {
			if e.GlobalPosition == nil {
				t.Errorf("hook received event with nil global position")
				continue
			}
			got = append(got, received{version: e.StreamVersion, global: *e.GlobalPosition})
		}
		return nil
	})

	store, _ := newHookTestStore(t, ctx, mongoClient, capture)

	streamID := typeid.NewV4("hooktype")
	if err := store.AppendStream(ctx, streamID, writableEvents("evt", 3), es.AppendStreamOptions{}); err != nil {
		t.Fatalf("appending: %v", err)
	}

	if len(got) != 3 {
		t.Fatalf("expected hook to receive 3 events, got %d", len(got))
	}
	for i, r := range got {
		if r.version != int64(i+1) {
			t.Errorf("event %d: expected version %d, got %d", i, i+1, r.version)
		}
		if r.global != int64(i+1) {
			t.Errorf("event %d: expected global %d, got %d", i, i+1, r.global)
		}
	}
}
