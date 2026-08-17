package eventstore_test

import (
	"context"
	"errors"
	"testing"

	"github.com/go-estoria/estoria-contrib/mongodb/eventstore"
	"github.com/go-estoria/estoria-contrib/mongodb/eventstore/strategy"
	coreeventstore "github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// requireEventIndexes fails the test unless the collection carries both unique event
// indexes.
func requireEventIndexes(ctx context.Context, t *testing.T, collection *mongo.Collection) {
	t.Helper()

	specs, err := collection.Indexes().ListSpecifications(ctx)
	if err != nil {
		t.Fatalf("listing indexes on %s: %v", collection.Name(), err)
	}

	unique := map[string]bool{}
	for _, spec := range specs {
		unique[spec.Name] = spec.Unique != nil && *spec.Unique
	}

	for _, name := range []string{"uniq_stream_offset", "uniq_global_offset"} {
		if isUnique, ok := unique[name]; !ok {
			t.Errorf("collection %s: index %s not found", collection.Name(), name)
		} else if !isUnique {
			t.Errorf("collection %s: index %s is not unique", collection.Name(), name)
		}
	}
}

func TestEventStore_Integration_EnsureIndexes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Parallel()

	ctx := t.Context()

	mongoClient, err := createMongoDBContainer(t)
	if err != nil {
		t.Fatalf("failed to create MongoDB container: %v", err)
	}

	t.Run("single collection strategy", func(t *testing.T) {
		db := mongoClient.Database("estoria_idx_single")
		t.Cleanup(func() {
			if err := db.Drop(context.WithoutCancel(ctx)); err != nil {
				t.Logf("tc cleanup: failed to drop database: %v", err)
			}
		})

		strat, err := strategy.NewSingleCollectionStrategy(db)
		if err != nil {
			t.Fatalf("tc setup: failed to create SingleCollectionStrategy: %v", err)
		}

		store, err := eventstore.New(mongoClient, eventstore.WithStrategy(strat))
		if err != nil {
			t.Fatalf("tc setup: failed to create EventStore: %v", err)
		}

		// Twice, pinning idempotency.
		for range 2 {
			if err := store.EnsureIndexes(ctx); err != nil {
				t.Fatalf("EnsureIndexes: %v", err)
			}
		}

		requireEventIndexes(ctx, t, db.Collection("events"))
	})

	// A multi-collection EnsureIndexes covers every event collection that exists when it
	// is called, and skips the streams collection.
	t.Run("multi collection strategy", func(t *testing.T) {
		db := mongoClient.Database("estoria_idx_multi")
		t.Cleanup(func() {
			if err := db.Drop(context.WithoutCancel(ctx)); err != nil {
				t.Logf("tc cleanup: failed to drop database: %v", err)
			}
		})

		strat, err := strategy.NewMultiCollectionStrategy(db, strategy.CollectionPerStreamID())
		if err != nil {
			t.Fatalf("tc setup: failed to create MultiCollectionStrategy: %v", err)
		}

		store, err := eventstore.New(mongoClient, eventstore.WithStrategy(strat))
		if err != nil {
			t.Fatalf("tc setup: failed to create EventStore: %v", err)
		}

		streamIDs := []typeid.ID{typeid.NewV4("user"), typeid.NewV4("order")}
		for _, streamID := range streamIDs {
			if _, err := store.AppendStream(ctx, streamID, []*coreeventstore.WritableEvent{
				{Type: "testevent", Data: []byte(`{}`)},
			}, coreeventstore.AppendStreamOptions{}); err != nil {
				t.Fatalf("tc setup: failed to append to stream %s: %v", streamID, err)
			}
		}

		if err := store.EnsureIndexes(ctx); err != nil {
			t.Fatalf("EnsureIndexes: %v", err)
		}

		for _, streamID := range streamIDs {
			requireEventIndexes(ctx, t, db.Collection(streamID.String()))
		}

		specs, err := db.Collection(strategy.DefaultStreamsCollectionName).Indexes().ListSpecifications(ctx)
		if err != nil {
			t.Fatalf("listing streams collection indexes: %v", err)
		}
		for _, spec := range specs {
			if spec.Name != "_id_" {
				t.Errorf("streams collection unexpectedly gained index %s", spec.Name)
			}
		}
	})
}

// With auto-ensure enabled, a collection created on the fly by the selector carries the
// event indexes after its first append, with no explicit EnsureIndexes call.
func TestEventStore_Integration_AutoEnsureIndexes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Parallel()

	ctx := t.Context()

	mongoClient, err := createMongoDBContainer(t)
	if err != nil {
		t.Fatalf("failed to create MongoDB container: %v", err)
	}

	db := mongoClient.Database("estoria_idx_auto")
	t.Cleanup(func() {
		if err := db.Drop(context.WithoutCancel(ctx)); err != nil {
			t.Logf("tc cleanup: failed to drop database: %v", err)
		}
	})

	strat, err := strategy.NewMultiCollectionStrategy(db, strategy.CollectionPerStreamID(),
		strategy.WithAutoEnsureIndexes())
	if err != nil {
		t.Fatalf("tc setup: failed to create MultiCollectionStrategy: %v", err)
	}

	store, err := eventstore.New(mongoClient, eventstore.WithStrategy(strat))
	if err != nil {
		t.Fatalf("tc setup: failed to create EventStore: %v", err)
	}

	streamIDs := []typeid.ID{typeid.NewV4("user"), typeid.NewV4("order")}
	for _, streamID := range streamIDs {
		if _, err := store.AppendStream(ctx, streamID, []*coreeventstore.WritableEvent{
			{Type: "testevent", Data: []byte(`{}`)},
		}, coreeventstore.AppendStreamOptions{}); err != nil {
			t.Fatalf("failed to append to stream %s: %v", streamID, err)
		}
	}

	for _, streamID := range streamIDs {
		requireEventIndexes(ctx, t, db.Collection(streamID.String()))
	}
}

// The unique (stream_type, stream_id, offset) index is the backstop for offset
// reservation: when a stored event already occupies a reserved offset — possible only
// with documents the counters never accounted for, such as an un-backfilled legacy
// dataset — the append must fail with a version mismatch and roll back its reservations.
func TestEventStore_Integration_DuplicateOffsetBackstop(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Parallel()

	ctx := t.Context()

	mongoClient, err := createMongoDBContainer(t)
	if err != nil {
		t.Fatalf("failed to create MongoDB container: %v", err)
	}

	db := mongoClient.Database("estoria_idx_backstop")
	t.Cleanup(func() {
		if err := db.Drop(context.WithoutCancel(ctx)); err != nil {
			t.Logf("tc cleanup: failed to drop database: %v", err)
		}
	})

	strat, err := strategy.NewSingleCollectionStrategy(db)
	if err != nil {
		t.Fatalf("tc setup: failed to create SingleCollectionStrategy: %v", err)
	}

	store, err := eventstore.New(mongoClient, eventstore.WithStrategy(strat))
	if err != nil {
		t.Fatalf("tc setup: failed to create EventStore: %v", err)
	}

	if err := store.EnsureIndexes(ctx); err != nil {
		t.Fatalf("EnsureIndexes: %v", err)
	}

	streamID := typeid.NewV4("user")
	if _, err := store.AppendStream(ctx, streamID, []*coreeventstore.WritableEvent{
		{Type: "testevent", Data: []byte(`{}`)},
	}, coreeventstore.AppendStreamOptions{}); err != nil {
		t.Fatalf("failed to append: %v", err)
	}

	// A document at offset 2 that the stream counter knows nothing about.
	forged := bson.D{
		{Key: "stream_type", Value: streamID.Type},
		{Key: "stream_id", Value: streamID.UUID.String()},
		{Key: "offset", Value: int64(2)},
		{Key: "global_offset", Value: int64(12345)},
	}
	if _, err := db.Collection("events").InsertOne(ctx, forged); err != nil {
		t.Fatalf("inserting forged document: %v", err)
	}

	// The counter hands out offset 2, the insert collides with the forged document, and
	// the failure surfaces as a version mismatch rather than a raw driver error.
	_, err = store.AppendStream(ctx, streamID, []*coreeventstore.WritableEvent{
		{Type: "testevent", Data: []byte(`{}`)},
	}, coreeventstore.AppendStreamOptions{})
	var mismatchErr coreeventstore.StreamVersionMismatchError
	if !errors.As(err, &mismatchErr) {
		t.Fatalf("expected StreamVersionMismatchError, got %v", err)
	}

	// The failed append's counter reservations must have rolled back: with the forged
	// document out of the way, the next append lands at version 2, not 3.
	if _, err := db.Collection("events").DeleteOne(ctx, bson.D{{Key: "global_offset", Value: int64(12345)}}); err != nil {
		t.Fatalf("deleting forged document: %v", err)
	}

	written, err := store.AppendStream(ctx, streamID, []*coreeventstore.WritableEvent{
		{Type: "testevent", Data: []byte(`{}`)},
	}, coreeventstore.AppendStreamOptions{})
	if err != nil {
		t.Fatalf("appending after removing forged document: %v", err)
	}
	if len(written) != 1 || written[0].StreamVersion != 2 {
		t.Fatalf("expected recovery append at version 2, got %+v", written)
	}
}
