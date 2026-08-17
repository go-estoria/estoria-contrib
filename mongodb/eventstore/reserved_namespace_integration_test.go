package eventstore_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/go-estoria/estoria-contrib/mongodb/eventstore"
	"github.com/go-estoria/estoria-contrib/mongodb/eventstore/strategy"
	coreeventstore "github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Underscore-prefixed collections are reserved for infrastructure sharing the database
// (the streams collection, an outbox): the multi-collection strategy must neither sweep
// them into global reads or stream listings, nor let a selector write events into them.
func TestEventStore_Integration_ReservedUnderscoreNamespace(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Parallel()

	ctx := t.Context()

	mongoClient, err := createMongoDBContainer(t)
	if err != nil {
		t.Fatalf("failed to create MongoDB container: %v", err)
	}

	t.Run("enumeration ignores underscore collections", func(t *testing.T) {
		db := mongoClient.Database("estoria_reserved_enum")
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

		// An infrastructure collection holding a document shaped enough like an event to
		// be swept into a global read if enumeration failed to exclude it.
		if _, err := db.Collection("_outbox").InsertOne(ctx, bson.D{
			{Key: "stream_type", Value: "junk"},
			{Key: "stream_id", Value: "00000000-0000-0000-0000-000000000000"},
			{Key: "offset", Value: int64(1)},
			{Key: "global_offset", Value: int64(999)},
		}); err != nil {
			t.Fatalf("tc setup: failed to insert infrastructure document: %v", err)
		}

		iter, err := store.ReadAll(ctx, coreeventstore.ReadAllOptions{})
		if err != nil {
			t.Fatalf("ReadAll: %v", err)
		}
		defer iter.Close(ctx)

		read := 0
		for {
			event, err := iter.Next(ctx)
			if errors.Is(err, coreeventstore.ErrEndOfEventStream) {
				break
			}
			if err != nil {
				t.Fatalf("reading event: %v", err)
			}
			read++
			if event.StreamID.Type == "junk" {
				t.Fatalf("global read yielded a document from the reserved namespace: %+v", event)
			}
		}
		if read != len(streamIDs) {
			t.Errorf("expected %d events from global read, got %d", len(streamIDs), read)
		}

		streams, err := store.ListStreams(ctx)
		if err != nil {
			t.Fatalf("ListStreams: %v", err)
		}
		if len(streams) != len(streamIDs) {
			t.Errorf("expected %d streams listed, got %d: %v", len(streamIDs), len(streams), streams)
		}
	})

	t.Run("selector may not choose reserved names", func(t *testing.T) {
		db := mongoClient.Database("estoria_reserved_selector")
		t.Cleanup(func() {
			if err := db.Drop(context.WithoutCancel(ctx)); err != nil {
				t.Logf("tc cleanup: failed to drop database: %v", err)
			}
		})

		strat, err := strategy.NewMultiCollectionStrategy(db,
			strategy.CollectionSelectorFunc(func(typeid.ID) string { return "_outbox" }))
		if err != nil {
			t.Fatalf("tc setup: failed to create MultiCollectionStrategy: %v", err)
		}

		store, err := eventstore.New(mongoClient, eventstore.WithStrategy(strat))
		if err != nil {
			t.Fatalf("tc setup: failed to create EventStore: %v", err)
		}

		streamID := typeid.NewV4("user")
		if _, err := store.AppendStream(ctx, streamID, []*coreeventstore.WritableEvent{
			{Type: "testevent", Data: []byte(`{}`)},
		}, coreeventstore.AppendStreamOptions{}); err == nil || !strings.Contains(err.Error(), "reserved") {
			t.Errorf("expected append rejecting reserved collection name, got %v", err)
		}

		if err := store.DeleteStream(ctx, streamID, coreeventstore.DeleteStreamOptions{}); err == nil || !strings.Contains(err.Error(), "reserved") {
			t.Errorf("expected delete rejecting reserved collection name, got %v", err)
		}
	})
}
