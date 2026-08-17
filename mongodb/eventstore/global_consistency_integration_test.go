package eventstore_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-estoria/estoria-contrib/internal/eventstoretest"
	"github.com/go-estoria/estoria-contrib/mongodb/eventstore"
	"github.com/go-estoria/estoria-contrib/mongodb/eventstore/strategy"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func TestEventStore_Integration_DeletionGlobalReadConsistency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Parallel()

	ctx := t.Context()

	mongoClient, err := createMongoDBContainer(t)
	if err != nil {
		t.Fatalf("failed to create MongoDB container: %v", err)
	}

	// The check asserts exact global-read contents, so each store gets its own database.
	dbCount := 0
	newDatabase := func(t *testing.T) *mongo.Database {
		t.Helper()
		dbCount++
		database := mongoClient.Database(fmt.Sprintf("estoria_gc_%d", dbCount))
		t.Cleanup(func() {
			if err := database.Drop(context.WithoutCancel(ctx)); err != nil {
				t.Fatalf("tc cleanup: failed to drop database: %v", err)
			}
		})
		return database
	}

	for _, tt := range []struct {
		name        string
		newStrategy func(*testing.T, *mongo.Database) eventstore.Strategy
	}{
		{
			name: "single collection strategy",
			newStrategy: func(t *testing.T, db *mongo.Database) eventstore.Strategy {
				t.Helper()
				strat, err := strategy.NewSingleCollectionStrategy(mongoClient, db)
				if err != nil {
					t.Fatalf("tc setup: failed to create SingleCollectionStrategy: %v", err)
				}
				return strat
			},
		},
		{
			// One collection per stream, so deletion and the global read span collections.
			name: "multi collection strategy",
			newStrategy: func(t *testing.T, db *mongo.Database) eventstore.Strategy {
				t.Helper()
				strat, err := strategy.NewMultiCollectionStrategy(mongoClient, db, strategy.CollectionPerStreamID())
				if err != nil {
					t.Fatalf("tc setup: failed to create MultiCollectionStrategy: %v", err)
				}
				return strat
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			eventstoretest.RunDeletionGlobalReadConsistency(t, func(t *testing.T) eventstoretest.DeleterGlobalStore {
				t.Helper()

				eventStore, err := eventstore.New(mongoClient, eventstore.WithStrategy(tt.newStrategy(t, newDatabase(t))))
				if err != nil {
					t.Fatalf("tc setup: failed to create EventStore: %v", err)
				}

				return eventStore
			})
		})
	}
}
