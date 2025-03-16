package eventstore_test

import (
	"context"
	"testing"

	"github.com/go-estoria/estoria-contrib/mongodb/eventstore"
	"github.com/go-estoria/estoria-contrib/mongodb/eventstore/strategy"
	"github.com/go-estoria/estoria-contrib/tests"
	"github.com/go-estoria/estoria/typeid"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func TestEventStore_AcceptanceTest(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Parallel()

	ctx := context.Background()

	mongoClient, err := createMongoDBContainer(t, ctx)
	if err != nil {
		t.Fatalf("failed to create MongoDB container: %v", err)
	}

	for _, tt := range []struct {
		name     string
		haveOpts func(*testing.T, *mongo.Database) []eventstore.EventStoreOption
	}{
		{
			name: "store with default options",
			haveOpts: func(*testing.T, *mongo.Database) []eventstore.EventStoreOption {
				return []eventstore.EventStoreOption{}
			},
		},
		{
			name: "store with single collection strategy",
			haveOpts: func(t *testing.T, db *mongo.Database) []eventstore.EventStoreOption {
				t.Helper()
				collection := db.Collection("events")
				t.Cleanup(func() {
					if err := collection.Drop(ctx); err != nil {
						t.Fatalf("tc cleanup: failed to drop collection: %v", err)
					}
				})
				strat, err := strategy.NewSingleCollectionStrategy(mongoClient, collection)
				if err != nil {
					t.Fatalf("tc setup: failed to create SingleCollectionStrategy: %v", err)
				}
				return []eventstore.EventStoreOption{eventstore.WithStrategy(strat)}
			},
		},
		{
			name: "store with multi collection strategy",
			haveOpts: func(t *testing.T, db *mongo.Database) []eventstore.EventStoreOption {
				t.Helper()
				strat, err := strategy.NewMultiCollectionStrategy(mongoClient, db,
					strategy.CollectionSelectorFunc(func(typeid.UUID) string {
						return "events"
					}),
				)
				if err != nil {
					t.Fatalf("tc setup: failed to create MultiCollectionStrategy: %v", err)
				}
				return []eventstore.EventStoreOption{eventstore.WithStrategy(strat)}
			},
		},
	} {
		database := mongoClient.Database("estoria")
		t.Cleanup(func() {
			if err := database.Drop(ctx); err != nil {
				t.Fatalf("tc cleanup: failed to drop database: %v", err)
			}
		})

		eventStore, err := eventstore.New(mongoClient, tt.haveOpts(t, database)...)
		if err != nil {
			t.Fatalf("tc setup: failed to create EventStore: %v", err)
		}

		if err := tests.EventStoreAcceptanceTest(t, eventStore); err != nil {
			t.Errorf("acceptance test failed: %s: %v", tt.name, err)
		}
	}
}
