package eventstore_test

import (
	"context"
	"testing"

	"github.com/go-estoria/estoria-contrib/mongodb/eventstore"
	"github.com/go-estoria/estoria-contrib/tests"
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
		haveOpts func(*testing.T) []eventstore.EventStoreOption
	}{
		{
			name: "store with default options",
			haveOpts: func(*testing.T) []eventstore.EventStoreOption {
				return []eventstore.EventStoreOption{}
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			database := mongoClient.Database("estoria")
			t.Cleanup(func() {
				if err := database.Drop(ctx); err != nil {
					t.Fatalf("tc cleanup: failed to drop database: %v", err)
				}
			})

			eventStore, err := eventstore.New(mongoClient, tt.haveOpts(t)...)
			if err != nil {
				t.Fatalf("tc setup: failed to create EventStore: %v", err)
			}

			if err := eventStore.EnsureIndexes(ctx); err != nil {
				t.Fatalf("tc setup: failed to ensure indexes: %v", err)
			}

			if err := tests.EventStoreAcceptanceTest(t, eventStore); err != nil {
				t.Errorf("acceptance test failed: %s: %v", tt.name, err)
			}
		})
	}
}
