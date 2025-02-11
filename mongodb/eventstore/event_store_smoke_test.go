package eventstore_test

import (
	"context"
	"testing"

	"github.com/go-estoria/estoria-contrib/mongodb/eventstore"
	"github.com/go-estoria/estoria-contrib/tests"
)

func TestEventStore_SmokeTest(t *testing.T) {
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
			name:     "store with default options",
			haveOpts: func(*testing.T) []eventstore.EventStoreOption { return []eventstore.EventStoreOption{} },
		},
	} {
		eventStore, err := eventstore.New(mongoClient, tt.haveOpts(t)...)
		if err != nil {
			t.Fatalf("tc setup: failed to create EventStore: %v", err)
		}

		if err := tests.EventStoreSmokeTest(t, eventStore); err != nil {
			t.Errorf("smoke test failed: %s: %v", tt.name, err)
		}
	}
}
