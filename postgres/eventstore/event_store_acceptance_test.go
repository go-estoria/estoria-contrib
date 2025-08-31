package eventstore_test

import (
	"context"
	"testing"

	"github.com/go-estoria/estoria-contrib/postgres/eventstore"
	"github.com/go-estoria/estoria-contrib/tests"
)

func TestEventStore_AcceptanceTest(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Parallel()

	ctx := context.Background()

	db, err := createPostgresContainer(t, ctx)
	if err != nil {
		t.Fatalf("failed to create Postgres container: %v", err)
	}

	eventStore, err := eventstore.New(db)
	if err != nil {
		t.Fatalf("tc setup: failed to create EventStore: %v", err)
	}

	if err := tests.EventStoreAcceptanceTest(t, eventStore); err != nil {
		t.Errorf("acceptance test failed: %v", err)
	}
}
