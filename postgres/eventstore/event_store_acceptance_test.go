package eventstore_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/go-estoria/estoria-contrib/postgres/eventstore"
	"github.com/go-estoria/estoria-contrib/postgres/eventstore/strategy"
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

	for _, tt := range []struct {
		name     string
		haveOpts func(*testing.T, *sql.DB) []eventstore.EventStoreOption
	}{
		{
			name: "store with default options",
			haveOpts: func(*testing.T, *sql.DB) []eventstore.EventStoreOption {
				t.Helper()
				return []eventstore.EventStoreOption{}
			},
		},
		{
			name: "store with single table strategy",
			haveOpts: func(t *testing.T, db *sql.DB) []eventstore.EventStoreOption {
				t.Helper()
				strat, err := strategy.NewSingleTableStrategy(db, "events")
				if err != nil {
					t.Fatalf("tc setup: failed to create SingleTableStrategy: %v", err)
				}
				return []eventstore.EventStoreOption{eventstore.WithStrategy(strat)}
			},
		},
	} {
		eventStore, err := eventstore.New(db, tt.haveOpts(t, db)...)
		if err != nil {
			t.Fatalf("tc setup: failed to create EventStore: %v", err)
		}

		if err := tests.EventStoreAcceptanceTest(t, eventStore); err != nil {
			t.Errorf("smoke test failed: %s: %v", tt.name, err)
		}
	}
}
