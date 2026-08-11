package eventstore_test

import (
	"testing"

	"github.com/go-estoria/estoria-contrib/internal/eventstoretest"
	"github.com/go-estoria/estoria-contrib/sqlite/eventstore"
	"github.com/go-estoria/estoria-contrib/sqlite/eventstore/strategy"
)

func TestEventStore_Integration_DeletionGlobalReadConsistency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Parallel()

	eventstoretest.RunDeletionGlobalReadConsistency(t, func(t *testing.T) eventstoretest.DeleterGlobalStore {
		t.Helper()

		db := newSQLiteDB(t)
		strat := must(strategy.NewDefaultStrategy())

		if _, err := db.ExecContext(t.Context(), strat.Schema()); err != nil {
			t.Fatalf("tc setup: failed to create events table: %v", err)
		}

		eventStore, err := eventstore.New(db, eventstore.WithStrategy(strat))
		if err != nil {
			t.Fatalf("tc setup: failed to create EventStore: %v", err)
		}

		return eventStore
	})
}
