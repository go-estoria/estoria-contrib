package eventstore_test

import (
	"fmt"
	"testing"

	"github.com/go-estoria/estoria-contrib/internal/eventstoretest"
	"github.com/go-estoria/estoria-contrib/postgres/eventstore"
	"github.com/go-estoria/estoria-contrib/postgres/eventstore/strategy"
)

func TestEventStore_Integration_DeletionGlobalReadConsistency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Parallel()

	db, err := createPostgresContainer(t)
	if err != nil {
		t.Fatalf("failed to create Postgres container: %v", err)
	}

	// The check asserts exact global-read contents, so each store gets its own tables.
	subtest := 0
	eventstoretest.RunDeletionGlobalReadConsistency(t, func(t *testing.T) eventstoretest.DeleterGlobalStore {
		t.Helper()

		subtest++
		strat := must(strategy.NewDefaultStrategy(
			strategy.WithEventsTableName(fmt.Sprintf("event_gc%d", subtest)),
			strategy.WithStreamsTableName(fmt.Sprintf("stream_gc%d", subtest)),
		))

		if _, err := db.Exec(t.Context(), strat.Schema()); err != nil {
			t.Fatalf("tc setup: failed to create tables: %v", err)
		}

		eventStore, err := eventstore.New(db, eventstore.WithStrategy(strat))
		if err != nil {
			t.Fatalf("tc setup: failed to create EventStore: %v", err)
		}

		return eventStore
	})
}
