package eventstore_test

import (
	"testing"

	"github.com/go-estoria/estoria-contrib/sqlite/eventstore"
	"github.com/go-estoria/estoria-contrib/sqlite/eventstore/strategy"
	coreeventstore "github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/eventstore/storetest"
)

func TestEventStore_AcceptanceTest(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping acceptance test")
	}

	t.Parallel()

	for _, tStrat := range []struct {
		name   string
		create func(*testing.T) eventstore.Strategy
	}{
		{
			name: testStrategyDefault,
			create: func(t *testing.T) eventstore.Strategy {
				t.Helper()
				return must(strategy.NewDefaultStrategy())
			},
		},
		{
			// The names here must differ from the strategy defaults ("event" and "stream"),
			// or the case is indistinguishable from the one above and passes no matter how
			// the strategy is wired.
			name: "custom table names",
			create: func(t *testing.T) eventstore.Strategy {
				t.Helper()
				return must(strategy.NewDefaultStrategy(
					strategy.WithEventsTableName("custom_event"),
					strategy.WithStreamsTableName("custom_stream"),
				))
			},
		},
	} {
		t.Run(tStrat.name, func(t *testing.T) {
			db := newSQLiteDB(t)

			// Build the schema from the case's own strategy and hand that same strategy to
			// the store. Previously both were hardcoded to NewDefaultStrategy(), so the
			// custom-table-names case silently exercised the default one.
			strat := tStrat.create(t)

			if _, err := db.ExecContext(t.Context(), strat.Schema()); err != nil {
				t.Fatalf("tc setup: failed to create events table: %v", err)
			}

			eventStore, err := eventstore.New(db, eventstore.WithStrategy(strat))
			if err != nil {
				t.Fatalf("tc setup: failed to create EventStore: %v", err)
			}

			storetest.RunEventStoreSuite(t, func(*testing.T) coreeventstore.Store {
				return eventStore
			})
		})
	}
}
