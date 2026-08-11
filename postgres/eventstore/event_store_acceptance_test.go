package eventstore_test

import (
	"fmt"
	"testing"

	"github.com/go-estoria/estoria-contrib/postgres/eventstore"
	"github.com/go-estoria/estoria-contrib/postgres/eventstore/strategy"
	coreeventstore "github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/eventstore/storetest"
)

func TestEventStore_AcceptanceTest(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
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
			db, err := createPostgresContainer(t)
			if err != nil {
				t.Fatalf("failed to create Postgres container: %v", err)
			}

			// Build the schema from the case's own strategy and hand that same strategy to
			// the store. Previously both were hardcoded to NewDefaultStrategy(), so the
			// custom-table-names case silently exercised the default one.
			strat := tStrat.create(t)

			if _, err = db.Exec(t.Context(), strat.Schema()); err != nil {
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

func TestEventStore_StreamDeleterAcceptanceTest(t *testing.T) {
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
			db, err := createPostgresContainer(t)
			if err != nil {
				t.Fatalf("failed to create Postgres container: %v", err)
			}

			strat := tStrat.create(t)

			if _, err = db.Exec(t.Context(), strat.Schema()); err != nil {
				t.Fatalf("tc setup: failed to create events table: %v", err)
			}

			eventStore, err := eventstore.New(db, eventstore.WithStrategy(strat))
			if err != nil {
				t.Fatalf("tc setup: failed to create EventStore: %v", err)
			}

			storetest.RunStreamDeleterSuite(t, func(*testing.T) storetest.DeleterStore {
				return eventStore
			})
		})
	}
}

// The global reader suite requires exclusive ownership of the store's history, so unlike
// the suite above, every clause gets its own event and stream tables — a fresh pair per
// clause within one container per case, rather than a container per clause.
func TestEventStore_GlobalReaderAcceptanceTest(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping acceptance test")
	}

	t.Parallel()

	for _, tStrat := range []struct {
		name         string
		eventsTable  string
		streamsTable string
	}{
		{name: testStrategyDefault, eventsTable: "event", streamsTable: "stream"},
		{name: "custom table names", eventsTable: "custom_event", streamsTable: "custom_stream"},
	} {
		t.Run(tStrat.name, func(t *testing.T) {
			db, err := createPostgresContainer(t)
			if err != nil {
				t.Fatalf("failed to create Postgres container: %v", err)
			}

			clause := 0
			storetest.RunGlobalReaderSuite(t, func(t *testing.T) storetest.GlobalStore {
				t.Helper()

				clause++
				strat := must(strategy.NewDefaultStrategy(
					strategy.WithEventsTableName(fmt.Sprintf("%s_g%d", tStrat.eventsTable, clause)),
					strategy.WithStreamsTableName(fmt.Sprintf("%s_g%d", tStrat.streamsTable, clause)),
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
		})
	}
}
