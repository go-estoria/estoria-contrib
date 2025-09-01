package eventstore_test

import (
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

	for _, tStrat := range []struct {
		name   string
		desc   string
		create func(*testing.T) eventstore.Strategy
	}{
		{
			name: testStrategyDefault,
			desc: "default options",
			create: func(*testing.T) eventstore.Strategy {
				t.Helper()
				return must(strategy.NewDefaultStrategy())
			},
		},
		{
			name: testStrategyDefault,
			desc: "custom table names",
			create: func(t *testing.T) eventstore.Strategy {
				t.Helper()
				return must(strategy.NewDefaultStrategy(
					strategy.WithEventsTableName("event"),
					strategy.WithStreamsTableName("stream"),
				))
			},
		},
	} {
		t.Run(tStrat.name+"_"+tStrat.desc, func(t *testing.T) {
			db, err := createPostgresContainer(t, t.Context())
			if err != nil {
				t.Fatalf("failed to create Postgres container: %v", err)
			}

			strat, err := strategy.NewDefaultStrategy()
			if err != nil {
				t.Fatalf("tc setup: failed to create event store strategy: %v", err)
			}

			if _, err = db.ExecContext(t.Context(), strat.Schema()); err != nil {
				t.Fatalf("tc setup: failed to create events table: %v", err)
			}

			eventStore, err := eventstore.New(db)
			if err != nil {
				t.Fatalf("tc setup: failed to create EventStore: %v", err)
			}

			if err := tests.EventStoreAcceptanceTest(t, eventStore); err != nil {
				t.Errorf("acceptance test failed: %v", err)
			}
		})
	}
}
