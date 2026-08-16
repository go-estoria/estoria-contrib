package eventstore_test

import (
	"context"
	"testing"

	pgeventstore "github.com/go-estoria/estoria-contrib/postgres/eventstore"
	"github.com/go-estoria/estoria-contrib/postgres/eventstore/strategy"
	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

// benchmarkPool starts a dedicated Postgres container for a benchmark run.
// Deliberately self-contained — sharing no test helpers — so the same file
// dropped onto an older revision measures its append path unchanged.
func benchmarkPool(b *testing.B) *pgxpool.Pool {
	b.Helper()

	ctx := context.Background()

	container, err := postgres.Run(ctx, "postgres:17",
		postgres.WithUsername("username"),
		postgres.WithPassword("password"),
		postgres.WithDatabase("estoria"),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		b.Fatalf("starting Postgres container: %v", err)
	}

	b.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			b.Fatalf("failed to terminate Postgres container: %v", err)
		}
	})

	connString, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		b.Fatalf("getting connection string: %v", err)
	}

	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		b.Fatalf("creating pool: %v", err)
	}

	b.Cleanup(pool.Close)

	return pool
}

// BenchmarkAppendStream measures append throughput under the serialized
// global-position allocator: the single-writer cost of the allocator update,
// batching's effect on it, and the contention cost when concurrent writers
// to distinct streams serialize on the allocator row.
func BenchmarkAppendStream(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping benchmark")
	}

	pool := benchmarkPool(b)

	newStore := func(b *testing.B, suffix string) *pgeventstore.EventStore {
		b.Helper()

		strat, err := strategy.NewDefaultStrategy(
			strategy.WithEventsTableName("event_bench_"+suffix),
			strategy.WithStreamsTableName("stream_bench_"+suffix),
		)
		if err != nil {
			b.Fatalf("creating strategy: %v", err)
		}

		if _, err := pool.Exec(context.Background(), strat.Schema()); err != nil {
			b.Fatalf("creating tables: %v", err)
		}

		store, err := pgeventstore.New(pool, pgeventstore.WithStrategy(strat))
		if err != nil {
			b.Fatalf("creating store: %v", err)
		}

		return store
	}

	newEvents := func(n int) []*eventstore.WritableEvent {
		events := make([]*eventstore.WritableEvent, n)
		for i := range events {
			events[i] = &eventstore.WritableEvent{Type: "benchevent", Data: []byte(`{"n":1}`)}
		}

		return events
	}

	b.Run("single writer, one event per append", func(b *testing.B) {
		store := newStore(b, "s1")
		streamID := typeid.NewV4("bench")

		for b.Loop() {
			if _, err := store.AppendStream(context.Background(), streamID, newEvents(1), eventstore.AppendStreamOptions{}); err != nil {
				b.Fatalf("appending: %v", err)
			}
		}
	})

	b.Run("single writer, ten events per append", func(b *testing.B) {
		store := newStore(b, "s10")
		streamID := typeid.NewV4("bench")

		for b.Loop() {
			if _, err := store.AppendStream(context.Background(), streamID, newEvents(10), eventstore.AppendStreamOptions{}); err != nil {
				b.Fatalf("appending: %v", err)
			}
		}
	})

	b.Run("concurrent writers, distinct streams", func(b *testing.B) {
		store := newStore(b, "par")

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			streamID := typeid.NewV4("bench")
			for pb.Next() {
				if _, err := store.AppendStream(context.Background(), streamID, newEvents(1), eventstore.AppendStreamOptions{}); err != nil {
					b.Errorf("appending: %v", err)
					return
				}
			}
		})
	})
}
