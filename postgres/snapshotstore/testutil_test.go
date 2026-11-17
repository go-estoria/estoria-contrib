package snapshotstore_test

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/go-estoria/estoria"
	pgsnapshotstore "github.com/go-estoria/estoria-contrib/postgres/snapshotstore"
	"github.com/go-estoria/estoria/snapshotstore"
	"github.com/go-estoria/estoria/typeid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func createPostgresContainer(t *testing.T) (*pgxpool.Pool, error) {
	t.Helper()

	ctx := t.Context()

	postgresContainer, err := postgres.Run(ctx, "postgres:17",
		postgres.WithUsername("username"),
		postgres.WithPassword("password"),
		postgres.WithDatabase("estoria"),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		return nil, fmt.Errorf("starting Postgres container: %w", err)
	}

	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(postgresContainer); err != nil {
			t.Fatalf("failed to terminate Postgres container: %v", err)
		}
	})

	connStr, err := postgresContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return nil, fmt.Errorf("failed to get Postgres connection string: %w", err)
	}

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		t.Fatalf("failed to create Postgres pool: %v", err)
	}

	t.Cleanup(pool.Close)

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping Postgres: %w", err)
	}

	return pool, nil
}

// newStore creates a snapshot store over pool and applies its schema.
func newStore(t *testing.T, pool *pgxpool.Pool, opts ...pgsnapshotstore.Option) *pgsnapshotstore.SnapshotStore {
	t.Helper()

	store, err := pgsnapshotstore.New(pool, opts...)
	if err != nil {
		t.Fatalf("tc setup: failed to create SnapshotStore: %v", err)
	}

	if _, err := pool.Exec(t.Context(), store.Schema()); err != nil {
		t.Fatalf("tc setup: failed to create snapshot table: %v", err)
	}

	return store
}

func writeVersions(t *testing.T, store snapshotstore.SnapshotWriter, aggregateID typeid.ID, versions ...int64) {
	t.Helper()

	for _, version := range versions {
		if err := store.WriteSnapshot(t.Context(), &snapshotstore.AggregateSnapshot{
			AggregateID:      aggregateID,
			AggregateVersion: version,
			Data:             []byte(`{}`),
		}); err != nil {
			t.Fatalf("writing snapshot at version %d: %v", version, err)
		}
	}
}

// storedVersions returns the versions of the aggregate's snapshots in table, ascending,
// read with raw SQL so that retention is observed independently of ReadSnapshot.
func storedVersions(t *testing.T, pool *pgxpool.Pool, table string, aggregateID typeid.ID) []int64 {
	t.Helper()

	rows, err := pool.Query(t.Context(), fmt.Sprintf(
		`SELECT aggregate_version FROM %s WHERE aggregate_type = $1 AND aggregate_id = $2 ORDER BY aggregate_version`,
		pgx.Identifier{table}.Sanitize(),
	), aggregateID.Type, aggregateID.UUID)
	if err != nil {
		t.Fatalf("querying stored versions: %v", err)
	}

	versions, err := pgx.CollectRows(rows, pgx.RowTo[int64])
	if err != nil {
		t.Fatalf("collecting stored versions: %v", err)
	}

	return versions
}

// warnCounter is an estoria.Logger that counts warnings and discards everything else.
type warnCounter struct {
	warnings *atomic.Int64
}

func newWarnCounter() warnCounter {
	return warnCounter{warnings: &atomic.Int64{}}
}

func (l warnCounter) Debug(string, ...any)            {}
func (l warnCounter) Info(string, ...any)             {}
func (l warnCounter) Warn(string, ...any)             { l.warnings.Add(1) }
func (l warnCounter) Error(string, ...any)            {}
func (l warnCounter) With(...any) estoria.Logger      { return l }
func (l warnCounter) WithGroup(string) estoria.Logger { return l }
