package outbox_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func createPostgresContainer(t *testing.T, ctx context.Context) (*pgxpool.Pool, error) {
	t.Helper()

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

func must[T any](val T, err error) T {
	if err != nil {
		panic("unexpected error: " + err.Error())
	}
	return val
}
