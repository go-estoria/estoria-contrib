package strategy_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func createPostgresContainer(t *testing.T, ctx context.Context) (*sql.DB, error) {
	t.Helper()

	postgresContainer, err := postgres.Run(ctx, "postgres:17")
	if err != nil {
		return nil, fmt.Errorf("starting Postgres container: %w", err)
	}

	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(postgresContainer); err != nil {
			t.Fatalf("failed to terminate Postgres container: %v", err)
		}
	})

	connStr, err := postgresContainer.ConnectionString(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get Postgres connection string: %w", err)
	}

	t.Log("Postgres container connection string:", connStr)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Fatalf("failed to create SQL client: %v", err)
	}

	t.Log("Created SQL client")

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping Postgres: %w", err)
	}

	t.Log("Successfully pinged Postgres")

	return db, nil
}
