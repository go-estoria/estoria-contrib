package eventstore_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/lib/pq"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func createPostgresContainer(t *testing.T, ctx context.Context) (*sql.DB, error) {
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

	t.Log("Postgres container connection string:", connStr)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Fatalf("failed to create Postgres client: %v", err)
	}

	t.Log("Created Postgres client")

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping Postgres: %w", err)
	}

	t.Log("Successfully pinged Postgres")

	return db, nil
}
