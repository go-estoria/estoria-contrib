package eventstore_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
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

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Fatalf("failed to create Postgres client: %v", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping Postgres: %w", err)
	}

	return db, nil
}

func must[T any](val T, err error) T {
	if err != nil {
		panic("unexpected error: " + err.Error())
	}
	return val
}

func reversed[T any](s []T) []T {
	r := make([]T, len(s))
	copy(r, s)
	slices.Reverse(r)
	return r
}

// jsonEq compares the JSON in two byte slices.
func jsonEq(a, b []byte) (bool, error) {
	var j, j2 interface{}
	if err := json.Unmarshal(a, &j); err != nil {
		return false, err
	}
	if err := json.Unmarshal(b, &j2); err != nil {
		return false, err
	}
	return reflect.DeepEqual(j2, j), nil
}
