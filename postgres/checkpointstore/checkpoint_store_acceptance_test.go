package checkpointstore_test

import (
	"testing"

	pgcheckpointstore "github.com/go-estoria/estoria-contrib/postgres/checkpointstore"
	"github.com/go-estoria/estoria/projection/checkpointstore"
	"github.com/go-estoria/estoria/projection/checkpointstore/storetest"
)

func TestCheckpointStore_AcceptanceTest(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping acceptance test")
	}

	t.Parallel()

	for _, tc := range []struct {
		name string
		opts []pgcheckpointstore.Option
	}{
		{
			name: "default table name",
		},
		{
			// The name must differ from the default ("projection_checkpoint"), or the
			// case is indistinguishable from the one above and passes no matter how
			// the option is wired.
			name: "custom table name",
			opts: []pgcheckpointstore.Option{pgcheckpointstore.WithTableName("custom_checkpoint")},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool, err := createPostgresContainer(t)
			if err != nil {
				t.Fatalf("failed to create Postgres container: %v", err)
			}

			store, err := pgcheckpointstore.New(pool, tc.opts...)
			if err != nil {
				t.Fatalf("tc setup: failed to create CheckpointStore: %v", err)
			}

			if _, err := pool.Exec(t.Context(), store.Schema()); err != nil {
				t.Fatalf("tc setup: failed to create checkpoint table: %v", err)
			}

			storetest.RunCheckpointStoreSuite(t, func(*testing.T) checkpointstore.Store {
				return store
			})
		})
	}
}
