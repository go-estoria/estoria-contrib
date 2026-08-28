package checkpointstore_test

import (
	"testing"

	sqlitecheckpointstore "github.com/go-estoria/estoria-contrib/sqlite/checkpointstore"
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
		opts []sqlitecheckpointstore.Option
	}{
		{
			name: "default table name",
		},
		{
			// The name must differ from the default ("projection_checkpoint"), or the
			// case is indistinguishable from the one above and passes no matter how
			// the option is wired.
			name: "custom table name",
			opts: []sqlitecheckpointstore.Option{sqlitecheckpointstore.WithTableName("custom_checkpoint")},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := newSQLiteDB(t)

			store, err := sqlitecheckpointstore.New(db, tc.opts...)
			if err != nil {
				t.Fatalf("tc setup: failed to create CheckpointStore: %v", err)
			}

			if _, err := db.ExecContext(t.Context(), store.Schema()); err != nil {
				t.Fatalf("tc setup: failed to create checkpoint table: %v", err)
			}

			storetest.RunCheckpointStoreSuite(t, func(*testing.T) checkpointstore.Store {
				return store
			})
		})
	}
}
