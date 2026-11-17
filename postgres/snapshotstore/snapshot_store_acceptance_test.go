package snapshotstore_test

import (
	"testing"

	pgsnapshotstore "github.com/go-estoria/estoria-contrib/postgres/snapshotstore"
	"github.com/go-estoria/estoria/snapshotstore"
	"github.com/go-estoria/estoria/snapshotstore/storetest"
)

func TestSnapshotStore_AcceptanceTest(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping acceptance test")
	}

	t.Parallel()

	for _, tc := range []struct {
		name string
		opts []pgsnapshotstore.Option
	}{
		{
			name: "default table name",
		},
		{
			// The name must differ from the default ("aggregate_snapshot"), or the case
			// is indistinguishable from the one above and passes no matter how the
			// option is wired.
			name: "custom table name",
			opts: []pgsnapshotstore.Option{pgsnapshotstore.WithTableName("custom_snapshot")},
		},
		{
			name: "retaining one snapshot",
			opts: []pgsnapshotstore.Option{pgsnapshotstore.WithMaxSnapshots(1)},
		},
		{
			name: "retaining two snapshots",
			opts: []pgsnapshotstore.Option{pgsnapshotstore.WithMaxSnapshots(2)},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool, err := createPostgresContainer(t)
			if err != nil {
				t.Fatalf("failed to create Postgres container: %v", err)
			}

			store := newStore(t, pool, tc.opts...)

			storetest.RunSnapshotStoreSuite(t, func(*testing.T) snapshotstore.SnapshotStore {
				return store
			})
		})
	}
}
