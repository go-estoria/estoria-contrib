package checkpointstore_test

import (
	"errors"
	"sync"
	"testing"

	sqlitecheckpointstore "github.com/go-estoria/estoria-contrib/sqlite/checkpointstore"
	"github.com/go-estoria/estoria/projection"
	"github.com/go-estoria/estoria/projection/checkpointstore"
)

// Pins the Store contract's concurrent-delete clause under the recommended
// connection configuration (WAL + busy_timeout, as newSQLiteDB configures):
// overlapping deletes of one ID serialize on the write lock rather than fail
// with SQLITE_BUSY, exactly one observes the row, and the rest report
// ErrCheckpointNotFound.
func TestCheckpointStore_Integration_ConcurrentDeletes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Parallel()

	db := newSQLiteDB(t)

	store, err := sqlitecheckpointstore.New(db)
	if err != nil {
		t.Fatalf("tc setup: failed to create CheckpointStore: %v", err)
	}

	if _, err := db.ExecContext(t.Context(), store.Schema()); err != nil {
		t.Fatalf("tc setup: failed to create checkpoint table: %v", err)
	}

	id := projection.ID{Name: "concurrent_delete", Version: 1}
	if err := store.Save(t.Context(), id, 10); err != nil {
		t.Fatalf("saving checkpoint: %v", err)
	}

	const deleters = 8

	var wg sync.WaitGroup
	results := make([]error, deleters)
	for i := range deleters {
		wg.Go(func() {
			results[i] = store.Delete(t.Context(), id)
		})
	}
	wg.Wait()

	deleted := 0
	for i, err := range results {
		switch {
		case err == nil:
			deleted++
		case !errors.Is(err, checkpointstore.ErrCheckpointNotFound):
			t.Errorf("want nil or ErrCheckpointNotFound from deleter %d, got %v", i, err)
		}
	}

	if deleted != 1 {
		t.Errorf("want exactly 1 successful delete, got %d", deleted)
	}
}
