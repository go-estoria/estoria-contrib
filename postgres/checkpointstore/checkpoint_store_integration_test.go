package checkpointstore_test

import (
	"math"
	"testing"

	pgcheckpointstore "github.com/go-estoria/estoria-contrib/postgres/checkpointstore"
	"github.com/go-estoria/estoria/projection"
)

// projection.ID.Version is a Go int, so the schema's columns must round-trip
// 64-bit values; a 32-bit integer column would fail the save.
func TestCheckpointStore_Integration_BoundaryValues(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Parallel()

	pool, err := createPostgresContainer(t)
	if err != nil {
		t.Fatalf("failed to create Postgres container: %v", err)
	}

	store, err := pgcheckpointstore.New(pool)
	if err != nil {
		t.Fatalf("tc setup: failed to create CheckpointStore: %v", err)
	}

	if _, err := pool.Exec(t.Context(), store.Schema()); err != nil {
		t.Fatalf("tc setup: failed to create checkpoint table: %v", err)
	}

	id := projection.ID{Name: "boundary", Version: math.MaxInt}

	if err := store.Save(t.Context(), id, math.MaxInt64); err != nil {
		t.Fatalf("saving checkpoint: %v", err)
	}

	checkpoint, err := store.Load(t.Context(), id)
	if err != nil {
		t.Fatalf("loading checkpoint: %v", err)
	}

	if checkpoint.ProjectionID != id {
		t.Errorf("want projection ID %s, got %s", id, checkpoint.ProjectionID)
	}

	if checkpoint.Position != math.MaxInt64 {
		t.Errorf("want position %d, got %d", int64(math.MaxInt64), checkpoint.Position)
	}

	if err := store.Delete(t.Context(), id); err != nil {
		t.Errorf("deleting checkpoint: %v", err)
	}
}
