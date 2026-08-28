package checkpointstore

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

// Pins the strict-primary mode of the collection pin. The wire-level
// integration test cannot distinguish primary from primary-preferred — a
// direct connection sends both as "primaryPreferred" — so the mode is
// asserted here on the options themselves, and the wire test verifies the
// options are actually applied.
func TestPinnedCollectionOptions_StrictPrimary(t *testing.T) {
	t.Parallel()

	var collOpts options.CollectionOptions
	for _, setter := range pinnedCollectionOptions().Opts {
		if err := setter(&collOpts); err != nil {
			t.Fatalf("applying collection option: %v", err)
		}
	}

	if collOpts.ReadPreference == nil {
		t.Fatal("want a pinned read preference, got none")
	}

	if mode := collOpts.ReadPreference.Mode(); mode != readpref.PrimaryMode {
		t.Errorf("want read preference mode %v (strict primary), got %v", readpref.PrimaryMode, mode)
	}
}
