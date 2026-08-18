package strategy

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

// TestReadViewOptions_PinsStrictPrimaryAndMajority pins the read view's exact
// configuration where the wire cannot: a direct connection renders both primary and
// primaryPreferred as "primaryPreferred", so only this assertion separates the strict
// mode — which refuses to fall back to a lagging secondary under replica-set
// discovery — from the fallback-permitting one.
func TestReadViewOptions_PinsStrictPrimaryAndMajority(t *testing.T) {
	t.Parallel()

	opts := &options.CollectionOptions{}
	for _, apply := range readViewOptions().List() {
		if err := apply(opts); err != nil {
			t.Fatalf("applying read view options: %v", err)
		}
	}

	if opts.ReadPreference == nil || opts.ReadPreference.Mode() != readpref.PrimaryMode {
		t.Fatalf("want the read view pinned to strict primary mode, got %v", opts.ReadPreference)
	}
	if opts.ReadConcern == nil || opts.ReadConcern.Level != "majority" {
		t.Fatalf("want the read view pinned to majority read concern, got %+v", opts.ReadConcern)
	}
}
