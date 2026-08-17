package strategy

import (
	"reflect"
	"testing"

	"github.com/go-estoria/estoria/eventstore"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// TestFindOptsFromReadAllOptions_BoundsAtFrontier pins the filter every global-read
// cursor is built from: an inclusive upper bound at the read's frontier, plus the
// exclusive resume bound when one is set. The bound cannot be left to store-shaped
// integration behavior: the pinned server happens to keep an open cursor stable over
// data written by this store's own transactions, so an unbounded filter passes those
// reads and bites only over documents written outside transactions.
func TestFindOptsFromReadAllOptions_BoundsAtFrontier(t *testing.T) {
	_, filter := findOptsFromReadAllOptions(eventstore.ReadAllOptions{}, 42)
	want := bson.D{{Key: fieldGlobalOffset, Value: bson.D{{Key: opLTE, Value: int64(42)}}}}
	if !reflect.DeepEqual(filter, want) {
		t.Errorf("want the frontier bound %v, got %v", want, filter)
	}

	_, filter = findOptsFromReadAllOptions(eventstore.ReadAllOptions{AfterPosition: 7}, 42)
	want = bson.D{{Key: fieldGlobalOffset, Value: bson.D{{Key: opLTE, Value: int64(42)}, {Key: "$gt", Value: int64(7)}}}}
	if !reflect.DeepEqual(filter, want) {
		t.Errorf("want the frontier and resume bounds %v, got %v", want, filter)
	}
}
