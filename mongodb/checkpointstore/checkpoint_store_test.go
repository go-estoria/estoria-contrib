package checkpointstore_test

import (
	"testing"

	mongocheckpointstore "github.com/go-estoria/estoria-contrib/mongodb/checkpointstore"
)

func TestNew_RequiresCollection(t *testing.T) {
	t.Parallel()

	if _, err := mongocheckpointstore.New(nil); err == nil {
		t.Error("want an error creating a CheckpointStore without a collection, got nil")
	}
}
