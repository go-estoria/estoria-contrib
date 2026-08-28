package checkpointstore_test

import (
	"errors"
	"testing"

	mongocheckpointstore "github.com/go-estoria/estoria-contrib/mongodb/checkpointstore"
	"github.com/go-estoria/estoria/projection"
	"github.com/go-estoria/estoria/projection/checkpointstore"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

// An unacknowledged write concern makes Save fire-and-forget and Delete's
// DeletedCount meaningless; both must surface as errors rather than as success
// or a spurious ErrCheckpointNotFound.
func TestCheckpointStore_Integration_RejectsUnacknowledgedWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Parallel()

	client, err := createMongoDBContainer(t)
	if err != nil {
		t.Fatalf("failed to create MongoDB container: %v", err)
	}

	coll := client.Database(testDatabaseName(t)).Collection(checkpointCollName,
		options.Collection().SetWriteConcern(writeconcern.Unacknowledged()))

	store, err := mongocheckpointstore.New(coll)
	if err != nil {
		t.Fatalf("tc setup: failed to create CheckpointStore: %v", err)
	}

	id := projection.ID{Name: "unacknowledged", Version: 1}

	if err := store.Save(t.Context(), id, 1); err == nil {
		t.Error("want an error saving with an unacknowledged write concern, got nil")
	}

	if err := store.Delete(t.Context(), id); err == nil {
		t.Error("want an error deleting with an unacknowledged write concern, got nil")
	} else if errors.Is(err, checkpointstore.ErrCheckpointNotFound) {
		t.Errorf("want an error other than ErrCheckpointNotFound deleting with an unacknowledged write concern, got %v", err)
	}
}
