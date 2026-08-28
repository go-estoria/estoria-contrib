package checkpointstore_test

import (
	"testing"

	mongocheckpointstore "github.com/go-estoria/estoria-contrib/mongodb/checkpointstore"
	"github.com/go-estoria/estoria/projection/checkpointstore"
	"github.com/go-estoria/estoria/projection/checkpointstore/storetest"
)

// The leading underscore keeps the checkpoint collection out of the namespace a
// MultiCollectionStrategy's selector can produce, so its event collection
// enumeration ignores it.
const checkpointCollName = "_projection_checkpoints"

func TestCheckpointStore_AcceptanceTest(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping acceptance test")
	}

	t.Parallel()

	client, err := createMongoDBContainer(t)
	if err != nil {
		t.Fatalf("failed to create MongoDB container: %v", err)
	}

	coll := client.Database(testDatabaseName(t)).Collection(checkpointCollName)

	store, err := mongocheckpointstore.New(coll)
	if err != nil {
		t.Fatalf("tc setup: failed to create CheckpointStore: %v", err)
	}

	storetest.RunCheckpointStoreSuite(t, func(*testing.T) checkpointstore.Store {
		return store
	})
}
