package checkpointstore_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	mongocheckpointstore "github.com/go-estoria/estoria-contrib/mongodb/checkpointstore"
	"github.com/go-estoria/estoria/projection"
	"github.com/go-estoria/estoria/projection/checkpointstore"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

// Pins the strict-primary read pin against a hostile collection configured for
// secondary reads. The assertion is at the wire level: the harness connects
// with a direct connection, and a directly-connected mongod serves reads
// regardless of $readPreference, so a stale read cannot be provoked here — but
// the command the driver sends still reveals which member Load would target.
// Without the pin, the collection's secondary read preference appears in the
// find command (as "secondary" or the direct-connection "secondaryPreferred"
// translation) and a lagging secondary could serve a pre-rewind position.
func TestCheckpointStore_Integration_PinsReadsToPrimary(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Parallel()

	connStr, err := startMongoDBContainer(t)
	if err != nil {
		t.Fatalf("failed to create MongoDB container: %v", err)
	}

	var (
		mu    sync.Mutex
		finds []bson.Raw
	)
	monitor := &event.CommandMonitor{
		Started: func(_ context.Context, evt *event.CommandStartedEvent) {
			if evt.CommandName != "find" {
				return
			}
			// The event's buffer is only valid during the callback.
			raw := bson.Raw(append([]byte(nil), evt.Command...))
			mu.Lock()
			finds = append(finds, raw)
			mu.Unlock()
		},
	}

	client, err := connectMongoDBClient(t, connStr, monitor)
	if err != nil {
		t.Fatalf("failed to create MongoDB client: %v", err)
	}

	coll := client.Database(testDatabaseName(t)).Collection(checkpointCollName,
		options.Collection().SetReadPreference(readpref.Secondary()))

	store, err := mongocheckpointstore.New(coll)
	if err != nil {
		t.Fatalf("tc setup: failed to create CheckpointStore: %v", err)
	}

	id := projection.ID{Name: "read_pin", Version: 1}

	if err := store.Save(t.Context(), id, 42); err != nil {
		t.Fatalf("saving checkpoint: %v", err)
	}

	loaded, err := store.Load(t.Context(), id)
	if err != nil {
		t.Fatalf("loading checkpoint: %v", err)
	}

	if loaded.Position != 42 {
		t.Errorf("want position 42, got %d", loaded.Position)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(finds) == 0 {
		t.Fatal("want at least one find command captured, got none")
	}

	for _, find := range finds {
		pref := find.Lookup("$readPreference")
		if pref.Validate() != nil {
			continue // absent: the driver's default, which is primary
		}

		// On a direct connection the driver translates primary to
		// primaryPreferred so the directly-connected member can serve reads
		// even mid-election; a secondary-flavored mode means the pin is gone.
		if mode := pref.Document().Lookup("mode").StringValue(); mode != "primary" && mode != "primaryPreferred" {
			t.Errorf("want find commands to read from the primary, got read preference mode %q", mode)
		}
	}
}

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
