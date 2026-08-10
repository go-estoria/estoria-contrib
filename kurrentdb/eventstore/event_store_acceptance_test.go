package eventstore_test

import (
	"testing"

	"github.com/go-estoria/estoria-contrib/kurrentdb/eventstore"
	coreeventstore "github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/eventstore/storetest"
	"github.com/gofrs/uuid/v5"
)

func TestEventStore_AcceptanceTest(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Parallel()

	for _, tStrat := range []struct {
		name string
	}{
		{
			name: "default",
		},
	} {
		t.Run(tStrat.name, func(t *testing.T) {
			db, err := createKurrentContainer(t)
			if err != nil {
				t.Fatalf("failed to create EventStoreDB container: %v", err)
			}

			eventStore, err := eventstore.New(db)
			if err != nil {
				t.Fatalf("tc setup: failed to create EventStore: %v", err)
			}

			storetest.RunEventStoreSuite(t, func(*testing.T) coreeventstore.Store {
				return eventStore
			})
		})
	}
}

// The global reader suite requires exclusive ownership of the store's history, and a
// KurrentDB node has exactly one $all: every clause gets a store with a fresh stream
// prefix, whose global reads see none of the shared node's other streams.
func TestEventStore_GlobalReaderAcceptanceTest(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping acceptance test")
	}

	t.Parallel()

	db, err := createKurrentContainer(t)
	if err != nil {
		t.Fatalf("failed to create KurrentDB container: %v", err)
	}

	storetest.RunGlobalReaderSuite(t, func(t *testing.T) storetest.GlobalStore {
		t.Helper()

		prefix := "g" + uuid.Must(uuid.NewV4()).String()[0:8]
		eventStore, err := eventstore.New(db, eventstore.WithStreamPrefix(prefix))
		if err != nil {
			t.Fatalf("tc setup: failed to create EventStore: %v", err)
		}

		return eventStore
	})
}
