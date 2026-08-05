package eventstore_test

import (
	"testing"

	"github.com/go-estoria/estoria-contrib/kurrentdb/eventstore"
	coreeventstore "github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/eventstore/storetest"
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
