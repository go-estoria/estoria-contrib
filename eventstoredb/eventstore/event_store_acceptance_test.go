package eventstore_test

import (
	"testing"

	"github.com/go-estoria/estoria-contrib/eventstoredb/eventstore"
	"github.com/go-estoria/estoria-contrib/tests"
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
			db, err := createKurrentContainer(t, t.Context())
			if err != nil {
				t.Fatalf("failed to create EventStoreDB container: %v", err)
			}

			eventStore, err := eventstore.NewEventStore(db)
			if err != nil {
				t.Fatalf("tc setup: failed to create EventStore: %v", err)
			}

			if err := tests.EventStoreAcceptanceTest(t, eventStore); err != nil {
				t.Errorf("acceptance test failed: %v", err)
			}
		})
	}
}
