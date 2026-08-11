// Package eventstoretest provides cross-backend test helpers for event store
// implementations.
package eventstoretest

import (
	"testing"

	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
)

// A DeleterGlobalStore is an event store implementing both optional capabilities whose
// interaction RunDeletionGlobalReadConsistency pins.
type DeleterGlobalStore interface {
	eventstore.StreamWriter
	eventstore.GlobalReader
	eventstore.StreamDeleter
}

// RunDeletionGlobalReadConsistency pins that deletion reaches the global read path: a
// deleted or truncated stream's events stop appearing in ReadAll, while surviving events
// keep the global positions they were written with. No storetest suite covers this
// interaction — the stream deleter suite never reads globally — so backends implementing
// both capabilities run it alongside the suites. Every call to newStore must return a
// store whose event history the check exclusively owns.
func RunDeletionGlobalReadConsistency(t *testing.T, newStore func(t *testing.T) DeleterGlobalStore) {
	t.Helper()

	appendOne := func(t *testing.T, store DeleterGlobalStore, streamID typeid.ID) *eventstore.Event {
		t.Helper()

		written, err := store.AppendStream(t.Context(), streamID, []*eventstore.WritableEvent{
			{Type: "testevent", Data: []byte(`{}`)},
		}, eventstore.AppendStreamOptions{})
		if err != nil {
			t.Fatalf("appending event: %v", err)
		}

		return written[0]
	}

	readGlobal := func(t *testing.T, store DeleterGlobalStore) []*eventstore.Event {
		t.Helper()

		iter, err := store.ReadAll(t.Context(), eventstore.ReadAllOptions{})
		if err != nil {
			t.Fatalf("reading all streams: %v", err)
		}

		t.Cleanup(func() { _ = iter.Close(t.Context()) })

		events, err := eventstore.Collect(t.Context(), iter)
		if err != nil {
			t.Fatalf("collecting events: %v", err)
		}

		return events
	}

	t.Run("a fully deleted stream vanishes from the global read", func(t *testing.T) {
		store := newStore(t)

		deleted := typeid.NewV4("deleted")
		bystander := typeid.NewV4("bystander")

		appendOne(t, store, deleted)
		survivor := appendOne(t, store, bystander)
		appendOne(t, store, deleted)

		if err := store.DeleteStream(t.Context(), deleted, eventstore.DeleteStreamOptions{}); err != nil {
			t.Fatalf("deleting stream: %v", err)
		}

		events := readGlobal(t, store)
		if len(events) != 1 {
			t.Fatalf("want only the bystander's event in the global read, got %d events", len(events))
		}

		if events[0].ID != survivor.ID {
			t.Errorf("want the bystander's event %s, got %s", survivor.ID, events[0].ID)
		}

		if *events[0].GlobalPosition != *survivor.GlobalPosition {
			t.Errorf("want the surviving event to keep global position %d, got %d",
				*survivor.GlobalPosition, *events[0].GlobalPosition)
		}
	})

	t.Run("truncated events vanish, retained events keep their positions", func(t *testing.T) {
		store := newStore(t)

		truncated := typeid.NewV4("truncated")
		bystander := typeid.NewV4("bystander")

		appendOne(t, store, truncated)
		other := appendOne(t, store, bystander)
		appendOne(t, store, truncated)
		retained := appendOne(t, store, truncated)

		if err := store.DeleteStream(t.Context(), truncated, eventstore.DeleteStreamOptions{ToVersion: 2}); err != nil {
			t.Fatalf("truncating stream: %v", err)
		}

		events := readGlobal(t, store)
		if len(events) != 2 {
			t.Fatalf("want 2 events in the global read after truncation, got %d", len(events))
		}

		if events[0].ID != other.ID || *events[0].GlobalPosition != *other.GlobalPosition {
			t.Errorf("want the bystander's event %s at position %d first, got %s at %d",
				other.ID, *other.GlobalPosition, events[0].ID, *events[0].GlobalPosition)
		}

		if events[1].ID != retained.ID || *events[1].GlobalPosition != *retained.GlobalPosition {
			t.Errorf("want the retained event %s at position %d second, got %s at %d",
				retained.ID, *retained.GlobalPosition, events[1].ID, *events[1].GlobalPosition)
		}
	})
}
