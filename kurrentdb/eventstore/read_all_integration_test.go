package eventstore_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-estoria/estoria-contrib/kurrentdb/eventstore"
	coreeventstore "github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
	"github.com/gofrs/uuid/v5"
	"github.com/kurrent-io/KurrentDB-Client-Go/kurrentdb"
)

func TestEventStore_Integration_ReadAll(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Parallel()

	ctx := t.Context()

	client, err := createKurrentContainer(t)
	if err != nil {
		t.Fatalf("failed to create KurrentDB container: %v", err)
	}

	writableEvents := func(n int) []*coreeventstore.WritableEvent {
		events := make([]*coreeventstore.WritableEvent, n)
		for i := range events {
			events[i] = &coreeventstore.WritableEvent{
				Type: "testevent",
				Data: fmt.Appendf(nil, `{"index":%d}`, i),
			}
		}
		return events
	}

	// The subtests run in order: this one asserts the exact contents of an unprefixed
	// store's global read, whose filtering is by name parsing alone, so it must observe
	// $all before the later subtests' prefixed streams land in it.
	t.Run("filters system streams from global reads", func(t *testing.T) {
		store, err := eventstore.New(client)
		if err != nil {
			t.Fatalf("tc setup: failed to create EventStore: %v", err)
		}

		streamA, streamB := typeid.NewV4("filtertest"), typeid.NewV4("filtertest")
		writtenA, err := store.AppendStream(ctx, streamA, writableEvents(2), coreeventstore.AppendStreamOptions{})
		if err != nil {
			t.Fatalf("appending to stream A: %v", err)
		}
		writtenB, err := store.AppendStream(ctx, streamB, writableEvents(1), coreeventstore.AppendStreamOptions{})
		if err != nil {
			t.Fatalf("appending to stream B: %v", err)
		}

		// Setting stream metadata writes an event into a $$-prefixed metadata stream,
		// which lands in $all alongside the projection noise the container generates.
		metadata := kurrentdb.StreamMetadata{}
		metadata.SetMaxCount(1000)
		if _, err := client.SetStreamMetadata(ctx, streamA.String(), kurrentdb.AppendToStreamOptions{}, metadata); err != nil {
			t.Fatalf("setting stream metadata: %v", err)
		}

		iter, err := store.ReadAll(ctx, coreeventstore.ReadAllOptions{})
		if err != nil {
			t.Fatalf("reading all events: %v", err)
		}
		t.Cleanup(func() { _ = iter.Close(ctx) })

		events, err := coreeventstore.Collect(ctx, iter)
		if err != nil {
			t.Fatalf("collecting events: %v", err)
		}

		want := append(append([]*coreeventstore.Event{}, writtenA...), writtenB...)
		if len(events) != len(want) {
			t.Fatalf("global read yielded %d events, want %d", len(events), len(want))
		}
		for i, event := range events {
			if event.ID != want[i].ID {
				t.Errorf("global read event %d is %s, want %s", i, event.ID, want[i].ID)
			}
		}
	})

	t.Run("resumes across read windows", func(t *testing.T) {
		// A window of 3 raw records forces the iterator to reopen windows repeatedly,
		// resuming from its cursor each time, even before filtering discards noise.
		prefix := "g" + uuid.Must(uuid.NewV4()).String()[0:8]
		store, err := eventstore.New(client,
			eventstore.WithStreamPrefix(prefix),
			eventstore.WithReadAllWindowSize(3),
		)
		if err != nil {
			t.Fatalf("tc setup: failed to create EventStore: %v", err)
		}

		streams := []typeid.ID{typeid.NewV4("windowtest"), typeid.NewV4("windowtest"), typeid.NewV4("windowtest")}
		var written []*coreeventstore.Event
		for i, streamID := range streams {
			events, err := store.AppendStream(ctx, streamID, writableEvents(i+2), coreeventstore.AppendStreamOptions{})
			if err != nil {
				t.Fatalf("appending to stream %d: %v", i, err)
			}
			written = append(written, events...)
		}

		assertEventIDs := func(t *testing.T, opts coreeventstore.ReadAllOptions, want []*coreeventstore.Event) {
			t.Helper()
			iter, err := store.ReadAll(ctx, opts)
			if err != nil {
				t.Fatalf("reading all events with %+v: %v", opts, err)
			}
			defer iter.Close(ctx)

			events, err := coreeventstore.Collect(ctx, iter)
			if err != nil {
				t.Fatalf("collecting events with %+v: %v", opts, err)
			}
			if len(events) != len(want) {
				t.Fatalf("read with %+v yielded %d events, want %d", opts, len(events), len(want))
			}
			for i, event := range events {
				if event.ID != want[i].ID {
					t.Errorf("read with %+v: event %d is %s, want %s", opts, i, event.ID, want[i].ID)
				}
			}
		}

		assertEventIDs(t, coreeventstore.ReadAllOptions{}, written)
		assertEventIDs(t, coreeventstore.ReadAllOptions{AfterPosition: *written[3].GlobalPosition}, written[4:])
		assertEventIDs(t, coreeventstore.ReadAllOptions{Count: 4}, written[:4])

		// A resume position between records — a synthetic checkpoint, not one a read
		// yielded — is rejected by the server (its chunk reader misparses the bytes at
		// that offset as a record header, surfacing as InvalidPosition with a nonsense
		// length). The iterator must recover by rescanning from the start of $all with
		// the bound still applied. The position must lie below the frontier: at or past
		// it, the read is born exhausted and the server is never consulted.
		assertEventIDs(t, coreeventstore.ReadAllOptions{AfterPosition: *written[1].GlobalPosition + 1}, written[2:])
	})

	t.Run("the minimum window size makes progress across reopens", func(t *testing.T) {
		// Every reopened window returns the inclusive cursor record first, so the
		// minimum window of 2 advances by at most one record per reopen — the
		// worst case for progress. The deadline is a starvation guard: a window
		// that cannot get past its own cursor record reopens forever.
		prefix := "g" + uuid.Must(uuid.NewV4()).String()[0:8]
		store, err := eventstore.New(client,
			eventstore.WithStreamPrefix(prefix),
			eventstore.WithReadAllWindowSize(2),
		)
		if err != nil {
			t.Fatalf("tc setup: failed to create EventStore: %v", err)
		}

		streamID := typeid.NewV4("minwindow")
		written, err := store.AppendStream(ctx, streamID, writableEvents(5), coreeventstore.AppendStreamOptions{})
		if err != nil {
			t.Fatalf("appending events: %v", err)
		}

		drainCtx, cancel := context.WithTimeout(ctx, time.Minute)
		defer cancel()

		iter, err := store.ReadAll(drainCtx, coreeventstore.ReadAllOptions{})
		if err != nil {
			t.Fatalf("reading all events: %v", err)
		}
		defer iter.Close(ctx)

		events, err := coreeventstore.Collect(drainCtx, iter)
		if err != nil {
			t.Fatalf("collecting events across minimum-size windows: %v", err)
		}

		if len(events) != len(written) {
			t.Fatalf("want all %d events across minimum-size windows, got %d", len(written), len(events))
		}
	})

	// Pins what the phase-0 spike established: events written in one append batch carry
	// distinct, strictly increasing commit positions, reported identically by the append
	// return, per-stream reads, and global reads.
	t.Run("batch events carry distinct ascending positions", func(t *testing.T) {
		prefix := "g" + uuid.Must(uuid.NewV4()).String()[0:8]
		store, err := eventstore.New(client, eventstore.WithStreamPrefix(prefix))
		if err != nil {
			t.Fatalf("tc setup: failed to create EventStore: %v", err)
		}

		streamID := typeid.NewV4("batchtest")
		written, err := store.AppendStream(ctx, streamID, writableEvents(3), coreeventstore.AppendStreamOptions{})
		if err != nil {
			t.Fatalf("appending batch: %v", err)
		}

		lastPosition := int64(0)
		for i, event := range written {
			if event.GlobalPosition == nil {
				t.Fatalf("written event %d has nil global position", i)
			}
			if *event.GlobalPosition <= lastPosition {
				t.Errorf("written event %d has position %d, not greater than previous %d", i, *event.GlobalPosition, lastPosition)
			}
			lastPosition = *event.GlobalPosition
		}

		assertPositionsMatch := func(t *testing.T, name string, events []*coreeventstore.Event) {
			t.Helper()
			if len(events) != len(written) {
				t.Fatalf("%s yielded %d events, want %d", name, len(events), len(written))
			}
			for i, event := range events {
				if event.GlobalPosition == nil {
					t.Fatalf("%s event %d has nil global position", name, i)
				}
				if *event.GlobalPosition != *written[i].GlobalPosition {
					t.Errorf("%s event %d has position %d, append returned %d", name, i, *event.GlobalPosition, *written[i].GlobalPosition)
				}
			}
		}

		iter, err := store.ReadStream(ctx, streamID, coreeventstore.ReadStreamOptions{})
		if err != nil {
			t.Fatalf("reading stream: %v", err)
		}
		streamEvents, err := coreeventstore.Collect(ctx, iter)
		_ = iter.Close(ctx)
		if err != nil {
			t.Fatalf("collecting stream events: %v", err)
		}
		assertPositionsMatch(t, "stream read", streamEvents)

		iter, err = store.ReadAll(ctx, coreeventstore.ReadAllOptions{})
		if err != nil {
			t.Fatalf("reading all events: %v", err)
		}
		allEvents, err := coreeventstore.Collect(ctx, iter)
		_ = iter.Close(ctx)
		if err != nil {
			t.Fatalf("collecting all events: %v", err)
		}
		assertPositionsMatch(t, "global read", allEvents)
	})
}
