package eventstore_test

import (
	"context"
	"fmt"
	"testing"

	es "github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
)

// writableEvents builds n simple writable events of the given type.
func writableEvents(eventType string, n int) []*es.WritableEvent {
	events := make([]*es.WritableEvent, n)
	for i := range events {
		events[i] = &es.WritableEvent{
			Type: eventType,
			Data: fmt.Appendf(nil, `{"index":%d}`, i+1),
		}
	}
	return events
}

// drain reads an iterator into a slice.
func drain(t *testing.T, ctx context.Context, iter es.StreamIterator) []*es.Event {
	t.Helper()
	events, err := es.ReadAll(ctx, iter)
	if err != nil {
		t.Fatalf("reading events: %v", err)
	}
	return events
}

func TestEventStore_Integration_ListStreams(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Parallel()

	ctx := context.Background()

	mongoClient, err := createMongoDBContainer(t, ctx)
	if err != nil {
		t.Fatalf("failed to create MongoDB container: %v", err)
	}

	t.Run("returns an empty slice when no streams exist", func(t *testing.T) {
		t.Parallel()
		store := newTestStore(t, ctx, mongoClient)

		streams, err := store.ListStreams(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(streams) != 0 {
			t.Fatalf("expected 0 streams, got %d", len(streams))
		}
	})

	t.Run("returns correct offset and global offset per stream", func(t *testing.T) {
		t.Parallel()
		store := newTestStore(t, ctx, mongoClient)

		streamA := typeid.NewV4("streamtypeA")
		streamB := typeid.NewV4("streamtypeB")

		// A gets 3 events (global 1..3), B gets 2 events (global 4..5).
		if err := store.AppendStream(ctx, streamA, writableEvents("evt", 3), es.AppendStreamOptions{}); err != nil {
			t.Fatalf("appending to A: %v", err)
		}
		if err := store.AppendStream(ctx, streamB, writableEvents("evt", 2), es.AppendStreamOptions{}); err != nil {
			t.Fatalf("appending to B: %v", err)
		}

		streams, err := store.ListStreams(ctx)
		if err != nil {
			t.Fatalf("listing streams: %v", err)
		}
		if len(streams) != 2 {
			t.Fatalf("expected 2 streams, got %d", len(streams))
		}

		byID := map[string]int64{}
		globalByID := map[string]int64{}
		for _, s := range streams {
			byID[s.StreamID.String()] = s.Offset
			globalByID[s.StreamID.String()] = s.GlobalOffset
		}

		if byID[streamA.String()] != 3 || globalByID[streamA.String()] != 3 {
			t.Errorf("stream A: expected offset 3 / global 3, got offset %d / global %d", byID[streamA.String()], globalByID[streamA.String()])
		}
		if byID[streamB.String()] != 2 || globalByID[streamB.String()] != 5 {
			t.Errorf("stream B: expected offset 2 / global 5, got offset %d / global %d", byID[streamB.String()], globalByID[streamB.String()])
		}
	})
}

func TestEventStore_Integration_ReadStream(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Parallel()

	ctx := context.Background()

	mongoClient, err := createMongoDBContainer(t, ctx)
	if err != nil {
		t.Fatalf("failed to create MongoDB container: %v", err)
	}

	store := newTestStore(t, ctx, mongoClient)

	streamID := typeid.NewV4("streamtype")
	if err := store.AppendStream(ctx, streamID, writableEvents("evt", 5), es.AppendStreamOptions{}); err != nil {
		t.Fatalf("appending: %v", err)
	}

	t.Run("forward, all", func(t *testing.T) {
		iter, err := store.ReadStream(ctx, streamID, es.ReadStreamOptions{})
		if err != nil {
			t.Fatalf("reading: %v", err)
		}
		events := drain(t, ctx, iter)
		if len(events) != 5 {
			t.Fatalf("expected 5 events, got %d", len(events))
		}
		for i, e := range events {
			if e.StreamVersion != int64(i+1) {
				t.Errorf("event %d: expected version %d, got %d", i, i+1, e.StreamVersion)
			}
		}
	})

	t.Run("forward, AfterVersion exclusive", func(t *testing.T) {
		iter, err := store.ReadStream(ctx, streamID, es.ReadStreamOptions{AfterVersion: 2})
		if err != nil {
			t.Fatalf("reading: %v", err)
		}
		events := drain(t, ctx, iter)
		if len(events) != 3 {
			t.Fatalf("expected 3 events, got %d", len(events))
		}
		if events[0].StreamVersion != 3 {
			t.Errorf("expected first version 3, got %d", events[0].StreamVersion)
		}
	})

	t.Run("forward, Count", func(t *testing.T) {
		iter, err := store.ReadStream(ctx, streamID, es.ReadStreamOptions{Count: 2})
		if err != nil {
			t.Fatalf("reading: %v", err)
		}
		events := drain(t, ctx, iter)
		if len(events) != 2 {
			t.Fatalf("expected 2 events, got %d", len(events))
		}
	})

	t.Run("reverse, all", func(t *testing.T) {
		iter, err := store.ReadStream(ctx, streamID, es.ReadStreamOptions{Direction: es.Reverse})
		if err != nil {
			t.Fatalf("reading: %v", err)
		}
		events := drain(t, ctx, iter)
		if len(events) != 5 {
			t.Fatalf("expected 5 events, got %d", len(events))
		}
		if events[0].StreamVersion != 5 {
			t.Errorf("expected first version 5, got %d", events[0].StreamVersion)
		}
	})

	t.Run("reverse, AfterVersion inclusive", func(t *testing.T) {
		iter, err := store.ReadStream(ctx, streamID, es.ReadStreamOptions{Direction: es.Reverse, AfterVersion: 3})
		if err != nil {
			t.Fatalf("reading: %v", err)
		}
		events := drain(t, ctx, iter)
		if len(events) != 3 {
			t.Fatalf("expected 3 events, got %d", len(events))
		}
		if events[0].StreamVersion != 3 || events[2].StreamVersion != 1 {
			t.Errorf("expected versions 3..1, got %d..%d", events[0].StreamVersion, events[2].StreamVersion)
		}
	})
}

func TestEventStore_Integration_ReadAll(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Parallel()

	ctx := context.Background()

	mongoClient, err := createMongoDBContainer(t, ctx)
	if err != nil {
		t.Fatalf("failed to create MongoDB container: %v", err)
	}

	store := newTestStore(t, ctx, mongoClient)

	streamA := typeid.NewV4("streamA")
	streamB := typeid.NewV4("streamB")

	// Interleave appends across streams: A(2), B(2), A(1) => global 1..5.
	if err := store.AppendStream(ctx, streamA, writableEvents("evt", 2), es.AppendStreamOptions{}); err != nil {
		t.Fatalf("append A: %v", err)
	}
	if err := store.AppendStream(ctx, streamB, writableEvents("evt", 2), es.AppendStreamOptions{}); err != nil {
		t.Fatalf("append B: %v", err)
	}
	if err := store.AppendStream(ctx, streamA, writableEvents("evt", 1), es.AppendStreamOptions{ExpectVersion: es.VersionPtr(2)}); err != nil {
		t.Fatalf("append A2: %v", err)
	}

	t.Run("global order, dense and gap-free", func(t *testing.T) {
		iter, err := store.ReadAll(ctx, es.ReadStreamOptions{})
		if err != nil {
			t.Fatalf("reading all: %v", err)
		}
		events := drain(t, ctx, iter)
		if len(events) != 5 {
			t.Fatalf("expected 5 events, got %d", len(events))
		}
		for i, e := range events {
			if e.GlobalPosition == nil {
				t.Fatalf("event %d has nil global position", i)
			}
			if *e.GlobalPosition != int64(i+1) {
				t.Errorf("event %d: expected global position %d, got %d", i, i+1, *e.GlobalPosition)
			}
		}
	})

	t.Run("AfterVersion and Count", func(t *testing.T) {
		iter, err := store.ReadAll(ctx, es.ReadStreamOptions{AfterVersion: 2, Count: 2})
		if err != nil {
			t.Fatalf("reading all: %v", err)
		}
		events := drain(t, ctx, iter)
		if len(events) != 2 {
			t.Fatalf("expected 2 events, got %d", len(events))
		}
		if *events[0].GlobalPosition != 3 || *events[1].GlobalPosition != 4 {
			t.Errorf("expected global positions 3,4, got %d,%d", *events[0].GlobalPosition, *events[1].GlobalPosition)
		}
	})
}

func TestEventStore_Integration_AppendEdgeCases(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Parallel()

	ctx := context.Background()

	mongoClient, err := createMongoDBContainer(t, ctx)
	if err != nil {
		t.Fatalf("failed to create MongoDB container: %v", err)
	}

	store := newTestStore(t, ctx, mongoClient)

	t.Run("empty append is a no-op", func(t *testing.T) {
		streamID := typeid.NewV4("emptytype")
		if err := store.AppendStream(ctx, streamID, nil, es.AppendStreamOptions{}); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		streams, err := store.ListStreams(ctx)
		if err != nil {
			t.Fatalf("listing streams: %v", err)
		}
		for _, s := range streams {
			if s.StreamID == streamID {
				t.Fatalf("empty append created a stream")
			}
		}
	})

	t.Run("metadata round-trips", func(t *testing.T) {
		streamID := typeid.NewV4("metatype")
		events := []*es.WritableEvent{{
			Type:     "evt",
			Data:     []byte(`{"x":1}`),
			Metadata: map[string]string{"k": "v", "a": "b"},
		}}
		if err := store.AppendStream(ctx, streamID, events, es.AppendStreamOptions{}); err != nil {
			t.Fatalf("appending: %v", err)
		}
		iter, err := store.ReadStream(ctx, streamID, es.ReadStreamOptions{})
		if err != nil {
			t.Fatalf("reading: %v", err)
		}
		read := drain(t, ctx, iter)
		if len(read) != 1 {
			t.Fatalf("expected 1 event, got %d", len(read))
		}
		if read[0].Metadata["k"] != "v" || read[0].Metadata["a"] != "b" {
			t.Errorf("metadata did not round-trip: %v", read[0].Metadata)
		}
	})

	t.Run("ExpectVersion and StreamMustNotExist are mutually exclusive", func(t *testing.T) {
		streamID := typeid.NewV4("exclusivetype")
		opts := es.AppendStreamOptions{ExpectVersion: es.VersionPtr(0), StreamMustNotExist: true}
		if err := store.AppendStream(ctx, streamID, writableEvents("evt", 1), opts); err == nil {
			t.Fatalf("expected an error, got nil")
		}
	})
}
