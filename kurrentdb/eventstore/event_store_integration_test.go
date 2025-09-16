package eventstore_test

import (
	"bytes"
	"errors"
	"testing"
	"time"

	kdbeventstore "github.com/go-estoria/estoria-contrib/kurrentdb/eventstore"
	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
)

var (
	now = time.Now().UTC().Add(-time.Hour)

	streamIDs = []typeid.ID{
		typeid.NewV4("stream"),
		typeid.NewV4("stream"),
		typeid.NewV4("stream"),
		typeid.NewV4("stream"),
		typeid.NewV4("stream"),
	}

	writableEvents = []*eventstore.WritableEvent{
		{Type: "event", Data: []byte(`{"foo":"one","bar":100,"baz":true}`)},
		{Type: "event", Data: []byte(`{"foo":"two","bar":200,"baz":false}`)},
		{Type: "event", Data: []byte(`{"foo":"three","bar":300,"baz":true}`)},
		{Type: "event", Data: []byte(`{"foo":"four","bar":400,"baz":false}`)},
		{Type: "event", Data: []byte(`{"foo":"five","bar":500,"baz":true}`)},
	}
)

func eventsFor(streamID typeid.ID) []*eventstore.Event {
	return []*eventstore.Event{
		{ID: typeid.NewV4("event"), StreamID: streamID, StreamVersion: 1, Timestamp: now.Add(1 * time.Second), Data: writableEvents[0].Data},
		{ID: typeid.NewV4("event"), StreamID: streamID, StreamVersion: 2, Timestamp: now.Add(2 * time.Second), Data: writableEvents[1].Data},
		{ID: typeid.NewV4("event"), StreamID: streamID, StreamVersion: 3, Timestamp: now.Add(3 * time.Second), Data: writableEvents[2].Data},
		{ID: typeid.NewV4("event"), StreamID: streamID, StreamVersion: 4, Timestamp: now.Add(4 * time.Second), Data: writableEvents[3].Data},
		{ID: typeid.NewV4("event"), StreamID: streamID, StreamVersion: 5, Timestamp: now.Add(5 * time.Second), Data: writableEvents[4].Data},
	}
}

func TestEventStore_Integration_ReadStream(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("skipping integration test")
	}

	for _, tt := range []struct {
		name         string
		withEvents   map[typeid.ID][]*eventstore.WritableEvent
		haveStreamID typeid.ID
		haveOpts     eventstore.ReadStreamOptions
		wantEvents   []*eventstore.Event
		wantErr      error
	}{
		{
			name:         "read non-existent stream",
			haveStreamID: typeid.NewV4("nonexistentstream"),
			wantErr:      eventstore.ErrStreamNotFound,
		},
		{
			name: "read stream with one event",
			withEvents: map[typeid.ID][]*eventstore.WritableEvent{
				streamIDs[0]: writableEvents[0:1],
			},
			haveStreamID: streamIDs[0],
			wantEvents:   eventsFor(streamIDs[0])[0:1],
		},
		{
			name: "read stream (default options)",
			withEvents: map[typeid.ID][]*eventstore.WritableEvent{
				streamIDs[0]: writableEvents,
			},
			haveStreamID: streamIDs[0],
			wantEvents:   eventsFor(streamIDs[0]),
		},
		{
			name: "read stream (offset)",
			withEvents: map[typeid.ID][]*eventstore.WritableEvent{
				streamIDs[0]: writableEvents,
			},
			haveStreamID: streamIDs[0],
			haveOpts:     eventstore.ReadStreamOptions{Offset: 2},
			wantEvents:   eventsFor(streamIDs[0])[2:],
		},
		{
			name: "read stream (count)",
			withEvents: map[typeid.ID][]*eventstore.WritableEvent{
				streamIDs[0]: writableEvents,
			},
			haveStreamID: streamIDs[0],
			haveOpts:     eventstore.ReadStreamOptions{Count: 2},
			wantEvents:   eventsFor(streamIDs[0])[:2],
		},
		{
			name: "read stream (offset,count)",
			withEvents: map[typeid.ID][]*eventstore.WritableEvent{
				streamIDs[0]: writableEvents,
			},
			haveStreamID: streamIDs[0],
			haveOpts:     eventstore.ReadStreamOptions{Offset: 2, Count: 2},
			wantEvents:   eventsFor(streamIDs[0])[2:4],
		},
		{
			name: "read stream (forward)",
			withEvents: map[typeid.ID][]*eventstore.WritableEvent{
				streamIDs[0]: writableEvents,
			},
			haveStreamID: streamIDs[0],
			haveOpts:     eventstore.ReadStreamOptions{Direction: eventstore.Forward},
			wantEvents:   eventsFor(streamIDs[0]),
		},
		{
			name: "read stream (reverse)",
			withEvents: map[typeid.ID][]*eventstore.WritableEvent{
				streamIDs[0]: writableEvents,
			},
			haveStreamID: streamIDs[0],
			haveOpts:     eventstore.ReadStreamOptions{Direction: eventstore.Reverse},
			wantEvents:   reversed(eventsFor(streamIDs[0])),
		},
		{
			name: "read stream (forward,offset)",
			withEvents: map[typeid.ID][]*eventstore.WritableEvent{
				streamIDs[0]: writableEvents,
			},
			haveStreamID: streamIDs[0],
			haveOpts:     eventstore.ReadStreamOptions{Direction: eventstore.Forward, Offset: 2},
			wantEvents:   eventsFor(streamIDs[0])[2:],
		},
		{
			name: "read stream (reverse,offset)",
			withEvents: map[typeid.ID][]*eventstore.WritableEvent{
				streamIDs[0]: writableEvents,
			},
			haveStreamID: streamIDs[0],
			haveOpts:     eventstore.ReadStreamOptions{Direction: eventstore.Reverse, Offset: 2},
			wantEvents:   reversed(eventsFor(streamIDs[0]))[2:],
		},
		{
			name: "read stream (forward,count)",
			withEvents: map[typeid.ID][]*eventstore.WritableEvent{
				streamIDs[0]: writableEvents,
			},
			haveStreamID: streamIDs[0],
			haveOpts:     eventstore.ReadStreamOptions{Direction: eventstore.Forward, Count: 2},
			wantEvents:   eventsFor(streamIDs[0])[:2],
		},
		{
			name: "read stream (reverse,count)",
			withEvents: map[typeid.ID][]*eventstore.WritableEvent{
				streamIDs[0]: writableEvents,
			},
			haveStreamID: streamIDs[0],
			haveOpts:     eventstore.ReadStreamOptions{Direction: eventstore.Reverse, Count: 2},
			wantEvents:   reversed(eventsFor(streamIDs[0]))[:2],
		},
		{
			name: "read stream (forward,offset,count)",
			withEvents: map[typeid.ID][]*eventstore.WritableEvent{
				streamIDs[0]: writableEvents,
			},
			haveStreamID: streamIDs[0],
			haveOpts:     eventstore.ReadStreamOptions{Direction: eventstore.Forward, Offset: 2, Count: 2},
			wantEvents:   eventsFor(streamIDs[0])[2:4],
		},
		{
			name: "read stream (reverse,offset,count)",
			withEvents: map[typeid.ID][]*eventstore.WritableEvent{
				streamIDs[0]: writableEvents,
			},
			haveStreamID: streamIDs[0],
			haveOpts:     eventstore.ReadStreamOptions{Direction: eventstore.Reverse, Offset: 2, Count: 2},
			wantEvents:   reversed(eventsFor(streamIDs[0]))[2:4],
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// spin up the Postgres container
			db, err := createKurrentContainer(t, t.Context())
			if err != nil {
				t.Fatalf("failed to create Postgres container: %v", err)
			}

			eventStore, err := kdbeventstore.New(db)
			if err != nil {
				t.Fatalf("failed to create Postgres event store: %v", err)
			}

			// setup test-specific DB state
			if len(tt.withEvents) > 0 {
				for streamID, events := range tt.withEvents {
					if err := eventStore.AppendStream(t.Context(), streamID, events, eventstore.AppendStreamOptions{}); err != nil {
						t.Fatalf("failed to setup DB state: %v", err)
					}
				}
			}

			eventsIter, err := eventStore.ReadStream(t.Context(), tt.haveStreamID, tt.haveOpts)
			if err != nil {
				if tt.wantErr == nil {
					t.Fatalf("unexpected error reading stream: %v", err)
				}
				if err.Error() != tt.wantErr.Error() {
					t.Fatalf("expected error %q, got %q", tt.wantErr.Error(), err.Error())
				}
				return
			}

			if tt.wantErr != nil {
				t.Fatalf("expected error %q, got nil", tt.wantErr.Error())
			}

			var gotEvents []*eventstore.Event
			for {
				event, err := eventsIter.Next(t.Context())
				if err != nil {
					if err == eventstore.ErrEndOfEventStream {
						break
					}
					t.Fatalf("error iterating events: %v", err)
				}
				if event == nil {
					t.Fatalf("expected event, got nil")
				}

				gotEvents = append(gotEvents, event)
			}

			if len(gotEvents) != len(tt.wantEvents) {
				for _, e := range tt.wantEvents {
					t.Logf("want event: ID=%s StreamID=%s StreamVersion=%d Data=%s", e.ID.String(), e.StreamID.String(), e.StreamVersion, string(e.Data))
				}
				for _, e := range gotEvents {
					t.Logf("got event: ID=%s StreamID=%s StreamVersion=%d Data=%s", e.ID.String(), e.StreamID.String(), e.StreamVersion, string(e.Data))
				}
				t.Fatalf("expected %d events, got %d", len(tt.wantEvents), len(gotEvents))
			}

			for i := range gotEvents {
				if gotEvents[i].ID.UUID.IsNil() {
					t.Errorf("event %d: ID is empty", i)
				}
				if gotEvents[i].ID.Type != tt.wantEvents[i].ID.Type {
					t.Errorf("event %d: expected ID type %q, got %q", i, tt.wantEvents[i].ID.Type, gotEvents[i].ID.Type)
				}
				if gotEvents[i].StreamID != tt.wantEvents[i].StreamID {
					t.Errorf("event %d: expected StreamID %q, got %q", i, tt.wantEvents[i].StreamID, gotEvents[i].StreamID)
				}
				if gotEvents[i].StreamVersion != tt.wantEvents[i].StreamVersion {
					t.Errorf("event %d: expected StreamVersion %d, got %d", i, tt.wantEvents[i].StreamVersion, gotEvents[i].StreamVersion)
				}
				if !bytes.Equal(gotEvents[i].Data, tt.wantEvents[i].Data) {
					t.Errorf("event %d: expected Data %q, got %q", i, string(tt.wantEvents[i].Data), string(gotEvents[i].Data))
				}
			}
		})
	}
}

// TestEventStore_Integration_AppendStream tests appending events to a stream
func TestEventStore_Integration_AppendStream(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("skipping integration test")
	}

	for _, tt := range []struct {
		name         string
		withEvents   map[typeid.ID][]*eventstore.WritableEvent
		haveStreamID typeid.ID
		haveOpts     eventstore.AppendStreamOptions
		haveEvents   []*eventstore.WritableEvent
		wantEvents   []*eventstore.Event
		wantErr      error
	}{
		{
			name:         "append single event (no streams exist)",
			haveStreamID: streamIDs[0],
			haveEvents:   writableEvents[0:1],
			wantEvents:   eventsFor(streamIDs[0])[0:1],
		},
		{
			name:         "append multiple events (no streams exist)",
			haveStreamID: streamIDs[0],
			haveEvents:   writableEvents,
			wantEvents:   eventsFor(streamIDs[0]),
		},
		{
			name: "append single event (non-existent stream)",
			withEvents: map[typeid.ID][]*eventstore.WritableEvent{
				streamIDs[0]: writableEvents,
			},
			haveStreamID: streamIDs[1],
			haveEvents:   writableEvents[0:1],
			wantEvents:   eventsFor(streamIDs[1])[0:1],
		},
		{
			name: "append multiple events (non-existent stream)",
			withEvents: map[typeid.ID][]*eventstore.WritableEvent{
				streamIDs[0]: writableEvents,
			},
			haveStreamID: streamIDs[1],
			haveEvents:   writableEvents,
			wantEvents:   eventsFor(streamIDs[1]),
		},
		{
			name: "append single event (existing stream)",
			withEvents: map[typeid.ID][]*eventstore.WritableEvent{
				streamIDs[0]: writableEvents,
				streamIDs[1]: writableEvents[0:2],
			},
			haveStreamID: streamIDs[1],
			haveEvents:   writableEvents[2:3],
			wantEvents:   eventsFor(streamIDs[1])[:3],
		},
		{
			name: "append multiple events (existing stream)",
			withEvents: map[typeid.ID][]*eventstore.WritableEvent{
				streamIDs[0]: writableEvents,
				streamIDs[1]: writableEvents[0:2],
			},
			haveStreamID: streamIDs[1],
			haveEvents:   writableEvents[2:3],
			wantEvents:   eventsFor(streamIDs[1])[:3],
		},
		{
			name: "append events (expected version match)",
			withEvents: map[typeid.ID][]*eventstore.WritableEvent{
				streamIDs[0]: writableEvents[0:2],
			},
			haveStreamID: streamIDs[0],
			haveEvents:   writableEvents[2:],
			haveOpts:     eventstore.AppendStreamOptions{ExpectVersion: 2},
			wantEvents:   eventsFor(streamIDs[0]),
		},
		{
			name: "append events (expected version mismatch)",
			withEvents: map[typeid.ID][]*eventstore.WritableEvent{
				streamIDs[0]: writableEvents[0:2],
			},
			haveStreamID: streamIDs[0],
			haveEvents:   writableEvents[2:],
			haveOpts:     eventstore.AppendStreamOptions{ExpectVersion: 1},
			wantErr:      eventstore.StreamVersionMismatchError{StreamID: streamIDs[0], ExpectedVersion: 1, ActualVersion: 2},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// spin up the Postgres container
			db, err := createKurrentContainer(t, t.Context())
			if err != nil {
				t.Fatalf("failed to create Postgres container: %v", err)
			}

			eventStore, err := kdbeventstore.New(db)
			if err != nil {
				t.Fatalf("failed to create Postgres event store: %v", err)
			}

			// setup test-specific DB state
			if len(tt.withEvents) > 0 {
				for streamID, events := range tt.withEvents {
					if err := eventStore.AppendStream(t.Context(), streamID, events, eventstore.AppendStreamOptions{}); err != nil {
						t.Fatalf("failed to setup DB state: %v", err)
					}
				}
			}

			err = eventStore.AppendStream(t.Context(), tt.haveStreamID, tt.haveEvents, tt.haveOpts)
			if err != nil {
				if tt.wantErr == nil {
					t.Fatalf("unexpected error reading stream: %v", err)
				}
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("expected error %q, got %q", tt.wantErr.Error(), err.Error())
				}
				return
			}

			if tt.wantErr != nil {
				t.Fatalf("expected error %q, got nil", tt.wantErr.Error())
			}

			iter, err := eventStore.ReadStream(t.Context(), tt.haveStreamID, eventstore.ReadStreamOptions{})
			if err != nil {
				t.Fatalf("unexpected error reading stream: %v", err)
			}

			var gotEvents []*eventstore.Event
			for {
				ev, err := iter.Next(t.Context())
				if err != nil {
					if err == eventstore.ErrEndOfEventStream {
						break
					}
					t.Fatalf("error iterating events: %v", err)
				}
				if ev == nil {
					t.Fatalf("expected event, got nil")
				}
				gotEvents = append(gotEvents, ev)
			}

			if len(gotEvents) != len(tt.wantEvents) {
				for _, e := range tt.wantEvents {
					t.Logf("want event: ID=%s StreamID=%s StreamVersion=%d Data=%s", e.ID.String(), e.StreamID.String(), e.StreamVersion, string(e.Data))
				}
				for _, e := range gotEvents {
					t.Logf("got event: ID=%s StreamID=%s StreamVersion=%d Data=%s", e.ID.String(), e.StreamID.String(), e.StreamVersion, string(e.Data))
				}
				t.Fatalf("expected %d events, got %d", len(tt.wantEvents), len(gotEvents))
			}

			for i := range gotEvents {
				if gotEvents[i].ID.UUID.IsNil() {
					t.Errorf("event %d: ID is empty", i)
				}
				if gotEvents[i].ID.Type != tt.wantEvents[i].ID.Type {
					t.Errorf("event %d: expected ID type %q, got %q", i, tt.wantEvents[i].ID.Type, gotEvents[i].ID.Type)
				}
				if gotEvents[i].StreamID != tt.wantEvents[i].StreamID {
					t.Errorf("event %d: expected StreamID %q, got %q", i, tt.wantEvents[i].StreamID, gotEvents[i].StreamID)
				}
				if gotEvents[i].StreamVersion != tt.wantEvents[i].StreamVersion {
					t.Errorf("event %d: expected StreamVersion %d, got %d", i, tt.wantEvents[i].StreamVersion, gotEvents[i].StreamVersion)
				}
				if !bytes.Equal(gotEvents[i].Data, tt.wantEvents[i].Data) {
					t.Errorf("event %d: expected Data %q, got %q", i, string(tt.wantEvents[i].Data), string(gotEvents[i].Data))
				}
			}
		})
	}
}
