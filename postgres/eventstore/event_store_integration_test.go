package eventstore_test

import (
	"errors"
	"testing"
	"time"

	pgeventstore "github.com/go-estoria/estoria-contrib/postgres/eventstore"
	"github.com/go-estoria/estoria-contrib/postgres/eventstore/strategy"
	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
)

const (
	testStrategyDefault = "default strategy"
)

var (
	now = time.Now().UTC().Add(-time.Hour)

	streamIDs = []typeid.UUID{
		must(typeid.NewUUID("stream_f78053db-5874-457c-8cf7-1e1bb524efba")),
		must(typeid.NewUUID("stream_87b29e1b-463f-469a-a3cb-5bb0598d64c0")),
		must(typeid.NewUUID("stream_1171f763-56c7-45c6-8460-d54d866a5660")),
		must(typeid.NewUUID("stream_eb342cfb-77f0-433a-979c-8ae0f435e4fa")),
		must(typeid.NewUUID("stream_fe1701cb-5e5c-4ef4-b031-250e93adaa3c")),
	}

	eventIDs = []typeid.UUID{
		must(typeid.NewUUID("event_7841ba8b-1e6e-4a96-a3cf-fce9c1c262b7")),
		must(typeid.NewUUID("event_072775fe-d52a-4940-80e7-0d990574df9d")),
		must(typeid.NewUUID("event_c3d6204b-40d7-40b2-8f70-a53882727fee")),
		must(typeid.NewUUID("event_0430d522-909b-4b15-8207-c4ea4623cca3")),
		must(typeid.NewUUID("event_7c187c90-b726-43d4-a273-b3c03e6ed096")),
	}

	writableEvents = []*eventstore.WritableEvent{
		{ID: eventIDs[0], Timestamp: now.Add(1 * time.Second), Data: []byte(`{"foo":"one","bar":100,"baz":true}`)},
		{ID: eventIDs[1], Timestamp: now.Add(2 * time.Second), Data: []byte(`{"foo":"two","bar":200,"baz":false}`)},
		{ID: eventIDs[2], Timestamp: now.Add(3 * time.Second), Data: []byte(`{"foo":"three","bar":300,"baz":true}`)},
		{ID: eventIDs[3], Timestamp: now.Add(4 * time.Second), Data: []byte(`{"foo":"four","bar":400,"baz":false}`)},
		{ID: eventIDs[4], Timestamp: now.Add(5 * time.Second), Data: []byte(`{"foo":"five","bar":500,"baz":true}`)},
	}
)

func eventsFor(streamID typeid.UUID) []*eventstore.Event {
	return []*eventstore.Event{
		{ID: eventIDs[0], StreamID: streamID, StreamVersion: 1, Timestamp: now.Add(1 * time.Second), Data: []byte(`{"foo":"one","bar":100,"baz":true}`)},
		{ID: eventIDs[1], StreamID: streamID, StreamVersion: 2, Timestamp: now.Add(2 * time.Second), Data: []byte(`{"foo":"two","bar":200,"baz":false}`)},
		{ID: eventIDs[2], StreamID: streamID, StreamVersion: 3, Timestamp: now.Add(3 * time.Second), Data: []byte(`{"foo":"three","bar":300,"baz":true}`)},
		{ID: eventIDs[3], StreamID: streamID, StreamVersion: 4, Timestamp: now.Add(4 * time.Second), Data: []byte(`{"foo":"four","bar":400,"baz":false}`)},
		{ID: eventIDs[4], StreamID: streamID, StreamVersion: 5, Timestamp: now.Add(5 * time.Second), Data: []byte(`{"foo":"five","bar":500,"baz":true}`)},
	}
}

func TestEventStore_Integration_ReadStream(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("skipping integration test")
	}

	for _, tStrat := range []struct {
		name   string
		desc   string
		create func(*testing.T) pgeventstore.Strategy
	}{
		{
			name: testStrategyDefault,
			desc: "default options",
			create: func(*testing.T) pgeventstore.Strategy {
				t.Helper()
				return must(strategy.NewDefaultStrategy())
			},
		},
		{
			name: testStrategyDefault,
			desc: "custom table names",
			create: func(t *testing.T) pgeventstore.Strategy {
				t.Helper()
				return must(strategy.NewDefaultStrategy(
					strategy.WithEventsTableName("event"),
					strategy.WithStreamsTableName("stream"),
				))
			},
		},
	} {
		for _, tt := range []struct {
			name         string
			withEvents   map[typeid.UUID][]*eventstore.WritableEvent
			haveStreamID typeid.UUID
			haveOpts     eventstore.ReadStreamOptions
			wantEvents   []*eventstore.Event
			wantErr      error
		}{
			{
				name:         "read non-existent stream",
				haveStreamID: must(typeid.NewUUID("nonexistentstream")),
				wantErr:      eventstore.ErrStreamNotFound,
			},
			{
				name: "read stream with one event",
				withEvents: map[typeid.UUID][]*eventstore.WritableEvent{
					streamIDs[0]: writableEvents[0:1],
				},
				haveStreamID: streamIDs[0],
				wantEvents:   eventsFor(streamIDs[0])[0:1],
			},
			{
				name: "read stream (default options)",
				withEvents: map[typeid.UUID][]*eventstore.WritableEvent{
					streamIDs[0]: writableEvents,
				},
				haveStreamID: streamIDs[0],
				wantEvents:   eventsFor(streamIDs[0]),
			},
			{
				name: "read stream (offset)",
				withEvents: map[typeid.UUID][]*eventstore.WritableEvent{
					streamIDs[0]: writableEvents,
				},
				haveStreamID: streamIDs[0],
				haveOpts:     eventstore.ReadStreamOptions{Offset: 2},
				wantEvents:   eventsFor(streamIDs[0])[2:],
			},
			{
				name: "read stream (count)",
				withEvents: map[typeid.UUID][]*eventstore.WritableEvent{
					streamIDs[0]: writableEvents,
				},
				haveStreamID: streamIDs[0],
				haveOpts:     eventstore.ReadStreamOptions{Count: 2},
				wantEvents:   eventsFor(streamIDs[0])[:2],
			},
			{
				name: "read stream (offset,count)",
				withEvents: map[typeid.UUID][]*eventstore.WritableEvent{
					streamIDs[0]: writableEvents,
				},
				haveStreamID: streamIDs[0],
				haveOpts:     eventstore.ReadStreamOptions{Offset: 2, Count: 2},
				wantEvents:   eventsFor(streamIDs[0])[2:4],
			},
			{
				name: "read stream (forward)",
				withEvents: map[typeid.UUID][]*eventstore.WritableEvent{
					streamIDs[0]: writableEvents,
				},
				haveStreamID: streamIDs[0],
				haveOpts:     eventstore.ReadStreamOptions{Direction: eventstore.Forward},
				wantEvents:   eventsFor(streamIDs[0]),
			},
			{
				name: "read stream (reverse)",
				withEvents: map[typeid.UUID][]*eventstore.WritableEvent{
					streamIDs[0]: writableEvents,
				},
				haveStreamID: streamIDs[0],
				haveOpts:     eventstore.ReadStreamOptions{Direction: eventstore.Reverse},
				wantEvents:   reversed(eventsFor(streamIDs[0])),
			},
			{
				name: "read stream (forward,offset)",
				withEvents: map[typeid.UUID][]*eventstore.WritableEvent{
					streamIDs[0]: writableEvents,
				},
				haveStreamID: streamIDs[0],
				haveOpts:     eventstore.ReadStreamOptions{Direction: eventstore.Forward, Offset: 2},
				wantEvents:   eventsFor(streamIDs[0])[2:],
			},
			{
				name: "read stream (reverse,offset)",
				withEvents: map[typeid.UUID][]*eventstore.WritableEvent{
					streamIDs[0]: writableEvents,
				},
				haveStreamID: streamIDs[0],
				haveOpts:     eventstore.ReadStreamOptions{Direction: eventstore.Reverse, Offset: 2},
				wantEvents:   reversed(eventsFor(streamIDs[0]))[2:],
			},
			{
				name: "read stream (forward,count)",
				withEvents: map[typeid.UUID][]*eventstore.WritableEvent{
					streamIDs[0]: writableEvents,
				},
				haveStreamID: streamIDs[0],
				haveOpts:     eventstore.ReadStreamOptions{Direction: eventstore.Forward, Count: 2},
				wantEvents:   eventsFor(streamIDs[0])[:2],
			},
			{
				name: "read stream (reverse,count)",
				withEvents: map[typeid.UUID][]*eventstore.WritableEvent{
					streamIDs[0]: writableEvents,
				},
				haveStreamID: streamIDs[0],
				haveOpts:     eventstore.ReadStreamOptions{Direction: eventstore.Reverse, Count: 2},
				wantEvents:   reversed(eventsFor(streamIDs[0]))[:2],
			},
			{
				name: "read stream (forward,offset,count)",
				withEvents: map[typeid.UUID][]*eventstore.WritableEvent{
					streamIDs[0]: writableEvents,
				},
				haveStreamID: streamIDs[0],
				haveOpts:     eventstore.ReadStreamOptions{Direction: eventstore.Forward, Offset: 2, Count: 2},
				wantEvents:   eventsFor(streamIDs[0])[2:4],
			},
			{
				name: "read stream (reverse,offset,count)",
				withEvents: map[typeid.UUID][]*eventstore.WritableEvent{
					streamIDs[0]: writableEvents,
				},
				haveStreamID: streamIDs[0],
				haveOpts:     eventstore.ReadStreamOptions{Direction: eventstore.Reverse, Offset: 2, Count: 2},
				wantEvents:   reversed(eventsFor(streamIDs[0]))[2:4],
			},
		} {
			t.Run(tStrat.name+"_"+tStrat.desc+"_"+tt.name, func(t *testing.T) {
				t.Parallel()

				// spin up the Postgres container
				db, err := createPostgresContainer(t, t.Context())
				if err != nil {
					t.Fatalf("failed to create Postgres container: %v", err)
				}

				// create the strategy and initialize the DB schema
				s := tStrat.create(t)
				if _, err := db.ExecContext(t.Context(), s.Schema()); err != nil {
					t.Fatalf("failed to create DB schema: %v", err)
				}

				eventStore, err := pgeventstore.New(db, pgeventstore.WithStrategy(s))
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
					if gotEvents[i].ID.TypeName() != tt.wantEvents[i].ID.TypeName() {
						t.Errorf("event %d: expected type %q, got %q", i, tt.wantEvents[i].ID.TypeName(), gotEvents[i].ID.TypeName())
					}
					if gotEvents[i].ID.Value() != tt.wantEvents[i].ID.Value() {
						t.Errorf("event %d: expected ID %q, got %q", i, tt.wantEvents[i].ID.Value(), gotEvents[i].ID.Value())
					}
					if gotEvents[i].StreamID != tt.wantEvents[i].StreamID {
						t.Errorf("event %d: expected StreamID %q, got %q", i, tt.wantEvents[i].StreamID, gotEvents[i].StreamID)
					}
					if gotEvents[i].StreamVersion != tt.wantEvents[i].StreamVersion {
						t.Errorf("event %d: expected StreamVersion %d, got %d", i, tt.wantEvents[i].StreamVersion, gotEvents[i].StreamVersion)
					}
					if string(gotEvents[i].Data) != string(tt.wantEvents[i].Data) {
						t.Errorf("event %d: expected Data %q, got %q", i, string(tt.wantEvents[i].Data), string(gotEvents[i].Data))
					}
				}
			})
		}
	}
}

// TestEventStore_Integration_AppendStream tests appending events to a stream
func TestEventStore_Integration_AppendStream(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("skipping integration test")
	}

	for _, tStrat := range []struct {
		name   string
		desc   string
		create func(*testing.T) pgeventstore.Strategy
	}{
		{
			name: testStrategyDefault,
			desc: "default options",
			create: func(*testing.T) pgeventstore.Strategy {
				t.Helper()
				return must(strategy.NewDefaultStrategy())
			},
		},
		{
			name: testStrategyDefault,
			desc: "custom table names",
			create: func(t *testing.T) pgeventstore.Strategy {
				t.Helper()
				return must(strategy.NewDefaultStrategy(
					strategy.WithEventsTableName("event"),
					strategy.WithStreamsTableName("stream"),
				))
			},
		},
	} {
		for _, tt := range []struct {
			name         string
			withEvents   map[typeid.UUID][]*eventstore.WritableEvent
			haveStreamID typeid.UUID
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
				withEvents: map[typeid.UUID][]*eventstore.WritableEvent{
					streamIDs[0]: writableEvents,
				},
				haveStreamID: streamIDs[1],
				haveEvents:   writableEvents[0:1],
				wantEvents:   eventsFor(streamIDs[1])[0:1],
			},
			{
				name: "append multiple events (non-existent stream)",
				withEvents: map[typeid.UUID][]*eventstore.WritableEvent{
					streamIDs[0]: writableEvents,
				},
				haveStreamID: streamIDs[1],
				haveEvents:   writableEvents,
				wantEvents:   eventsFor(streamIDs[1]),
			},
			{
				name: "append single event (existing stream)",
				withEvents: map[typeid.UUID][]*eventstore.WritableEvent{
					streamIDs[0]: writableEvents,
					streamIDs[1]: writableEvents[0:2],
				},
				haveStreamID: streamIDs[1],
				haveEvents:   writableEvents[2:3],
				wantEvents:   eventsFor(streamIDs[1])[:3],
			},
			{
				name: "append multiple events (existing stream)",
				withEvents: map[typeid.UUID][]*eventstore.WritableEvent{
					streamIDs[0]: writableEvents,
					streamIDs[1]: writableEvents[0:2],
				},
				haveStreamID: streamIDs[1],
				haveEvents:   writableEvents[2:3],
				wantEvents:   eventsFor(streamIDs[1])[:3],
			},
			{
				name: "append events (expected version match)",
				withEvents: map[typeid.UUID][]*eventstore.WritableEvent{
					streamIDs[0]: writableEvents[0:2],
				},
				haveStreamID: streamIDs[0],
				haveEvents:   writableEvents[2:],
				haveOpts:     eventstore.AppendStreamOptions{ExpectVersion: 2},
				wantEvents:   eventsFor(streamIDs[0]),
			},
			{
				name: "append events (expected version mismatch)",
				withEvents: map[typeid.UUID][]*eventstore.WritableEvent{
					streamIDs[0]: writableEvents[0:2],
				},
				haveStreamID: streamIDs[0],
				haveEvents:   writableEvents[2:],
				haveOpts:     eventstore.AppendStreamOptions{ExpectVersion: 1},
				wantErr:      eventstore.StreamVersionMismatchError{StreamID: streamIDs[0], ExpectedVersion: 1, ActualVersion: 2},
			},
		} {
			t.Run(tStrat.name+"_"+tStrat.desc+"_"+tt.name, func(t *testing.T) {
				t.Parallel()

				// spin up the Postgres container
				db, err := createPostgresContainer(t, t.Context())
				if err != nil {
					t.Fatalf("failed to create Postgres container: %v", err)
				}

				// create the strategy and initialize the DB schema
				s := tStrat.create(t)
				if _, err := db.ExecContext(t.Context(), s.Schema()); err != nil {
					t.Fatalf("failed to create DB schema: %v", err)
				}

				eventStore, err := pgeventstore.New(db, pgeventstore.WithStrategy(s))
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
					if gotEvents[i].ID.TypeName() != tt.wantEvents[i].ID.TypeName() {
						t.Errorf("event %d: expected type %q, got %q", i, tt.wantEvents[i].ID.TypeName(), gotEvents[i].ID.TypeName())
					}
					if gotEvents[i].ID.Value() != tt.wantEvents[i].ID.Value() {
						t.Errorf("event %d: expected ID %q, got %q", i, tt.wantEvents[i].ID.Value(), gotEvents[i].ID.Value())
					}
					if gotEvents[i].StreamID != tt.wantEvents[i].StreamID {
						t.Errorf("event %d: expected StreamID %q, got %q", i, tt.wantEvents[i].StreamID, gotEvents[i].StreamID)
					}
					if gotEvents[i].StreamVersion != tt.wantEvents[i].StreamVersion {
						t.Errorf("event %d: expected StreamVersion %d, got %d", i, tt.wantEvents[i].StreamVersion, gotEvents[i].StreamVersion)
					}
					if string(gotEvents[i].Data) != string(tt.wantEvents[i].Data) {
						t.Errorf("event %d: expected Data %q, got %q", i, string(tt.wantEvents[i].Data), string(gotEvents[i].Data))
					}
				}
			})
		}
	}
}
