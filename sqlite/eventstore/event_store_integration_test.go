package eventstore_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	sqliteeventstore "github.com/go-estoria/estoria-contrib/sqlite/eventstore"
	"github.com/go-estoria/estoria-contrib/sqlite/eventstore/strategy"
	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
)

const (
	testStrategyDefault = "default strategy"
)

var (
	now = time.Now().UTC().Add(-time.Hour)

	streamIDs = []typeid.ID{
		typeid.NewV4("stream_f78053db-5874-457c-8cf7-1e1bb524efba"),
		typeid.NewV4("stream_87b29e1b-463f-469a-a3cb-5bb0598d64c0"),
		typeid.NewV4("stream_1171f763-56c7-45c6-8460-d54d866a5660"),
		typeid.NewV4("stream_eb342cfb-77f0-433a-979c-8ae0f435e4fa"),
		typeid.NewV4("stream_fe1701cb-5e5c-4ef4-b031-250e93adaa3c"),
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

	for _, tStrat := range []struct {
		name   string
		desc   string
		create func(*testing.T) sqliteeventstore.Strategy
	}{
		{
			name: testStrategyDefault,
			desc: "default options",
			create: func(*testing.T) sqliteeventstore.Strategy {
				t.Helper()
				return must(strategy.NewDefaultStrategy())
			},
		},
		{
			name: testStrategyDefault,
			desc: "custom table names",
			create: func(t *testing.T) sqliteeventstore.Strategy {
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
				name: "read stream (after_version)",
				withEvents: map[typeid.ID][]*eventstore.WritableEvent{
					streamIDs[0]: writableEvents,
				},
				haveStreamID: streamIDs[0],
				haveOpts:     eventstore.ReadStreamOptions{AfterVersion: 2},
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
				name: "read stream (after_version,count)",
				withEvents: map[typeid.ID][]*eventstore.WritableEvent{
					streamIDs[0]: writableEvents,
				},
				haveStreamID: streamIDs[0],
				haveOpts:     eventstore.ReadStreamOptions{AfterVersion: 2, Count: 2},
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
				name: "read stream (forward,after_version)",
				withEvents: map[typeid.ID][]*eventstore.WritableEvent{
					streamIDs[0]: writableEvents,
				},
				haveStreamID: streamIDs[0],
				haveOpts:     eventstore.ReadStreamOptions{Direction: eventstore.Forward, AfterVersion: 2},
				wantEvents:   eventsFor(streamIDs[0])[2:],
			},
			{
				name: "read stream (reverse,after_version)",
				withEvents: map[typeid.ID][]*eventstore.WritableEvent{
					streamIDs[0]: writableEvents,
				},
				haveStreamID: streamIDs[0],
				haveOpts:     eventstore.ReadStreamOptions{Direction: eventstore.Reverse, AfterVersion: 2},
				wantEvents:   reversed(eventsFor(streamIDs[0])[:2]),
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
				name: "read stream (forward,after_version,count)",
				withEvents: map[typeid.ID][]*eventstore.WritableEvent{
					streamIDs[0]: writableEvents,
				},
				haveStreamID: streamIDs[0],
				haveOpts:     eventstore.ReadStreamOptions{Direction: eventstore.Forward, AfterVersion: 2, Count: 2},
				wantEvents:   eventsFor(streamIDs[0])[2:4],
			},
			{
				name: "read stream (reverse,after_version,count)",
				withEvents: map[typeid.ID][]*eventstore.WritableEvent{
					streamIDs[0]: writableEvents,
				},
				haveStreamID: streamIDs[0],
				haveOpts:     eventstore.ReadStreamOptions{Direction: eventstore.Reverse, AfterVersion: 2, Count: 2},
				wantEvents:   reversed(eventsFor(streamIDs[0])[:2]),
			},
		} {
			t.Run(tStrat.name+"_"+tStrat.desc+"_"+tt.name, func(t *testing.T) {
				t.Parallel()

				db := newSQLiteDB(t)

				// create the strategy and initialize the DB schema
				s := tStrat.create(t)
				if _, err := db.ExecContext(t.Context(), s.Schema()); err != nil {
					t.Fatalf("failed to create DB schema: %v", err)
				}

				eventStore, err := sqliteeventstore.New(db, sqliteeventstore.WithStrategy(s))
				if err != nil {
					t.Fatalf("failed to create SQLite event store: %v", err)
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
					if eq, err := jsonEq(gotEvents[i].Data, tt.wantEvents[i].Data); err != nil {
						t.Errorf("event %d: error comparing JSON data: %v", i, err)
					} else if !eq {
						t.Errorf("event %d: expected Data %q, got %q", i, string(tt.wantEvents[i].Data), string(gotEvents[i].Data))
					}
				}
			})
		}
	}
}

// TestEventStore_Integration_ProductionReadiness tests production-readiness behaviors,
// including guard conditions, iterator state, and constraint handling.
func TestEventStore_Integration_ProductionReadiness(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("skipping integration test")
	}

	newStore := func(t *testing.T, opts ...sqliteeventstore.EventStoreOption) *sqliteeventstore.EventStore {
		t.Helper()

		db := newSQLiteDB(t)

		strat := must(strategy.NewDefaultStrategy())
		if _, err := db.ExecContext(t.Context(), strat.Schema()); err != nil {
			t.Fatalf("failed to create DB schema: %v", err)
		}

		baseOpts := []sqliteeventstore.EventStoreOption{sqliteeventstore.WithStrategy(strat)}
		baseOpts = append(baseOpts, opts...)

		es, err := sqliteeventstore.New(db, baseOpts...)
		if err != nil {
			t.Fatalf("failed to create event store: %v", err)
		}

		return es
	}

	t.Run("append_empty_events_is_noop", func(t *testing.T) {
		t.Parallel()

		es := newStore(t)
		streamID := typeid.NewV4("test")

		err := es.AppendStream(t.Context(), streamID, []*eventstore.WritableEvent{}, eventstore.AppendStreamOptions{})
		if err != nil {
			t.Fatalf("AppendStream with empty slice returned unexpected error: %v", err)
		}

		_, err = es.ReadStream(t.Context(), streamID, eventstore.ReadStreamOptions{})
		if !errors.Is(err, eventstore.ErrStreamNotFound) {
			t.Errorf("expected ErrStreamNotFound after no-op append, got: %v", err)
		}
	})

	t.Run("next_after_close_returns_iterator_closed", func(t *testing.T) {
		t.Parallel()

		es := newStore(t)
		streamID := typeid.NewV4("test")

		if err := es.AppendStream(t.Context(), streamID, writableEvents[0:1], eventstore.AppendStreamOptions{}); err != nil {
			t.Fatalf("AppendStream failed: %v", err)
		}

		iter, err := es.ReadStream(t.Context(), streamID, eventstore.ReadStreamOptions{})
		if err != nil {
			t.Fatalf("ReadStream failed: %v", err)
		}

		if err := iter.Close(t.Context()); err != nil {
			t.Fatalf("Close failed: %v", err)
		}

		_, err = iter.Next(t.Context())
		if !errors.Is(err, eventstore.ErrStreamIteratorClosed) {
			t.Errorf("expected ErrStreamIteratorClosed after Close(), got: %v", err)
		}
	})

	t.Run("read_all_empty_store_returns_empty_iterator", func(t *testing.T) {
		t.Parallel()

		es := newStore(t)

		iter, err := es.ReadAll(t.Context(), eventstore.ReadStreamOptions{})
		if err != nil {
			t.Fatalf("ReadAll on empty store returned unexpected error: %v", err)
		}
		if iter == nil {
			t.Fatal("ReadAll on empty store returned nil iterator")
		}

		_, err = iter.Next(t.Context())
		if !errors.Is(err, eventstore.ErrEndOfEventStream) {
			t.Errorf("expected ErrEndOfEventStream from empty iterator, got: %v", err)
		}
	})

	t.Run("append_with_empty_stream_type_returns_error", func(t *testing.T) {
		t.Parallel()

		es := newStore(t)
		emptyTypeID := typeid.New("", typeid.NewV4("ignored").UUID)

		err := es.AppendStream(t.Context(), emptyTypeID, writableEvents[0:1], eventstore.AppendStreamOptions{})
		if err == nil {
			t.Fatal("AppendStream with empty stream type returned nil error, expected an error")
		}
		const wantSubstr = "stream type is required"
		if !containsSubstring(err.Error(), wantSubstr) {
			t.Errorf("expected error to contain %q, got: %v", wantSubstr, err)
		}
	})

	t.Run("read_with_empty_stream_type_returns_error", func(t *testing.T) {
		t.Parallel()

		es := newStore(t)
		emptyTypeID := typeid.New("", typeid.NewV4("ignored").UUID)

		_, err := es.ReadStream(t.Context(), emptyTypeID, eventstore.ReadStreamOptions{})
		if err == nil {
			t.Fatal("ReadStream with empty stream type returned nil error, expected an error")
		}
		const wantSubstr = "stream type is required"
		if !containsSubstring(err.Error(), wantSubstr) {
			t.Errorf("expected error to contain %q, got: %v", wantSubstr, err)
		}
	})

	t.Run("append_exceeding_max_data_bytes_returns_error", func(t *testing.T) {
		t.Parallel()

		const limit = 100
		es := newStore(t, sqliteeventstore.WithMaxEventDataBytes(limit))
		streamID := typeid.NewV4("test")

		largeData := make([]byte, limit+1)
		for i := range largeData {
			largeData[i] = 'x'
		}

		oversizedEvent := []*eventstore.WritableEvent{{Type: "event", Data: largeData}}
		err := es.AppendStream(t.Context(), streamID, oversizedEvent, eventstore.AppendStreamOptions{})
		if err == nil {
			t.Fatal("AppendStream with oversized data returned nil error, expected an error")
		}

		smallData := []byte(`{"key":"value"}`)
		smallEvent := []*eventstore.WritableEvent{{Type: "event", Data: smallData}}
		if err := es.AppendStream(t.Context(), streamID, smallEvent, eventstore.AppendStreamOptions{}); err != nil {
			t.Errorf("AppendStream with data within limit returned unexpected error: %v", err)
		}
	})

	t.Run("list_streams_with_cancelled_context", func(t *testing.T) {
		t.Parallel()

		es := newStore(t)
		streamID := typeid.NewV4("test")

		if err := es.AppendStream(t.Context(), streamID, writableEvents[0:1], eventstore.AppendStreamOptions{}); err != nil {
			t.Fatalf("AppendStream failed: %v", err)
		}

		cancelledCtx, cancel := context.WithCancel(t.Context())
		cancel()

		_, err := es.ListStreams(cancelledCtx)
		if err == nil {
			t.Fatal("ListStreams with cancelled context returned nil error, expected an error")
		}
	})

	t.Run("expect_version_mismatch_returns_typed_error", func(t *testing.T) {
		t.Parallel()

		es := newStore(t)
		streamID := typeid.NewV4("test")

		if err := es.AppendStream(t.Context(), streamID, writableEvents[0:2], eventstore.AppendStreamOptions{}); err != nil {
			t.Fatalf("initial AppendStream failed: %v", err)
		}

		err := es.AppendStream(t.Context(), streamID, writableEvents[2:3], eventstore.AppendStreamOptions{ExpectVersion: eventstore.VersionPtr(1)})
		if err == nil {
			t.Fatal("AppendStream with wrong ExpectVersion returned nil error, expected StreamVersionMismatchError")
		}

		var mismatch eventstore.StreamVersionMismatchError
		if !errors.As(err, &mismatch) {
			t.Errorf("expected StreamVersionMismatchError, got: %T: %v", err, err)
		}
	})

	t.Run("version_mismatch_does_not_corrupt_stream", func(t *testing.T) {
		t.Parallel()

		es := newStore(t)
		streamID := typeid.NewV4("test")

		if err := es.AppendStream(t.Context(), streamID, writableEvents[0:3], eventstore.AppendStreamOptions{}); err != nil {
			t.Fatalf("initial AppendStream failed: %v", err)
		}

		err := es.AppendStream(t.Context(), streamID, writableEvents[3:4], eventstore.AppendStreamOptions{ExpectVersion: eventstore.VersionPtr(1)})
		if err == nil {
			t.Fatal("AppendStream with wrong ExpectVersion returned nil error, expected an error")
		}
		var mismatch eventstore.StreamVersionMismatchError
		if !errors.As(err, &mismatch) {
			t.Errorf("expected StreamVersionMismatchError from stale append, got: %T: %v", err, err)
		}

		if err := es.AppendStream(t.Context(), streamID, writableEvents[3:4], eventstore.AppendStreamOptions{ExpectVersion: eventstore.VersionPtr(3)}); err != nil {
			t.Fatalf("corrected AppendStream failed after previous mismatch: %v", err)
		}

		iter, err := es.ReadStream(t.Context(), streamID, eventstore.ReadStreamOptions{})
		if err != nil {
			t.Fatalf("ReadStream failed: %v", err)
		}

		var gotEvents []*eventstore.Event
		for {
			ev, err := iter.Next(t.Context())
			if errors.Is(err, eventstore.ErrEndOfEventStream) {
				break
			}
			if err != nil {
				t.Fatalf("unexpected error during iteration: %v", err)
			}
			gotEvents = append(gotEvents, ev)
		}

		const wantCount = 4
		if len(gotEvents) != wantCount {
			t.Errorf("expected %d events after corruption check, got %d", wantCount, len(gotEvents))
		}

		for i, ev := range gotEvents {
			wantVersion := int64(i + 1)
			if ev.StreamVersion != wantVersion {
				t.Errorf("event %d: expected StreamVersion %d, got %d", i, wantVersion, ev.StreamVersion)
			}
		}
	})

	t.Run("stream_must_not_exist_blocks_existing_stream", func(t *testing.T) {
		t.Parallel()

		es := newStore(t)
		streamID := typeid.NewV4("test")

		if err := es.AppendStream(t.Context(), streamID, writableEvents[0:1], eventstore.AppendStreamOptions{}); err != nil {
			t.Fatalf("initial AppendStream failed: %v", err)
		}

		err := es.AppendStream(t.Context(), streamID, writableEvents[1:2], eventstore.AppendStreamOptions{StreamMustNotExist: true})
		if err == nil {
			t.Fatal("AppendStream with StreamMustNotExist on existing stream returned nil error")
		}

		var mismatch eventstore.StreamVersionMismatchError
		if !errors.As(err, &mismatch) {
			t.Errorf("expected StreamVersionMismatchError, got: %T: %v", err, err)
		}
	})

	t.Run("stream_must_not_exist_succeeds_for_new_stream", func(t *testing.T) {
		t.Parallel()

		es := newStore(t)
		streamID := typeid.NewV4("test")

		if err := es.AppendStream(t.Context(), streamID, writableEvents[0:1], eventstore.AppendStreamOptions{StreamMustNotExist: true}); err != nil {
			t.Errorf("AppendStream with StreamMustNotExist on new stream returned unexpected error: %v", err)
		}
	})
}

func containsSubstring(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		func() bool {
			for i := 0; i <= len(s)-len(substr); i++ {
				if s[i:i+len(substr)] == substr {
					return true
				}
			}
			return false
		}())
}

func TestEventStore_Integration_AppendStream(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("skipping integration test")
	}

	for _, tStrat := range []struct {
		name   string
		desc   string
		create func(*testing.T) sqliteeventstore.Strategy
	}{
		{
			name: testStrategyDefault,
			desc: "default options",
			create: func(*testing.T) sqliteeventstore.Strategy {
				t.Helper()
				return must(strategy.NewDefaultStrategy())
			},
		},
		{
			name: testStrategyDefault,
			desc: "custom table names",
			create: func(t *testing.T) sqliteeventstore.Strategy {
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
				haveOpts:     eventstore.AppendStreamOptions{ExpectVersion: eventstore.VersionPtr(2)},
				wantEvents:   eventsFor(streamIDs[0]),
			},
			{
				name: "append events (expected version mismatch)",
				withEvents: map[typeid.ID][]*eventstore.WritableEvent{
					streamIDs[0]: writableEvents[0:2],
				},
				haveStreamID: streamIDs[0],
				haveEvents:   writableEvents[2:],
				haveOpts:     eventstore.AppendStreamOptions{ExpectVersion: eventstore.VersionPtr(1)},
				wantErr:      eventstore.StreamVersionMismatchError{StreamID: streamIDs[0], ExpectedVersion: 1, ActualVersion: 2},
			},
		} {
			t.Run(tStrat.name+"_"+tStrat.desc+"_"+tt.name, func(t *testing.T) {
				t.Parallel()

				db := newSQLiteDB(t)

				s := tStrat.create(t)
				if _, err := db.ExecContext(t.Context(), s.Schema()); err != nil {
					t.Fatalf("failed to create DB schema: %v", err)
				}

				eventStore, err := sqliteeventstore.New(db, sqliteeventstore.WithStrategy(s))
				if err != nil {
					t.Fatalf("failed to create SQLite event store: %v", err)
				}

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
						t.Fatalf("unexpected error appending stream: %v", err)
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
					if eq, err := jsonEq(gotEvents[i].Data, tt.wantEvents[i].Data); err != nil {
						t.Errorf("event %d: error comparing JSON data: %v", i, err)
					} else if !eq {
						t.Errorf("event %d: expected Data %q, got %q", i, string(tt.wantEvents[i].Data), string(gotEvents[i].Data))
					}
				}
			})
		}
	}
}

// TestEventStore_Integration_ListStreamsAndReadAll exercises both StreamLister and AllReader,
// which the default strategy implements.
func TestEventStore_Integration_ListStreamsAndReadAll(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("skipping integration test")
	}

	db := newSQLiteDB(t)
	strat := must(strategy.NewDefaultStrategy())
	if _, err := db.ExecContext(t.Context(), strat.Schema()); err != nil {
		t.Fatalf("failed to create DB schema: %v", err)
	}

	es, err := sqliteeventstore.New(db, sqliteeventstore.WithStrategy(strat))
	if err != nil {
		t.Fatalf("failed to create SQLite event store: %v", err)
	}

	streamA := typeid.NewV4("alpha")
	streamB := typeid.NewV4("beta")

	if err := es.AppendStream(t.Context(), streamA, writableEvents[0:2], eventstore.AppendStreamOptions{}); err != nil {
		t.Fatalf("AppendStream A failed: %v", err)
	}
	if err := es.AppendStream(t.Context(), streamB, writableEvents[0:3], eventstore.AppendStreamOptions{}); err != nil {
		t.Fatalf("AppendStream B failed: %v", err)
	}

	// ListStreams should return metadata for both streams.
	streams, err := es.ListStreams(t.Context())
	if err != nil {
		t.Fatalf("ListStreams failed: %v", err)
	}
	if len(streams) != 2 {
		t.Fatalf("expected 2 streams, got %d", len(streams))
	}

	got := map[typeid.ID]int64{}
	for _, s := range streams {
		got[s.StreamID] = s.LastOffset
	}
	if got[streamA] != 2 {
		t.Errorf("expected last_offset=2 for streamA, got %d", got[streamA])
	}
	if got[streamB] != 3 {
		t.Errorf("expected last_offset=3 for streamB, got %d", got[streamB])
	}

	// ReadAll should return all 5 events ordered by global position.
	iter, err := es.ReadAll(t.Context(), eventstore.ReadStreamOptions{})
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	var allEvents []*eventstore.Event
	for {
		ev, err := iter.Next(t.Context())
		if errors.Is(err, eventstore.ErrEndOfEventStream) {
			break
		}
		if err != nil {
			t.Fatalf("iterator error: %v", err)
		}
		allEvents = append(allEvents, ev)
	}
	if len(allEvents) != 5 {
		t.Fatalf("expected 5 events from ReadAll, got %d", len(allEvents))
	}

	// Global positions should be strictly increasing.
	for i := 1; i < len(allEvents); i++ {
		prev := *allEvents[i-1].GlobalPosition
		curr := *allEvents[i].GlobalPosition
		if curr <= prev {
			t.Errorf("global positions not increasing at index %d: prev=%d curr=%d", i, prev, curr)
		}
	}

	// ReadAll with AfterVersion (interpreted as global position) skips earlier events.
	checkpoint := *allEvents[1].GlobalPosition
	iter2, err := es.ReadAll(t.Context(), eventstore.ReadStreamOptions{AfterVersion: checkpoint})
	if err != nil {
		t.Fatalf("ReadAll(AfterVersion) failed: %v", err)
	}
	var afterEvents []*eventstore.Event
	for {
		ev, err := iter2.Next(t.Context())
		if errors.Is(err, eventstore.ErrEndOfEventStream) {
			break
		}
		if err != nil {
			t.Fatalf("iterator error: %v", err)
		}
		afterEvents = append(afterEvents, ev)
	}
	if len(afterEvents) != 3 {
		t.Errorf("expected 3 events after checkpoint, got %d", len(afterEvents))
	}
}

// TestEventStore_Integration_TransactionHook exercises the WithAppendTransactionHooks option
// using SQLite's normal transaction semantics: a hook that errors must roll back the append.
func TestEventStore_Integration_TransactionHook(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Run("hook_runs_with_appended_events", func(t *testing.T) {
		t.Parallel()

		db := newSQLiteDB(t)
		strat := must(strategy.NewDefaultStrategy())
		if _, err := db.ExecContext(t.Context(), strat.Schema()); err != nil {
			t.Fatalf("failed to create DB schema: %v", err)
		}

		var hookEvents []*eventstore.Event
		es, err := sqliteeventstore.New(db,
			sqliteeventstore.WithStrategy(strat),
			sqliteeventstore.WithAppendTransactionHooks(sqliteeventstore.TransactionHookFunc(
				func(_ context.Context, _ *sql.Tx, evs []*eventstore.Event) error {
					hookEvents = append(hookEvents, evs...)
					return nil
				},
			)),
		)
		if err != nil {
			t.Fatalf("failed to create SQLite event store: %v", err)
		}

		streamID := typeid.NewV4("test")
		if err := es.AppendStream(t.Context(), streamID, writableEvents[0:2], eventstore.AppendStreamOptions{}); err != nil {
			t.Fatalf("AppendStream failed: %v", err)
		}

		if len(hookEvents) != 2 {
			t.Errorf("expected hook to receive 2 events, got %d", len(hookEvents))
		}
		for i, ev := range hookEvents {
			if ev.StreamVersion != int64(i+1) {
				t.Errorf("hook event %d: expected StreamVersion %d, got %d", i, i+1, ev.StreamVersion)
			}
		}
	})

	t.Run("hook_error_aborts_append", func(t *testing.T) {
		t.Parallel()

		db := newSQLiteDB(t)
		strat := must(strategy.NewDefaultStrategy())
		if _, err := db.ExecContext(t.Context(), strat.Schema()); err != nil {
			t.Fatalf("failed to create DB schema: %v", err)
		}

		hookErr := errors.New("hook failure")
		es, err := sqliteeventstore.New(db,
			sqliteeventstore.WithStrategy(strat),
			sqliteeventstore.WithAppendTransactionHooks(sqliteeventstore.TransactionHookFunc(
				func(_ context.Context, _ *sql.Tx, _ []*eventstore.Event) error {
					return hookErr
				},
			)),
		)
		if err != nil {
			t.Fatalf("failed to create SQLite event store: %v", err)
		}

		streamID := typeid.NewV4("test")
		err = es.AppendStream(t.Context(), streamID, writableEvents[0:2], eventstore.AppendStreamOptions{})
		if !errors.Is(err, hookErr) {
			t.Fatalf("expected AppendStream to return hook error, got: %v", err)
		}

		// The append should have been rolled back: the stream must not exist.
		_, err = es.ReadStream(t.Context(), streamID, eventstore.ReadStreamOptions{})
		if !errors.Is(err, eventstore.ErrStreamNotFound) {
			t.Errorf("expected ErrStreamNotFound after rolled-back append, got: %v", err)
		}
	})
}
