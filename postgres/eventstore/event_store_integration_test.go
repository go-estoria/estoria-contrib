package eventstore_test

import (
	"database/sql"
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

func TestEventStore_Integration_ReadStream(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

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
	} {
		for _, tt := range []struct {
			name         string
			withDBState  func(*testing.T, *sql.DB)
			haveStreamID typeid.UUID
			haveOpts     eventstore.ReadStreamOptions
			wantEvents   []*eventstore.Event
			wantErr      error
		}{
			{
				name:         "read from empty stream",
				haveStreamID: must(typeid.NewUUID("nonexistentstream")),
				wantEvents:   []*eventstore.Event{},
			},
			{
				name: "read from stream with one event",
				withDBState: func(t *testing.T, db *sql.DB) {
					t.Helper()
					insertEvents(t, db, "default", []*eventstore.Event{
						{
							ID:            must(typeid.ParseUUID("event_7841ba8b-1e6e-4a96-a3cf-fce9c1c262b7")),
							StreamID:      must(typeid.ParseUUID("stream_d0972d91-9b86-4adc-9158-3eb894f5e0a3")),
							StreamVersion: 1,
							Timestamp:     now,
							Data:          []byte(`{"foo":"abc","bar":123,"baz":true}`),
						},
					})
				},
				haveStreamID: must(typeid.ParseUUID("stream_d0972d91-9b86-4adc-9158-3eb894f5e0a3")),
				wantEvents: []*eventstore.Event{
					{
						ID:            must(typeid.ParseUUID("event_7841ba8b-1e6e-4a96-a3cf-fce9c1c262b7")),
						StreamID:      must(typeid.ParseUUID("stream_d0972d91-9b86-4adc-9158-3eb894f5e0a3")),
						StreamVersion: 1,
						Timestamp:     now,
						Data:          []byte(`{"foo":"abc","bar":123,"baz":true}`),
					},
				},
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

				// setup test-specific DB state
				if tt.withDBState != nil {
					tt.withDBState(t, db)
				}

				eventStore, err := pgeventstore.New(db, pgeventstore.WithStrategy(s))
				if err != nil {
					t.Fatalf("failed to create Postgres event store: %v", err)
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

func insertEvents(t *testing.T, db *sql.DB, strat string, events []*eventstore.Event) {
	t.Helper()
	switch strat {
	default:
		for i, e := range events {
			if _, err := db.ExecContext(t.Context(), `
			INSERT INTO event (event_id, event_type, stream_id, stream_type, stream_offset, timestamp, data)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
		`, e.ID.Value(), e.ID.TypeName(), e.StreamID.Value(), e.StreamID.TypeName(), e.StreamVersion, e.Timestamp, e.Data); err != nil {
				t.Fatalf("failed to insert test event %d of %d: %v", i+1, len(events), err)
			}
		}
	}
}

func must[T any](val T, err error) T {
	if err != nil {
		panic("unexpected error: " + err.Error())
	}
	return val
}
