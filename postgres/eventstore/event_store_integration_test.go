package eventstore_test

import (
	"database/sql"
	"testing"

	pgeventstore "github.com/go-estoria/estoria-contrib/postgres/eventstore"
	"github.com/go-estoria/estoria-contrib/postgres/eventstore/strategy"
	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
)

func TestEventStore_Integration_ReadStream(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("skipping integration test")
	}

	for _, tt := range []struct {
		name         string
		withDB       func(*testing.T, *sql.DB)
		haveStreamID typeid.UUID
		haveOpts     eventstore.ReadStreamOptions
		wantEvents   []*eventstore.Event
		wantErr      error
	}{
		{},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db, err := createPostgresContainer(t, t.Context())
			if err != nil {
				t.Fatalf("failed to create Postgres container: %v", err)
			}

			if tt.withDB != nil {
				tt.withDB(t, db)
			}

			strat, err := strategy.NewDefaultStrategy()
			if err != nil {
				t.Fatalf("failed to create default strategy: %v", err)
			}

			if _, err := db.ExecContext(t.Context(), strat.Schema()); err != nil {
				t.Fatalf("failed to create DB schema: %v", err)
			}

			eventStore, err := pgeventstore.New(db, pgeventstore.WithStrategy(strat))
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
