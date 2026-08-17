package eventstore_test

import (
	"context"
	"sync"
	"testing"

	"github.com/go-estoria/estoria-contrib/mongodb/eventstore"
	"github.com/go-estoria/estoria-contrib/mongodb/eventstore/strategy"
	coreeventstore "github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// A commandRecorder captures the commands a monitored client puts on the wire, so a
// test can assert what a read actually carried rather than what the code intended.
type commandRecorder struct {
	mu      sync.Mutex
	started []recordedCommand
}

type recordedCommand struct {
	name     string
	database string
	command  bson.Raw
}

func (r *commandRecorder) monitor() *event.CommandMonitor {
	return &event.CommandMonitor{
		Started: func(_ context.Context, evt *event.CommandStartedEvent) {
			// The event's buffer is the driver's; copy before retaining.
			command := make(bson.Raw, len(evt.Command))
			copy(command, evt.Command)

			r.mu.Lock()
			defer r.mu.Unlock()
			r.started = append(r.started, recordedCommand{
				name:     evt.CommandName,
				database: evt.DatabaseName,
				command:  command,
			})
		},
	}
}

func (r *commandRecorder) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.started = nil
}

// finds returns the find commands recorded against the named collection.
func (r *commandRecorder) finds(database, collection string) []bson.Raw {
	r.mu.Lock()
	defer r.mu.Unlock()

	commands := []bson.Raw{}
	for _, rec := range r.started {
		if rec.name != "find" || rec.database != database {
			continue
		}
		if target, err := rec.command.LookupErr("find"); err == nil && target.StringValue() == collection {
			commands = append(commands, rec.command)
		}
	}

	return commands
}

// requireMajorityConcern asserts a recorded command carried majority read concern. The
// clients in this file are configured with no read concern at all, so an unpinned read
// sends none and the lookup fails: the assertion discriminates the strategies' derived
// read view from the client's own settings.
func requireMajorityConcern(t *testing.T, command bson.Raw, what string) {
	t.Helper()

	level, err := command.LookupErr("readConcern", "level")
	if err != nil {
		t.Fatalf("want the %s to carry a read concern, got none (%v)", what, err)
	}
	if got := level.StringValue(); got != "majority" {
		t.Fatalf("want the %s to read with majority concern, got %q", what, got)
	}
}

// TestEventStore_Integration_GlobalReadsCarryMajorityConcernAndFrontier pins, on the
// wire, the two properties every command of a global read must carry: majority read
// concern, so no yielded position can be rolled back by a failover, and the one shared
// frontier bound, so every cursor of the read — one per collection under the
// multi-collection strategy — is capped at the same majority-committed offset. The
// per-collection bound is what store-shaped behavioral tests cannot observe over
// transactionally-written data, where the pinned server keeps even unbounded cursors
// coincidentally stable. Read preference is not asserted here: on a direct connection
// to a single-node replica set it is unobservable, so the primary pin is enforced at
// derive time in the strategies' read view and covered by the topology contract.
func TestEventStore_Integration_GlobalReadsCarryMajorityConcernAndFrontier(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Parallel()

	ctx := t.Context()

	mongoClient, connStr, err := createMongoDBContainerWithConnStr(t)
	if err != nil {
		t.Fatalf("failed to create MongoDB container: %v", err)
	}

	recorder := &commandRecorder{}
	monitored, err := mongo.Connect(options.Client().
		ApplyURI(connStr).
		SetReplicaSet("rs0").
		SetDirect(true).
		SetMonitor(recorder.monitor()),
	)
	if err != nil {
		t.Fatalf("tc setup: failed to create monitored client: %v", err)
	}
	t.Cleanup(func() {
		if err := monitored.Disconnect(context.WithoutCancel(ctx)); err != nil {
			t.Logf("tc cleanup: failed to disconnect monitored client: %v", err)
		}
	})

	for _, tt := range []struct {
		name   string
		dbName string
		// newStrategy builds the strategy over the monitored client, whose commands the
		// recorder captures.
		newStrategy func(t *testing.T, db *mongo.Database) eventstore.Strategy
		// eventCollections are the collections the two appended streams land in, each
		// of which must receive exactly one bounded find.
		eventCollections []string
	}{
		{
			name:   "single collection strategy",
			dbName: "estoria_rv_single",
			newStrategy: func(t *testing.T, db *mongo.Database) eventstore.Strategy {
				t.Helper()
				strat, err := strategy.NewSingleCollectionStrategy(monitored, db)
				if err != nil {
					t.Fatalf("tc setup: failed to create SingleCollectionStrategy: %v", err)
				}
				return strat
			},
			eventCollections: []string{strategy.DefaultEventsCollectionName},
		},
		{
			name:   "multi collection strategy",
			dbName: "estoria_rv_multi",
			newStrategy: func(t *testing.T, db *mongo.Database) eventstore.Strategy {
				t.Helper()
				strat, err := strategy.NewMultiCollectionStrategy(monitored, db, strategy.CollectionPerStreamType())
				if err != nil {
					t.Fatalf("tc setup: failed to create MultiCollectionStrategy: %v", err)
				}
				return strat
			},
			eventCollections: []string{"rvtypea", "rvtypeb"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			db := monitored.Database(tt.dbName)
			t.Cleanup(func() {
				if err := mongoClient.Database(tt.dbName).Drop(context.WithoutCancel(ctx)); err != nil {
					t.Fatalf("tc cleanup: failed to drop database: %v", err)
				}
			})

			store, err := eventstore.New(monitored, eventstore.WithStrategy(tt.newStrategy(t, db)))
			if err != nil {
				t.Fatalf("tc setup: failed to create EventStore: %v", err)
			}

			// Two streams whose types match the expected event collections, so the
			// multi-collection case opens one cursor per collection. Five events fix
			// the frontier the read's finds must carry.
			const frontier = 5
			if _, err := store.AppendStream(ctx, typeid.NewV4("rvtypea"), frontierWritableEvents(3), coreeventstore.AppendStreamOptions{}); err != nil {
				t.Fatalf("appending events: %v", err)
			}
			if _, err := store.AppendStream(ctx, typeid.NewV4("rvtypeb"), frontierWritableEvents(2), coreeventstore.AppendStreamOptions{}); err != nil {
				t.Fatalf("appending events: %v", err)
			}

			recorder.reset()

			events := collectAll(t, store, coreeventstore.ReadAllOptions{})
			if len(events) != frontier {
				t.Fatalf("want %d events from the global read, got %d", frontier, len(events))
			}

			// The frontier read on the streams collection must itself be
			// majority-committed; a local read could return an offset that a failover
			// rolls back, along with the events below it.
			counterFinds := recorder.finds(tt.dbName, strategy.DefaultStreamsCollectionName)
			if len(counterFinds) != 1 {
				t.Fatalf("want exactly 1 frontier read on the streams collection, got %d", len(counterFinds))
			}
			requireMajorityConcern(t, counterFinds[0], "frontier read")

			for _, collection := range tt.eventCollections {
				finds := recorder.finds(tt.dbName, collection)
				if len(finds) != 1 {
					t.Fatalf("want exactly 1 find on collection %q, got %d", collection, len(finds))
				}

				requireMajorityConcern(t, finds[0], "event find on "+collection)

				bound, err := finds[0].LookupErr("filter", "global_offset", "$lte")
				if err != nil {
					t.Fatalf("want the find on %q to carry a frontier bound, got none (%v)", collection, err)
				}
				if got, ok := bound.AsInt64OK(); !ok || got != frontier {
					t.Fatalf("want the find on %q bounded at the shared frontier %d, got %v", collection, frontier, bound)
				}
			}
		})
	}
}
