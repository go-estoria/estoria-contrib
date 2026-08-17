package eventstore_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-estoria/estoria-contrib/mongodb/eventstore"
	"github.com/go-estoria/estoria-contrib/mongodb/eventstore/strategy"
	coreeventstore "github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
	"github.com/gofrs/uuid/v5"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// orderingGuard bounds joins on writers that should finish promptly; it is a deadlock
// guard only, never evidence of blocking.
const orderingGuard = 60 * time.Second

// frontierStrategyCases enumerates both storage strategies; every subtest builds its
// stores over a fresh database so global offsets start at 1.
func frontierStrategyCases(mongoClient *mongo.Client) []struct {
	name        string
	newStrategy func(t *testing.T, db *mongo.Database) eventstore.Strategy
} {
	return []struct {
		name        string
		newStrategy func(t *testing.T, db *mongo.Database) eventstore.Strategy
	}{
		{
			name: "single collection strategy",
			newStrategy: func(t *testing.T, db *mongo.Database) eventstore.Strategy {
				t.Helper()
				strat, err := strategy.NewSingleCollectionStrategy(mongoClient, db)
				if err != nil {
					t.Fatalf("tc setup: failed to create SingleCollectionStrategy: %v", err)
				}
				return strat
			},
		},
		{
			name: "multi collection strategy",
			newStrategy: func(t *testing.T, db *mongo.Database) eventstore.Strategy {
				t.Helper()
				strat, err := strategy.NewMultiCollectionStrategy(mongoClient, db, strategy.CollectionPerStreamID())
				if err != nil {
					t.Fatalf("tc setup: failed to create MultiCollectionStrategy: %v", err)
				}
				return strat
			},
		},
	}
}

func frontierWritableEvents(n int) []*coreeventstore.WritableEvent {
	events := make([]*coreeventstore.WritableEvent, n)
	for i := range events {
		events[i] = &coreeventstore.WritableEvent{
			Type: "frontierevent",
			Data: fmt.Appendf(nil, `{"index":%d}`, i),
		}
	}
	return events
}

// collectAll drains a fresh global read and returns its events.
func collectAll(t *testing.T, store *eventstore.EventStore, opts coreeventstore.ReadAllOptions) []*coreeventstore.Event {
	t.Helper()

	iter, err := store.ReadAll(t.Context(), opts)
	if err != nil {
		t.Fatalf("reading all events: %v", err)
	}
	defer func() { _ = iter.Close(t.Context()) }()

	events, err := coreeventstore.Collect(t.Context(), iter)
	if err != nil {
		t.Fatalf("collecting events: %v", err)
	}

	return events
}

// TestEventStore_Integration_ReadAllFrontier pins the bounded shape of a multi-batch
// global read racing appends: the drain ends at the frontier captured when ReadAll
// returned, and the next poll receives exactly what the frontier excluded. Over
// store-written data the pinned server happens to keep open cursors stable, so this
// test documents the contract across both strategies rather than proving the bound
// load-bearing — that proof lives in the non-transactional-data regression below,
// where the live scan demonstrably chases the race.
func TestEventStore_Integration_ReadAllFrontier(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Parallel()

	ctx := t.Context()

	mongoClient, err := createMongoDBContainer(t)
	if err != nil {
		t.Fatalf("failed to create MongoDB container: %v", err)
	}

	dbCount := 0
	newDatabase := func(t *testing.T) *mongo.Database {
		t.Helper()
		dbCount++
		database := mongoClient.Database(fmt.Sprintf("estoria_fr_%d", dbCount))
		t.Cleanup(func() {
			if err := database.Drop(context.WithoutCancel(ctx)); err != nil {
				t.Fatalf("tc cleanup: failed to drop database: %v", err)
			}
		})
		return database
	}

	for _, tt := range frontierStrategyCases(mongoClient) {
		t.Run(tt.name, func(t *testing.T) {
			store, err := eventstore.New(mongoClient, eventstore.WithStrategy(tt.newStrategy(t, newDatabase(t))))
			if err != nil {
				t.Fatalf("tc setup: failed to create EventStore: %v", err)
			}

			// 300 events span several cursor batches.
			const initial = 300
			streamID := typeid.NewV4("frontiertest")
			if _, err := store.AppendStream(ctx, streamID, frontierWritableEvents(initial), coreeventstore.AppendStreamOptions{}); err != nil {
				t.Fatalf("appending initial events: %v", err)
			}

			iter, err := store.ReadAll(ctx, coreeventstore.ReadAllOptions{})
			if err != nil {
				t.Fatalf("reading all events: %v", err)
			}
			defer func() { _ = iter.Close(ctx) }()

			// Drain a little first, so the racing append below lands mid-iteration,
			// behind the batches the open cursor has yet to fetch.
			for i := range 10 {
				event, err := iter.Next(ctx)
				if err != nil {
					t.Fatalf("draining event %d: %v", i, err)
				}
				if got := *event.GlobalPosition; got != int64(i)+1 {
					t.Fatalf("want position %d while draining, got %d", i+1, got)
				}
			}

			if _, err := store.AppendStream(ctx, streamID, frontierWritableEvents(5), coreeventstore.AppendStreamOptions{}); err != nil {
				t.Fatalf("appending racing events: %v", err)
			}

			events, err := coreeventstore.Collect(ctx, iter)
			if err != nil {
				t.Fatalf("collecting events: %v", err)
			}

			if got := 10 + len(events); got != initial {
				t.Fatalf("want the read bounded at its frontier of %d events, got %d", initial, got)
			}

			if got := *events[len(events)-1].GlobalPosition; got != initial {
				t.Errorf("want the read's last position at the frontier %d, got %d", initial, got)
			}

			// The next poll receives exactly what this read excluded.
			next := collectAll(t, store, coreeventstore.ReadAllOptions{AfterPosition: initial})
			if len(next) != 5 || *next[0].GlobalPosition != initial+1 {
				t.Fatalf("want the next poll to receive the 5 racing events from %d, got %d events", initial+1, len(next))
			}
		})
	}
}

// TestEventStore_Integration_ReadAllFrontierNonTransactionalData proves the frontier
// bound is load-bearing where store-shaped data cannot: the pinned server happens to
// keep an open cursor stable over documents written by this store's own transactions,
// but MongoDB guarantees no cursor isolation, and over documents written outside
// transactions — a bulk import, a legacy backfill — the same open cursor provably
// returns documents committed during iteration. Only the frontier excludes them.
func TestEventStore_Integration_ReadAllFrontierNonTransactionalData(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Parallel()

	ctx := t.Context()

	mongoClient, err := createMongoDBContainer(t)
	if err != nil {
		t.Fatalf("failed to create MongoDB container: %v", err)
	}

	db := mongoClient.Database("estoria_frl")
	t.Cleanup(func() {
		if err := db.Drop(context.WithoutCancel(ctx)); err != nil {
			t.Fatalf("tc cleanup: failed to drop database: %v", err)
		}
	})

	rawEvents := db.Collection("events")
	rawStreams := db.Collection(strategy.DefaultStreamsCollectionName)

	strat, err := strategy.NewSingleCollectionStrategy(mongoClient, db)
	if err != nil {
		t.Fatalf("tc setup: failed to create SingleCollectionStrategy: %v", err)
	}
	store, err := eventstore.New(mongoClient, eventstore.WithStrategy(strat))
	if err != nil {
		t.Fatalf("tc setup: failed to create EventStore: %v", err)
	}

	streamID := typeid.NewV4("legacytest")
	rawDoc := func(offset int64) any {
		return eventstore.EventDocument{
			StreamType:   streamID.Type,
			StreamID:     streamID.UUID.String(),
			EventType:    "legacyevent",
			EventID:      uuid.Must(uuid.NewV4()).String(),
			Offset:       offset,
			GlobalOffset: offset,
			Timestamp:    time.Now().UTC().Truncate(time.Millisecond),
			EventData:    []byte(`{}`),
		}
	}

	// A legacy dataset: 300 documents and their counters written directly, with no
	// transactions, the way an import or an earlier writer would.
	const initial = 300
	docs := make([]any, initial)
	for i := range docs {
		docs[i] = rawDoc(int64(i) + 1)
	}
	if _, err := rawEvents.InsertMany(ctx, docs); err != nil {
		t.Fatalf("seeding legacy events: %v", err)
	}
	if _, err := rawStreams.InsertOne(ctx, bson.M{
		"_id":         streamID.String(),
		"stream_type": streamID.Type,
		"stream_id":   streamID.UUID.String(),
		"last_offset": int64(initial),
	}); err != nil {
		t.Fatalf("seeding stream document: %v", err)
	}
	if _, err := rawStreams.InsertOne(ctx, bson.M{"_id": "_global", "last_offset": int64(initial)}); err != nil {
		t.Fatalf("seeding global counter: %v", err)
	}

	// The live scan needs the global offset index, which only appends auto-ensure.
	if err := store.EnsureIndexes(ctx); err != nil {
		t.Fatalf("ensuring indexes: %v", err)
	}

	iter, err := store.ReadAll(ctx, coreeventstore.ReadAllOptions{})
	if err != nil {
		t.Fatalf("reading all events: %v", err)
	}
	defer func() { _ = iter.Close(ctx) }()

	for i := range 10 {
		if _, err := iter.Next(ctx); err != nil {
			t.Fatalf("draining event %d: %v", i, err)
		}
	}

	// The legacy writer races the drain: a direct insert and counter bump land ahead
	// of the scan while the cursor is open.
	if _, err := rawEvents.InsertMany(ctx, []any{rawDoc(initial + 1)}); err != nil {
		t.Fatalf("racing legacy insert: %v", err)
	}
	if _, err := rawStreams.UpdateOne(ctx,
		bson.M{"_id": "_global"},
		bson.M{"$inc": bson.M{"last_offset": int64(1)}},
	); err != nil {
		t.Fatalf("racing counter bump: %v", err)
	}

	events, err := coreeventstore.Collect(ctx, iter)
	if err != nil {
		t.Fatalf("collecting events: %v", err)
	}

	if got := 10 + len(events); got != initial {
		t.Fatalf("want the read bounded at its frontier of %d events, got %d", initial, got)
	}

	// The next poll receives what this read excluded.
	next := collectAll(t, store, coreeventstore.ReadAllOptions{AfterPosition: initial})
	if len(next) != 1 || *next[0].GlobalPosition != initial+1 {
		t.Fatalf("want the next poll to receive the raced event at %d, got %d events", initial+1, len(next))
	}
}

// TestEventStore_Integration_ReadAllFrontierNonTransactionalDataMultiCollection proves
// the frontier bound is load-bearing for the multi-collection read specifically: every
// per-collection cursor must be built from the one frontier the strategy captured, and
// the single-collection regression above never exercises that pass-through. Over
// documents written outside transactions, an unbounded cursor provably chases a racing
// insert into its collection, so a multi-collection read that dropped or widened its
// frontier fails here even though store-shaped data would let it pass.
func TestEventStore_Integration_ReadAllFrontierNonTransactionalDataMultiCollection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Parallel()

	ctx := t.Context()

	mongoClient, err := createMongoDBContainer(t)
	if err != nil {
		t.Fatalf("failed to create MongoDB container: %v", err)
	}

	db := mongoClient.Database("estoria_frlm")
	t.Cleanup(func() {
		if err := db.Drop(context.WithoutCancel(ctx)); err != nil {
			t.Fatalf("tc cleanup: failed to drop database: %v", err)
		}
	})

	// Two stream types, so the legacy events land in two collections and the global
	// read is a genuine multi-cursor merge.
	streamA, streamB := typeid.NewV4("legacya"), typeid.NewV4("legacyb")
	rawDoc := func(id typeid.ID, offset, global int64) any {
		return eventstore.EventDocument{
			StreamType:   id.Type,
			StreamID:     id.UUID.String(),
			EventType:    "legacyevent",
			EventID:      uuid.Must(uuid.NewV4()).String(),
			Offset:       offset,
			GlobalOffset: global,
			Timestamp:    time.Now().UTC().Truncate(time.Millisecond),
			EventData:    []byte(`{}`),
		}
	}

	// A legacy dataset: 300 documents with alternating global offsets, written directly
	// with no transactions, the way an import or an earlier writer would.
	const initial = 300
	docsA := make([]any, 0, initial/2)
	docsB := make([]any, 0, initial/2)
	for global := int64(1); global <= initial; global++ {
		if global%2 == 1 {
			docsA = append(docsA, rawDoc(streamA, (global+1)/2, global))
		} else {
			docsB = append(docsB, rawDoc(streamB, global/2, global))
		}
	}

	collectionA := db.Collection(streamA.Type)
	rawStreams := db.Collection(strategy.DefaultStreamsCollectionName)
	if _, err := collectionA.InsertMany(ctx, docsA); err != nil {
		t.Fatalf("seeding legacy events for stream A: %v", err)
	}
	if _, err := db.Collection(streamB.Type).InsertMany(ctx, docsB); err != nil {
		t.Fatalf("seeding legacy events for stream B: %v", err)
	}
	for _, id := range []typeid.ID{streamA, streamB} {
		if _, err := rawStreams.InsertOne(ctx, bson.M{
			"_id":         id.String(),
			"stream_type": id.Type,
			"stream_id":   id.UUID.String(),
			"last_offset": int64(initial / 2),
		}); err != nil {
			t.Fatalf("seeding stream document: %v", err)
		}
	}
	if _, err := rawStreams.InsertOne(ctx, bson.M{"_id": "_global", "last_offset": int64(initial)}); err != nil {
		t.Fatalf("seeding global counter: %v", err)
	}

	strat, err := strategy.NewMultiCollectionStrategy(mongoClient, db, strategy.CollectionPerStreamType())
	if err != nil {
		t.Fatalf("tc setup: failed to create MultiCollectionStrategy: %v", err)
	}
	store, err := eventstore.New(mongoClient, eventstore.WithStrategy(strat))
	if err != nil {
		t.Fatalf("tc setup: failed to create EventStore: %v", err)
	}

	// The live scans need the global offset indexes, which only appends auto-ensure.
	if err := store.EnsureIndexes(ctx); err != nil {
		t.Fatalf("ensuring indexes: %v", err)
	}

	iter, err := store.ReadAll(ctx, coreeventstore.ReadAllOptions{})
	if err != nil {
		t.Fatalf("reading all events: %v", err)
	}
	defer func() { _ = iter.Close(ctx) }()

	for i := range 10 {
		event, err := iter.Next(ctx)
		if err != nil {
			t.Fatalf("draining event %d: %v", i, err)
		}
		if got := *event.GlobalPosition; got != int64(i)+1 {
			t.Fatalf("want merged position %d while draining, got %d", i+1, got)
		}
	}

	// The legacy writer races the drain, landing in a collection whose cursor is
	// already open.
	if _, err := collectionA.InsertMany(ctx, []any{rawDoc(streamA, initial/2+1, initial+1)}); err != nil {
		t.Fatalf("racing legacy insert: %v", err)
	}
	if _, err := rawStreams.UpdateOne(ctx,
		bson.M{"_id": "_global"},
		bson.M{"$inc": bson.M{"last_offset": int64(1)}},
	); err != nil {
		t.Fatalf("racing counter bump: %v", err)
	}

	events, err := coreeventstore.Collect(ctx, iter)
	if err != nil {
		t.Fatalf("collecting events: %v", err)
	}

	if got := 10 + len(events); got != initial {
		t.Fatalf("want the read bounded at its frontier of %d events, got %d", initial, got)
	}

	if got := *events[len(events)-1].GlobalPosition; got != initial {
		t.Errorf("want the read's last position at the frontier %d, got %d", initial, got)
	}

	// The next poll receives what this read excluded.
	next := collectAll(t, store, coreeventstore.ReadAllOptions{AfterPosition: initial})
	if len(next) != 1 || *next[0].GlobalPosition != initial+1 {
		t.Fatalf("want the next poll to receive the raced event at %d, got %d events", initial+1, len(next))
	}
}

// A pausingHook holds writer A's append transaction open at the hook point — after its
// offsets are reserved and its events inserted — or aborts it, so the tests can hold
// and resolve an unresolved earlier reservation deterministically.
type pausingHook struct {
	entered  chan struct{}
	once     sync.Once
	proceed  chan struct{}
	abortErr error
}

func (h *pausingHook) HandleEvents(ctx context.Context, _ []*coreeventstore.Event) error {
	h.once.Do(func() { close(h.entered) })

	if h.abortErr != nil {
		return h.abortErr
	}

	select {
	case <-h.proceed:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// TestEventStore_Integration_GlobalOffsetsPublishInOrder pins the serialization that
// makes the committed counter a stable frontier: an append reserves its offsets and
// inserts in one transaction against the shared counter document, so no append can
// allocate past an increment it cannot see, and visible offsets only ever extend
// upward — a committed event can never appear below one already observed. The
// mechanics differ by counter state, and each arm pins its own. On an existing counter,
// concurrent updates conflict at write time: the later append retries and publishes
// nothing while the earlier reservation is unresolved. On a fresh counter, both
// transactions upsert-insert the document and MongoDB resolves the race at commit
// time: the first committer wins the low range and the loser retries onto higher
// offsets — reordered relative to reservation, never overlapping, never retroactive.
// The aborted arm proves a rolled-back reservation is reissued rather than left as a
// gap.
func TestEventStore_Integration_GlobalOffsetsPublishInOrder(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Parallel()

	ctx := t.Context()

	mongoClient, err := createMongoDBContainer(t)
	if err != nil {
		t.Fatalf("failed to create MongoDB container: %v", err)
	}

	dbCount := 0
	newDatabase := func(t *testing.T) *mongo.Database {
		t.Helper()
		dbCount++
		database := mongoClient.Database(fmt.Sprintf("estoria_po_%d", dbCount))
		t.Cleanup(func() {
			if err := database.Drop(context.WithoutCancel(ctx)); err != nil {
				t.Fatalf("tc cleanup: failed to drop database: %v", err)
			}
		})
		return database
	}

	for _, tt := range frontierStrategyCases(mongoClient) {
		t.Run(tt.name, func(t *testing.T) {
			t.Run("a later append publishes nothing while a reservation on an existing counter is unresolved", func(t *testing.T) {
				strat := tt.newStrategy(t, newDatabase(t))

				hook := &pausingHook{entered: make(chan struct{}), proceed: make(chan struct{})}
				storeA, err := eventstore.New(mongoClient, eventstore.WithStrategy(strat), eventstore.WithTransactionHook(hook))
				if err != nil {
					t.Fatalf("tc setup: failed to create paused store: %v", err)
				}
				storeB, err := eventstore.New(mongoClient, eventstore.WithStrategy(strat))
				if err != nil {
					t.Fatalf("tc setup: failed to create store: %v", err)
				}

				// The counter must exist committed, so the writers' reservations are
				// conflicting updates — the write-time serialization under test —
				// rather than a fresh upsert race.
				if _, err := storeB.AppendStream(ctx, typeid.NewV4("potest"), frontierWritableEvents(1), coreeventstore.AppendStreamOptions{}); err != nil {
					t.Fatalf("seeding the counter: %v", err)
				}

				// A paused writer left behind by a test failure would hold its
				// transaction open and stall the database drop in cleanup.
				var releaseOnce sync.Once
				release := func() { releaseOnce.Do(func() { close(hook.proceed) }) }
				t.Cleanup(release)

				// Different streams, so the only shared document is the global counter.
				streamA, streamB := typeid.NewV4("potest"), typeid.NewV4("potest")

				var (
					writerAEvents []*coreeventstore.Event
					writerAErr    error
				)
				writerADone := make(chan struct{})
				go func() {
					defer close(writerADone)
					writerAEvents, writerAErr = storeA.AppendStream(ctx, streamA, frontierWritableEvents(2), coreeventstore.AppendStreamOptions{})
				}()

				select {
				case <-hook.entered:
				case <-writerADone:
					t.Fatalf("writer A finished before pausing: %v", writerAErr)
				case <-time.After(orderingGuard):
					t.Fatal("deadlock guard: writer A never reached its hook")
				}

				var (
					writerBEvents []*coreeventstore.Event
					writerBErr    error
				)
				writerBDone := make(chan struct{})
				go func() {
					defer close(writerBDone)
					writerBEvents, writerBErr = storeB.AppendStream(ctx, streamB, frontierWritableEvents(1), coreeventstore.AppendStreamOptions{})
				}()

				// Violation net, not a blocking proof: an implementation that allocates
				// or publishes outside the reservation's transaction completes writer B
				// or publishes past the pending reservation within milliseconds, and
				// the final order assertions below cannot catch that on their own —
				// once A commits, the end state looks identical either way.
				net := time.After(1500 * time.Millisecond)
				for watching := true; watching; {
					select {
					case <-writerBDone:
						t.Fatalf("writer B completed while writer A's earlier reservation was unresolved: err=%v", writerBErr)
					case <-writerADone:
						t.Fatalf("writer A finished while paused in its hook: %v", writerAErr)
					case <-net:
						watching = false
					case <-time.After(25 * time.Millisecond):
						if events := collectAll(t, storeB, coreeventstore.ReadAllOptions{}); len(events) != 1 {
							t.Fatalf("want only the seed published while the reservation is unresolved, got %d events", len(events))
						}
					}
				}

				release()

				select {
				case <-writerADone:
				case <-time.After(orderingGuard):
					t.Fatal("deadlock guard: writer A never finished after release")
				}
				select {
				case <-writerBDone:
				case <-time.After(orderingGuard):
					t.Fatal("deadlock guard: writer B never finished after release")
				}

				if writerAErr != nil || writerBErr != nil {
					t.Fatalf("want both writers to succeed, got A: %v, B: %v", writerAErr, writerBErr)
				}

				if a1, a2 := *writerAEvents[0].GlobalPosition, *writerAEvents[1].GlobalPosition; a1 != 2 || a2 != 3 {
					t.Errorf("want writer A's earlier reservation at positions (2, 3), got (%d, %d)", a1, a2)
				}
				if b := *writerBEvents[0].GlobalPosition; b != 4 {
					t.Errorf("want writer B allocated after writer A at position 4, got %d", b)
				}

				events := collectAll(t, storeB, coreeventstore.ReadAllOptions{})
				if len(events) != 4 {
					t.Fatalf("want all 4 events published after both commits, got %d", len(events))
				}
			})

			t.Run("a fresh counter's upsert race is first-committer-wins without overlap", func(t *testing.T) {
				strat := tt.newStrategy(t, newDatabase(t))

				hook := &pausingHook{entered: make(chan struct{}), proceed: make(chan struct{})}
				storeA, err := eventstore.New(mongoClient, eventstore.WithStrategy(strat), eventstore.WithTransactionHook(hook))
				if err != nil {
					t.Fatalf("tc setup: failed to create paused store: %v", err)
				}
				storeB, err := eventstore.New(mongoClient, eventstore.WithStrategy(strat))
				if err != nil {
					t.Fatalf("tc setup: failed to create store: %v", err)
				}

				var releaseOnce sync.Once
				release := func() { releaseOnce.Do(func() { close(hook.proceed) }) }
				t.Cleanup(release)

				streamA, streamB := typeid.NewV4("potest"), typeid.NewV4("potest")

				var (
					writerAEvents []*coreeventstore.Event
					writerAErr    error
				)
				writerADone := make(chan struct{})
				go func() {
					defer close(writerADone)
					writerAEvents, writerAErr = storeA.AppendStream(ctx, streamA, frontierWritableEvents(2), coreeventstore.AppendStreamOptions{})
				}()

				select {
				case <-hook.entered:
				case <-writerADone:
					t.Fatalf("writer A finished before pausing: %v", writerAErr)
				case <-time.After(orderingGuard):
					t.Fatal("deadlock guard: writer A never reached its hook")
				}

				// Both transactions upsert-insert the counter document, and MongoDB
				// resolves insert races at commit time: writer B — the only one able
				// to commit while A is paused — wins the low range outright.
				var (
					writerBEvents []*coreeventstore.Event
					writerBErr    error
				)
				writerBDone := make(chan struct{})
				go func() {
					defer close(writerBDone)
					writerBEvents, writerBErr = storeB.AppendStream(ctx, streamB, frontierWritableEvents(1), coreeventstore.AppendStreamOptions{})
				}()

				select {
				case <-writerBDone:
				case <-time.After(orderingGuard):
					t.Fatal("deadlock guard: writer B never finished against the paused insert race")
				}

				if writerBErr != nil {
					t.Fatalf("want writer B to win the fresh-counter race, got %v", writerBErr)
				}

				// The stable prefix's observable form: whatever is visible now may only
				// ever be extended upward — the loser must land above it, never fill in
				// below a position already observed.
				observed := collectAll(t, storeB, coreeventstore.ReadAllOptions{})
				if len(observed) != 1 || *observed[0].GlobalPosition != 1 {
					t.Fatalf("want exactly the race winner at position 1 visible while A pends, got %d events", len(observed))
				}

				release()

				select {
				case <-writerADone:
				case <-time.After(orderingGuard):
					t.Fatal("deadlock guard: writer A never finished after release")
				}

				if writerAErr != nil {
					t.Fatalf("want the losing writer to retry onto higher offsets, got %v", writerAErr)
				}

				if b := *writerBEvents[0].GlobalPosition; b != 1 {
					t.Errorf("want the first committer at position 1, got %d", b)
				}
				if a1, a2 := *writerAEvents[0].GlobalPosition, *writerAEvents[1].GlobalPosition; a1 != 2 || a2 != 3 {
					t.Errorf("want the loser reallocated above the observed prefix at (2, 3), got (%d, %d)", a1, a2)
				}

				events := collectAll(t, storeB, coreeventstore.ReadAllOptions{})
				if len(events) != 3 {
					t.Fatalf("want all 3 events published after both commits, got %d", len(events))
				}
			})

			t.Run("an aborted reservation is reissued to the next append", func(t *testing.T) {
				strat := tt.newStrategy(t, newDatabase(t))

				abort := errors.New("hook aborts the append")
				hook := &pausingHook{entered: make(chan struct{}), abortErr: abort}
				storeA, err := eventstore.New(mongoClient, eventstore.WithStrategy(strat), eventstore.WithTransactionHook(hook))
				if err != nil {
					t.Fatalf("tc setup: failed to create aborting store: %v", err)
				}
				storeB, err := eventstore.New(mongoClient, eventstore.WithStrategy(strat))
				if err != nil {
					t.Fatalf("tc setup: failed to create store: %v", err)
				}

				if _, err := storeA.AppendStream(ctx, typeid.NewV4("potest"), frontierWritableEvents(1), coreeventstore.AppendStreamOptions{}); !errors.Is(err, abort) {
					t.Fatalf("want the hook abort surfaced from the append, got %v", err)
				}

				written, err := storeB.AppendStream(ctx, typeid.NewV4("potest"), frontierWritableEvents(1), coreeventstore.AppendStreamOptions{})
				if err != nil {
					t.Fatalf("appending after the aborted reservation: %v", err)
				}

				if got := *written[0].GlobalPosition; got != 1 {
					t.Errorf("want the aborted reservation reissued at position 1, got %d", got)
				}

				events := collectAll(t, storeB, coreeventstore.ReadAllOptions{})
				if len(events) != 1 {
					t.Fatalf("want exactly the surviving append published, got %d events", len(events))
				}
			})
		})
	}
}
