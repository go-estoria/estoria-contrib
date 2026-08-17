package eventstore_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	sqliteeventstore "github.com/go-estoria/estoria-contrib/sqlite/eventstore"
	"github.com/go-estoria/estoria-contrib/sqlite/eventstore/strategy"
	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
)

// frontierGuard bounds joins on work that should finish promptly; it is a deadlock
// guard only, never evidence of blocking.
const frontierGuard = 60 * time.Second

func frontierWritableEvents(n int) []*eventstore.WritableEvent {
	events := make([]*eventstore.WritableEvent, n)
	for i := range events {
		events[i] = &eventstore.WritableEvent{
			Type: "frontierevent",
			Data: fmt.Appendf(nil, `{"index":%d}`, i),
		}
	}
	return events
}

// collectAll drains a fresh global read and returns its events.
func collectAll(t *testing.T, store *sqliteeventstore.EventStore, opts eventstore.ReadAllOptions) []*eventstore.Event {
	t.Helper()

	iter, err := store.ReadAll(t.Context(), opts)
	if err != nil {
		t.Fatalf("reading all events: %v", err)
	}
	defer func() { _ = iter.Close(t.Context()) }()

	events, err := eventstore.Collect(t.Context(), iter)
	if err != nil {
		t.Fatalf("collecting events: %v", err)
	}

	return events
}

// newFrontierStore builds an event store with its schema applied over the given database.
func newFrontierStore(t *testing.T, db *sql.DB, opts ...sqliteeventstore.EventStoreOption) *sqliteeventstore.EventStore {
	t.Helper()

	strat, err := strategy.NewDefaultStrategy()
	if err != nil {
		t.Fatalf("tc setup: creating strategy: %v", err)
	}
	if _, err := db.ExecContext(t.Context(), strat.Schema()); err != nil {
		t.Fatalf("tc setup: applying schema: %v", err)
	}

	store, err := sqliteeventstore.New(db, append([]sqliteeventstore.EventStoreOption{sqliteeventstore.WithStrategy(strat)}, opts...)...)
	if err != nil {
		t.Fatalf("tc setup: creating event store: %v", err)
	}

	return store
}

// A pausingHook holds writer A's append transaction open at the hook point — after its
// ids are assigned, before commit — or aborts it, so the tests can hold and resolve an
// in-flight allocation deterministically.
type pausingHook struct {
	entered  chan struct{}
	once     sync.Once
	proceed  chan struct{}
	abortErr error
}

func (h *pausingHook) HandleEvents(ctx context.Context, _ *sql.Tx, _ []*eventstore.Event) error {
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

// TestEventStore_Integration_AppendSerializesOnWriteLock pins the mechanism that makes
// the auto-incrementing id a stable global prefix: SQLite permits one write transaction
// at a time, and an append assigns its ids while holding that write lock through
// commit. The paused arm proves it behaviorally — a later append publishes nothing
// while an earlier transaction holds assigned-but-uncommitted ids — which is exactly
// the interleaving that breaks a separable backend (ids visible out of commit order)
// and is unrepresentable here. The aborted arm pins the rollback shape.
func TestEventStore_Integration_AppendSerializesOnWriteLock(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Parallel()

	ctx := t.Context()

	t.Run("a later append publishes nothing while earlier ids are unresolved", func(t *testing.T) {
		db := newSQLiteDB(t)
		hook := &pausingHook{entered: make(chan struct{}), proceed: make(chan struct{})}
		storeA := newFrontierStore(t, db, sqliteeventstore.WithAppendTransactionHooks(hook))
		storeB, err := sqliteeventstore.New(db)
		if err != nil {
			t.Fatalf("tc setup: creating plain store: %v", err)
		}

		// A committed seed, so the reads below always have a stable prefix to see.
		if _, err := storeB.AppendStream(ctx, typeid.NewV4("seedstream"), frontierWritableEvents(1), eventstore.AppendStreamOptions{}); err != nil {
			t.Fatalf("seeding: %v", err)
		}

		// A paused writer left behind by a test failure would hold the write lock and
		// stall every later append until the busy timeout.
		var releaseOnce sync.Once
		release := func() { releaseOnce.Do(func() { close(hook.proceed) }) }
		t.Cleanup(release)

		streamA, streamB := typeid.NewV4("locktest"), typeid.NewV4("locktest")

		var (
			writerAEvents []*eventstore.Event
			writerAErr    error
		)
		writerADone := make(chan struct{})
		go func() {
			defer close(writerADone)
			writerAEvents, writerAErr = storeA.AppendStream(ctx, streamA, frontierWritableEvents(2), eventstore.AppendStreamOptions{})
		}()

		select {
		case <-hook.entered:
		case <-writerADone:
			t.Fatalf("writer A finished before pausing: %v", writerAErr)
		case <-time.After(frontierGuard):
			t.Fatal("deadlock guard: writer A never reached its hook")
		}

		var (
			writerBEvents []*eventstore.Event
			writerBErr    error
		)
		writerBDone := make(chan struct{})
		go func() {
			defer close(writerBDone)
			writerBEvents, writerBErr = storeB.AppendStream(ctx, streamB, frontierWritableEvents(1), eventstore.AppendStreamOptions{})
		}()

		// Violation net, not a blocking proof: an implementation that could assign or
		// publish ids outside the write lock would complete writer B or expose its rows
		// within milliseconds, and the final order assertions below cannot catch that
		// on their own — once A commits, the end state looks identical either way. The
		// net must stay shorter than the connection's busy timeout, or writer B fails
		// instead of waiting.
		net := time.After(1500 * time.Millisecond)
		for watching := true; watching; {
			select {
			case <-writerBDone:
				t.Fatalf("writer B committed while writer A's earlier ids were unresolved: err=%v", writerBErr)
			case <-writerADone:
				t.Fatalf("writer A finished while paused in its hook: %v", writerAErr)
			case <-net:
				watching = false
			case <-time.After(25 * time.Millisecond):
				if events := collectAll(t, storeB, eventstore.ReadAllOptions{}); len(events) != 1 {
					t.Fatalf("want only the seed visible while ids are unresolved, got %d events", len(events))
				}
			}
		}

		release()

		select {
		case <-writerADone:
		case <-time.After(frontierGuard):
			t.Fatal("deadlock guard: writer A never finished after release")
		}
		select {
		case <-writerBDone:
		case <-time.After(frontierGuard):
			t.Fatal("deadlock guard: writer B never finished after release")
		}

		if writerAErr != nil || writerBErr != nil {
			t.Fatalf("want both writers to succeed, got A: %v, B: %v", writerAErr, writerBErr)
		}

		if a1, a2 := *writerAEvents[0].GlobalPosition, *writerAEvents[1].GlobalPosition; a1 != 2 || a2 != 3 {
			t.Errorf("want writer A's earlier transaction at positions (2, 3), got (%d, %d)", a1, a2)
		}
		if b := *writerBEvents[0].GlobalPosition; b != 4 {
			t.Errorf("want writer B serialized after writer A at position 4, got %d", b)
		}

		events := collectAll(t, storeB, eventstore.ReadAllOptions{})
		if len(events) != 4 {
			t.Fatalf("want all 4 events published after both commits, got %d", len(events))
		}
		for i, event := range events {
			if got := *event.GlobalPosition; got != int64(i)+1 {
				t.Fatalf("want position %d at index %d, got %d", i+1, i, got)
			}
		}
	})

	t.Run("an aborted append's rollback releases its ids and its reservation", func(t *testing.T) {
		db := newSQLiteDB(t)
		abort := errors.New("hook aborts the append")
		hook := &pausingHook{entered: make(chan struct{}), abortErr: abort}
		storeA := newFrontierStore(t, db, sqliteeventstore.WithAppendTransactionHooks(hook))
		storeB, err := sqliteeventstore.New(db)
		if err != nil {
			t.Fatalf("tc setup: creating plain store: %v", err)
		}

		streamID := typeid.NewV4("aborttest")
		if _, err := storeB.AppendStream(ctx, streamID, frontierWritableEvents(1), eventstore.AppendStreamOptions{}); err != nil {
			t.Fatalf("seeding: %v", err)
		}

		// The aborted append targets the same stream, so a reservation that escaped the
		// rollback would surface as an offset gap in the follow-up append.
		if _, err := storeA.AppendStream(ctx, streamID, frontierWritableEvents(2), eventstore.AppendStreamOptions{}); !errors.Is(err, abort) {
			t.Fatalf("want the hook abort surfaced from the append, got %v", err)
		}

		written, err := storeB.AppendStream(ctx, streamID, frontierWritableEvents(1), eventstore.AppendStreamOptions{})
		if err != nil {
			t.Fatalf("appending after the aborted transaction: %v", err)
		}

		if got := written[0].StreamVersion; got != 2 {
			t.Errorf("want the aborted reservation rolled back so the next append lands at version 2, got %d", got)
		}
		if got := *written[0].GlobalPosition; got != 2 {
			t.Errorf("want the aborted ids returned so the next append lands at position 2, got %d", got)
		}

		events := collectAll(t, storeB, eventstore.ReadAllOptions{})
		if len(events) != 2 {
			t.Fatalf("want exactly the two committed events published, got %d", len(events))
		}
	})
}

// TestEventStore_Integration_ReadAllSnapshotStableUnderRacingAppends pins the reader
// half of the mechanism: an open global read holds its query's database snapshot for
// the iterator's lifetime, so in WAL mode a commit racing the drain lands invisibly
// beyond the read's frontier rather than extending it, and the next poll receives
// exactly what the frontier excluded.
func TestEventStore_Integration_ReadAllSnapshotStableUnderRacingAppends(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Parallel()

	ctx := t.Context()

	db := newSQLiteDB(t)
	store := newFrontierStore(t, db)

	const initial = 300
	streamID := typeid.NewV4("frontiertest")
	if _, err := store.AppendStream(ctx, streamID, frontierWritableEvents(initial), eventstore.AppendStreamOptions{}); err != nil {
		t.Fatalf("appending initial events: %v", err)
	}

	iter, err := store.ReadAll(ctx, eventstore.ReadAllOptions{})
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
			t.Fatalf("want position %d while draining, got %d", i+1, got)
		}
	}

	// WAL mode lets this commit proceed while the read is open; the snapshot keeps it
	// out of the open iterator.
	if _, err := store.AppendStream(ctx, streamID, frontierWritableEvents(5), eventstore.AppendStreamOptions{}); err != nil {
		t.Fatalf("appending racing events: %v", err)
	}

	events, err := eventstore.Collect(ctx, iter)
	if err != nil {
		t.Fatalf("collecting events: %v", err)
	}

	if got := 10 + len(events); got != initial {
		t.Fatalf("want the read bounded at its snapshot of %d events, got %d", initial, got)
	}
	if got := *events[len(events)-1].GlobalPosition; got != initial {
		t.Errorf("want the read's last position at its snapshot %d, got %d", initial, got)
	}

	next := collectAll(t, store, eventstore.ReadAllOptions{AfterPosition: initial})
	if len(next) != 5 || *next[0].GlobalPosition != initial+1 {
		t.Fatalf("want the next poll to receive the 5 racing events from %d, got %d events", initial+1, len(next))
	}
}

// TestEventStore_Integration_RollbackJournalReadExcludesRacingCommit pins the other
// journal mode's semantics: without WAL, an open read's shared lock makes a racing
// append wait at commit instead of proceeding invisibly, so the read is stable there
// too — by exclusion rather than snapshotting — and the append lands once the
// iterator closes. Deployments choose the availability trade-off; correctness holds
// in both modes.
func TestEventStore_Integration_RollbackJournalReadExcludesRacingCommit(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Parallel()

	ctx := t.Context()

	db := newSQLiteDBWithJournalMode(t, "DELETE")
	store := newFrontierStore(t, db)

	const initial = 20
	streamID := typeid.NewV4("journaltest")
	if _, err := store.AppendStream(ctx, streamID, frontierWritableEvents(initial), eventstore.AppendStreamOptions{}); err != nil {
		t.Fatalf("appending initial events: %v", err)
	}

	iter, err := store.ReadAll(ctx, eventstore.ReadAllOptions{})
	if err != nil {
		t.Fatalf("reading all events: %v", err)
	}
	defer func() { _ = iter.Close(ctx) }()

	for i := range 2 {
		if _, err := iter.Next(ctx); err != nil {
			t.Fatalf("draining event %d: %v", i, err)
		}
	}

	appendDone := make(chan error, 1)
	go func() {
		_, err := store.AppendStream(ctx, streamID, frontierWritableEvents(1), eventstore.AppendStreamOptions{})
		appendDone <- err
	}()

	// Violation net: the append's commit must wait on the open read's shared lock. It
	// must stay shorter than the connection's busy timeout, or the append fails
	// instead of waiting.
	select {
	case err := <-appendDone:
		t.Fatalf("want the racing append blocked at commit while the read is open, but it finished: %v", err)
	case <-time.After(1 * time.Second):
	}

	events, err := eventstore.Collect(ctx, iter)
	if err != nil {
		t.Fatalf("collecting events: %v", err)
	}
	if got := 2 + len(events); got != initial {
		t.Fatalf("want the read to see exactly its %d events, got %d", initial, got)
	}

	if err := iter.Close(ctx); err != nil {
		t.Fatalf("closing the iterator: %v", err)
	}

	select {
	case err := <-appendDone:
		if err != nil {
			t.Fatalf("want the append to land once the read closed, got %v", err)
		}
	case <-time.After(frontierGuard):
		t.Fatal("deadlock guard: the append never finished after the read closed")
	}

	next := collectAll(t, store, eventstore.ReadAllOptions{AfterPosition: initial})
	if len(next) != 1 || *next[0].GlobalPosition != initial+1 {
		t.Fatalf("want the raced event at %d after the read closed, got %d events", initial+1, len(next))
	}
}

// TestEventStore_Integration_PositionsNeverReusedAfterDeletion pins AUTOINCREMENT as
// load-bearing: deleting the newest events must not let their positions be reissued,
// or a consumer resuming from a checkpoint at the old tip would silently skip every
// event written into the reused range. A plain INTEGER PRIMARY KEY reuses the rowids;
// only AUTOINCREMENT's sequence makes positions permanent.
func TestEventStore_Integration_PositionsNeverReusedAfterDeletion(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Parallel()

	ctx := t.Context()

	db := newSQLiteDB(t)
	store := newFrontierStore(t, db)

	streamID := typeid.NewV4("reusetest")
	if _, err := store.AppendStream(ctx, streamID, frontierWritableEvents(3), eventstore.AppendStreamOptions{}); err != nil {
		t.Fatalf("appending initial events: %v", err)
	}

	// A checkpointed consumer has seen through position 3.
	if events := collectAll(t, store, eventstore.ReadAllOptions{}); len(events) != 3 {
		t.Fatalf("want 3 events before deletion, got %d", len(events))
	}

	if err := store.DeleteStream(ctx, streamID, eventstore.DeleteStreamOptions{}); err != nil {
		t.Fatalf("deleting the stream: %v", err)
	}

	written, err := store.AppendStream(ctx, typeid.NewV4("reusetest"), frontierWritableEvents(2), eventstore.AppendStreamOptions{})
	if err != nil {
		t.Fatalf("appending after deletion: %v", err)
	}

	if got := *written[0].GlobalPosition; got != 4 {
		t.Fatalf("want positions to continue at 4 after the tail was deleted, got %d", got)
	}

	// The consumer's resume sees exactly the new events; nothing was written beneath
	// its checkpoint.
	resumed := collectAll(t, store, eventstore.ReadAllOptions{AfterPosition: 3})
	if len(resumed) != 2 || *resumed[0].GlobalPosition != 4 || *resumed[1].GlobalPosition != 5 {
		t.Fatalf("want the resume to receive positions (4, 5), got %d events", len(resumed))
	}
}
