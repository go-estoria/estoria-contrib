package eventstore_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	pgeventstore "github.com/go-estoria/estoria-contrib/postgres/eventstore"
	"github.com/go-estoria/estoria-contrib/postgres/eventstore/strategy"
	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// deadlockGuard bounds waits that can only hang if the implementation
// deadlocks. It is never evidence of blocking: the observer's two-arm
// disjunction decides the test, not time.
const deadlockGuard = 60 * time.Second

// A pausingHook holds an append transaction open after its events are
// inserted — global positions reserved — until released, so a test can keep
// an allocation unresolved while a second writer runs. A non-nil abortErr
// makes the append roll back on release instead of committing.
type pausingHook struct {
	paused   chan struct{}
	release  chan struct{}
	abortErr error
}

func (h *pausingHook) HandleEvents(ctx context.Context, _ pgx.Tx, _ []*eventstore.Event) error {
	close(h.paused)

	select {
	case <-h.release:
	case <-ctx.Done():
		return ctx.Err()
	}

	return h.abortErr
}

// poolNamed returns a pool whose connections carry the given
// application_name, so pg_stat_activity can identify the writer.
func poolNamed(t *testing.T, connString, name string) *pgxpool.Pool {
	t.Helper()

	cfg, err := pgxpool.ParseConfig(connString)
	if err != nil {
		t.Fatalf("parsing connection string: %v", err)
	}

	cfg.ConnConfig.RuntimeParams["application_name"] = name

	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatalf("creating pool %q: %v", name, err)
	}

	t.Cleanup(pool.Close)

	return pool
}

// TestAppendStream_StablePrefix_Integration proves the allocator's
// publication ordering with no timing assumptions. While writer A holds an
// unresolved position reservation, an observer on a third connection polls a
// deterministic disjunction: either writer B's rows are committed — the
// broken behavior of the earlier bigserial schema, an immediate failure — or
// B is observed blocked by A specifically on the allocator update, the fixed
// behavior, after which A resolves and both writers' outcomes are asserted.
// The commit arm proves B publishes strictly after A; the rollback arm
// proves B proceeds with A's returned range, leaving no gap.
func TestAppendStream_StablePrefix_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Parallel()

	db, connString, err := createPostgresContainerWithConnString(t)
	if err != nil {
		t.Fatalf("failed to create Postgres container: %v", err)
	}

	errAbort := errors.New("aborting reservation")

	for i, tt := range []struct {
		name  string
		abort bool
	}{
		{name: "writer B publishes strictly after writer A commits"},
		{name: "writer B proceeds with the returned range after writer A aborts", abort: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			eventsTable := fmt.Sprintf("event_sp%d", i)
			allocatorTable := eventsTable + "_position_allocator"

			strat := must(strategy.NewDefaultStrategy(
				strategy.WithEventsTableName(eventsTable),
				strategy.WithStreamsTableName(fmt.Sprintf("stream_sp%d", i)),
			))

			if _, err := db.Exec(t.Context(), strat.Schema()); err != nil {
				t.Fatalf("creating tables: %v", err)
			}

			writerAName := fmt.Sprintf("sp%d_writer_a", i)
			writerBName := fmt.Sprintf("sp%d_writer_b", i)

			hook := &pausingHook{paused: make(chan struct{}), release: make(chan struct{})}
			if tt.abort {
				hook.abortErr = errAbort
			}

			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(hook.release) }) }

			storeA := must(pgeventstore.New(poolNamed(t, connString, writerAName),
				pgeventstore.WithStrategy(strat),
				pgeventstore.WithAppendTransactionHooks(hook),
			))
			storeB := must(pgeventstore.New(poolNamed(t, connString, writerBName),
				pgeventstore.WithStrategy(strat),
			))

			streamA := typeid.NewV4("spstream")
			streamB := typeid.NewV4("spstream")

			newEvents := func(n int) []*eventstore.WritableEvent {
				events := make([]*eventstore.WritableEvent, n)
				for j := range events {
					events[j] = &eventstore.WritableEvent{Type: "spevent", Data: []byte(`{}`)}
				}

				return events
			}

			// Writer A reserves two positions and pauses in its hook.
			var (
				writerAEvents []*eventstore.Event
				writerAErr    error
			)

			writerADone := make(chan struct{})
			go func() {
				defer close(writerADone)
				writerAEvents, writerAErr = storeA.AppendStream(t.Context(), streamA, newEvents(2), eventstore.AppendStreamOptions{})
			}()

			t.Cleanup(func() {
				release()
				select {
				case <-writerADone:
				case <-time.After(deadlockGuard):
					t.Errorf("deadlock guard: writer A never finished")
				}
			})

			select {
			case <-hook.paused:
			case <-writerADone:
				t.Fatalf("writer A finished before pausing: %v", writerAErr)
			case <-time.After(deadlockGuard):
				t.Fatal("deadlock guard: writer A never reached its transaction hook")
			}

			// Writer B appends to another stream while A is unresolved.
			var (
				writerBEvents []*eventstore.Event
				writerBErr    error
			)

			writerBDone := make(chan struct{})
			go func() {
				defer close(writerBDone)
				writerBEvents, writerBErr = storeB.AppendStream(t.Context(), streamB, newEvents(1), eventstore.AppendStreamOptions{})
			}()

			t.Cleanup(func() {
				release()
				select {
				case <-writerBDone:
				case <-time.After(deadlockGuard):
					t.Errorf("deadlock guard: writer B never finished")
				}
			})

			// The two-arm observation, through the third connection.
			guard := time.Now().Add(deadlockGuard)
			for blocked := false; !blocked; {
				if time.Now().After(guard) {
					t.Fatal("deadlock guard: writer B neither blocked on the allocator nor committed")
				}

				// Broken arm: B's rows are visible while A is unresolved.
				var committed int
				if err := db.QueryRow(t.Context(), fmt.Sprintf(
					`SELECT count(*) FROM %s WHERE stream_id = $1`, pgx.Identifier{eventsTable}.Sanitize(),
				), streamB.UUID).Scan(&committed); err != nil {
					t.Fatalf("observing committed rows: %v", err)
				}

				if committed > 0 {
					t.Fatal("stable-prefix violation: writer B committed while writer A's earlier reservation was unresolved")
				}

				// Fixed arm: B blocked by A, specifically on the allocator update.
				var query string
				err := db.QueryRow(t.Context(), `
					SELECT blocked.query
					FROM pg_stat_activity blocked
					WHERE blocked.application_name = $1
					  AND EXISTS (
						SELECT 1
						FROM pg_stat_activity blocker
						WHERE blocker.application_name = $2
						  AND blocker.pid = ANY (pg_blocking_pids(blocked.pid))
					  )`, writerBName, writerAName).Scan(&query)

				switch {
				case errors.Is(err, pgx.ErrNoRows):
					time.Sleep(5 * time.Millisecond)
				case err != nil:
					t.Fatalf("observing blocked writer: %v", err)
				case !strings.Contains(query, allocatorTable):
					t.Fatalf("writer B is blocked by writer A on an unexpected statement: %s", query)
				default:
					blocked = true
				}
			}

			release()

			select {
			case <-writerADone:
			case <-time.After(deadlockGuard):
				t.Fatal("deadlock guard: writer A never finished after release")
			}

			select {
			case <-writerBDone:
			case <-time.After(deadlockGuard):
				t.Fatal("deadlock guard: writer B never finished after release")
			}

			if writerBErr != nil {
				t.Fatalf("writer B failed: %v", writerBErr)
			}

			all := readAllEvents(t, storeB)

			if tt.abort {
				if !errors.Is(writerAErr, errAbort) {
					t.Fatalf("want writer A to surface the hook's abort error, got %v", writerAErr)
				}

				// B proceeded with the range A returned: position 1, no gap.
				if got := *writerBEvents[0].GlobalPosition; got != 1 {
					t.Errorf("want writer B to reuse the returned reservation at position 1, got %d", got)
				}

				if len(all) != 1 || all[0].ID != writerBEvents[0].ID {
					t.Fatalf("want exactly writer B's event in the global read, got %d events", len(all))
				}

				return
			}

			if writerAErr != nil {
				t.Fatalf("writer A failed: %v", writerAErr)
			}

			// B's position is strictly above A's, and the global read yields
			// A's events then B's.
			if aMax, bPos := *writerAEvents[1].GlobalPosition, *writerBEvents[0].GlobalPosition; bPos <= aMax {
				t.Errorf("want writer B's position above writer A's %d, got %d", aMax, bPos)
			}

			wantIDs := []typeid.ID{writerAEvents[0].ID, writerAEvents[1].ID, writerBEvents[0].ID}
			if len(all) != len(wantIDs) {
				t.Fatalf("want %d events in the global read, got %d", len(wantIDs), len(all))
			}

			for j, event := range all {
				if event.ID != wantIDs[j] {
					t.Errorf("event %d: want %s, got %s", j, wantIDs[j], event.ID)
				}
			}

			// Resuming after the last yielded position sees nothing: nothing
			// can commit into the skipped range.
			if resumed := readAllEventsAfter(t, storeB, *writerBEvents[0].GlobalPosition); len(resumed) != 0 {
				t.Errorf("want an empty resume after the tail, got %d events", len(resumed))
			}
		})
	}
}

func readAllEvents(t *testing.T, store *pgeventstore.EventStore) []*eventstore.Event {
	t.Helper()

	return readAllEventsAfter(t, store, 0)
}

func readAllEventsAfter(t *testing.T, store *pgeventstore.EventStore, position int64) []*eventstore.Event {
	t.Helper()

	iter, err := store.ReadAll(t.Context(), eventstore.ReadAllOptions{AfterPosition: position})
	if err != nil {
		t.Fatalf("reading all events: %v", err)
	}

	defer func() {
		if err := iter.Close(t.Context()); err != nil {
			t.Errorf("closing iterator: %v", err)
		}
	}()

	events, err := eventstore.Collect(t.Context(), iter)
	if err != nil {
		t.Fatalf("collecting events: %v", err)
	}

	return events
}
