package eventstore_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	pgeventstore "github.com/go-estoria/estoria-contrib/postgres/eventstore"
	"github.com/go-estoria/estoria-contrib/postgres/eventstore/strategy"
	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
	"github.com/gofrs/uuid/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestAppendStream_HookPanicReleasesAllocator pins the unconditional
// rollback: a panicking transaction hook unwinds through AppendStream while
// the transaction holds the global position allocator's lock, and without a
// rollback on that path the orphaned transaction would block every future
// append. The panic must propagate, and another writer must proceed
// immediately, reusing the released reservation.
func TestAppendStream_HookPanicReleasesAllocator(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Parallel()

	db, connString, err := createPostgresContainerWithConnString(t)
	if err != nil {
		t.Fatalf("failed to create Postgres container: %v", err)
	}

	strat := must(strategy.NewDefaultStrategy(
		strategy.WithEventsTableName("event_hp"),
		strategy.WithStreamsTableName("stream_hp"),
	))

	if _, err := db.Exec(t.Context(), strat.Schema()); err != nil {
		t.Fatalf("creating tables: %v", err)
	}

	panicking := pgeventstore.TransactionHookFunc(func(context.Context, pgx.Tx, []*eventstore.Event) error {
		panic("hook exploded")
	})

	// The panicking store gets its own pool, deliberately left unclosed:
	// under this regression's failure mode — no rollback on the panic path —
	// the leaked transaction's connection would deadlock pool.Close during
	// cleanup and hang the binary past every guard. Process exit reclaims it.
	panickingPool, err := pgxpool.New(t.Context(), connString)
	if err != nil {
		t.Fatalf("creating pool for the panicking store: %v", err)
	}

	storeA := must(pgeventstore.New(panickingPool, pgeventstore.WithStrategy(strat), pgeventstore.WithAppendTransactionHooks(panicking)))
	storeB := must(pgeventstore.New(db, pgeventstore.WithStrategy(strat)))

	newEvents := func(n int) []*eventstore.WritableEvent {
		events := make([]*eventstore.WritableEvent, n)
		for i := range events {
			events[i] = &eventstore.WritableEvent{Type: "hpevent", Data: []byte(`{}`)}
		}

		return events
	}

	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("want the hook panic to propagate out of AppendStream")
			}
		}()

		_, _ = storeA.AppendStream(t.Context(), typeid.NewV4("hpstream"), newEvents(1), eventstore.AppendStreamOptions{})
	}()

	// Deadlock guard only: with the rollback in place, B proceeds at once.
	ctx, cancel := context.WithTimeout(t.Context(), deadlockGuard)
	defer cancel()

	written, err := storeB.AppendStream(ctx, typeid.NewV4("hpstream"), newEvents(1), eventstore.AppendStreamOptions{})
	if err != nil {
		t.Fatalf("want a writer to proceed after the panicked append rolled back, got %v", err)
	}

	if got := *written[0].GlobalPosition; got != 1 {
		t.Errorf("want the panicked append's returned reservation at position 1, got %d", got)
	}
}

// A streamTouchingHook pauses inside writer A's transaction, then — once
// released — takes the streams-table row lock for another writer's stream
// through A's transaction, the way an outbox-style hook can touch rows a
// concurrent writer also locks.
type streamTouchingHook struct {
	entered      chan struct{}
	proceed      chan struct{}
	streamsTable string
	stream       typeid.ID
}

func (h *streamTouchingHook) HandleEvents(ctx context.Context, tx pgx.Tx, _ []*eventstore.Event) error {
	close(h.entered)

	select {
	case <-h.proceed:
	case <-ctx.Done():
		return ctx.Err()
	}

	_, err := tx.Exec(ctx, fmt.Sprintf(
		`UPDATE %s SET last_offset = last_offset WHERE stream_type = $1 AND stream_id = $2`,
		pgx.Identifier{h.streamsTable}.Sanitize(),
	), h.stream.Type, h.stream.UUID)

	return err
}

// TestAppendStream_AllocatorBeforeStreamLocks pins the lock order that keeps
// hooks deadlock-free: the allocator is acquired before any stream-specific
// lock, so a writer waiting on the allocator holds nothing a hook inside the
// allocator's holder could be waiting for. Under the reversed order, writer B
// holds its stream row while waiting on A's allocator, A's hook then waits on
// B's stream row, and PostgreSQL's deadlock detector kills one of them.
func TestAppendStream_AllocatorBeforeStreamLocks(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Parallel()

	db, connString, err := createPostgresContainerWithConnString(t)
	if err != nil {
		t.Fatalf("failed to create Postgres container: %v", err)
	}

	const (
		eventsTable  = "event_lo"
		streamsTable = "stream_lo"
	)

	allocatorTable := eventsTable + "_position_allocator"

	strat := must(strategy.NewDefaultStrategy(
		strategy.WithEventsTableName(eventsTable),
		strategy.WithStreamsTableName(streamsTable),
	))

	if _, err := db.Exec(t.Context(), strat.Schema()); err != nil {
		t.Fatalf("creating tables: %v", err)
	}

	newEvents := func(n int) []*eventstore.WritableEvent {
		events := make([]*eventstore.WritableEvent, n)
		for i := range events {
			events[i] = &eventstore.WritableEvent{Type: "loevent", Data: []byte(`{}`)}
		}

		return events
	}

	streamA := typeid.NewV4("lostream")
	streamB := typeid.NewV4("lostream")

	// Writer B's stream must pre-exist committed, so the hook's row touch
	// contends with B's own lock on it rather than seeing nothing.
	seed := must(pgeventstore.New(db, pgeventstore.WithStrategy(strat)))
	if _, err := seed.AppendStream(t.Context(), streamB, newEvents(1), eventstore.AppendStreamOptions{}); err != nil {
		t.Fatalf("seeding stream B: %v", err)
	}

	hook := &streamTouchingHook{
		entered:      make(chan struct{}),
		proceed:      make(chan struct{}),
		streamsTable: streamsTable,
		stream:       streamB,
	}

	storeA := must(pgeventstore.New(poolNamed(t, connString, "lo_writer_a"),
		pgeventstore.WithStrategy(strat),
		pgeventstore.WithAppendTransactionHooks(hook),
	))
	storeB := must(pgeventstore.New(poolNamed(t, connString, "lo_writer_b"),
		pgeventstore.WithStrategy(strat),
	))

	var (
		writerAEvents []*eventstore.Event
		writerAErr    error
	)

	writerADone := make(chan struct{})
	go func() {
		defer close(writerADone)
		writerAEvents, writerAErr = storeA.AppendStream(t.Context(), streamA, newEvents(1), eventstore.AppendStreamOptions{})
	}()

	t.Cleanup(func() {
		select {
		case <-hook.proceed:
		default:
			close(hook.proceed)
		}
		select {
		case <-writerADone:
		case <-time.After(deadlockGuard):
			t.Errorf("deadlock guard: writer A never finished")
		}
	})

	select {
	case <-hook.entered:
	case <-writerADone:
		t.Fatalf("writer A finished before pausing: %v", writerAErr)
	case <-time.After(deadlockGuard):
		t.Fatal("deadlock guard: writer A never reached its transaction hook")
	}

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
		select {
		case <-writerBDone:
		case <-time.After(deadlockGuard):
			t.Errorf("deadlock guard: writer B never finished")
		}
	})

	// Wait until B is observed blocked by A on the allocator update, then let
	// A's hook touch B's stream row. Under the correct order B holds nothing,
	// so the hook proceeds; under the reversed order this same sequence
	// deadlocks and one writer errors.
	guard := time.Now().Add(deadlockGuard)
	for blocked := false; !blocked; {
		select {
		case <-writerADone:
			t.Fatalf("writer A finished while it should be paused in its hook: %v", writerAErr)
		case <-writerBDone:
			t.Fatalf("writer B finished before blocking on the allocator: err=%v", writerBErr)
		default:
		}

		if time.Now().After(guard) {
			t.Fatal("deadlock guard: writer B never blocked on the allocator")
		}

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
			  )`, "lo_writer_b", "lo_writer_a").Scan(&query)

		switch {
		case errors.Is(err, pgx.ErrNoRows):
			time.Sleep(5 * time.Millisecond)
		case err != nil:
			t.Fatalf("observing blocked writer: %v", err)
		default:
			if !strings.Contains(query, allocatorTable) {
				t.Fatalf("writer B is blocked by writer A on an unexpected statement: %s", query)
			}

			blocked = true
		}
	}

	close(hook.proceed)

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

	if writerAErr != nil || writerBErr != nil {
		t.Fatalf("want both writers to succeed without deadlocking, got A: %v, B: %v", writerAErr, writerBErr)
	}

	if aPos, bPos := *writerAEvents[0].GlobalPosition, *writerBEvents[0].GlobalPosition; bPos <= aPos {
		t.Errorf("want writer B's position above writer A's %d, got %d", aPos, bPos)
	}
}

// TestAppendStream_UniqueViolationClassification pins that only the
// stream-offset constraint reports a version conflict. The events table is
// deliberately named so its primary key's name contains "stream_offset",
// which a substring classification would misread as a lost stream race,
// masking allocator corruption.
func TestAppendStream_UniqueViolationClassification(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Parallel()

	db, err := createPostgresContainer(t)
	if err != nil {
		t.Fatalf("failed to create Postgres container: %v", err)
	}

	const eventsTable = "orders_stream_offset"

	strat := must(strategy.NewDefaultStrategy(
		strategy.WithEventsTableName(eventsTable),
		strategy.WithStreamsTableName("stream_uc"),
	))

	if _, err := db.Exec(t.Context(), strat.Schema()); err != nil {
		t.Fatalf("creating tables: %v", err)
	}

	store := must(pgeventstore.New(db, pgeventstore.WithStrategy(strat)))

	newEvent := []*eventstore.WritableEvent{{Type: "ucevent", Data: []byte(`{}`)}}

	t.Run("a stream-offset collision is a version conflict", func(t *testing.T) {
		desynced := typeid.NewV4("ucstream")

		// An event row with no streams-table row: the next append computes
		// offset 1 from the empty highwater and collides on the constraint.
		if _, err := db.Exec(t.Context(), fmt.Sprintf(`
			INSERT INTO %s (id, stream_id, stream_type, event_id, event_type, stream_offset, timestamp, data, data_content_type)
			VALUES (999, $1, $2, $3, $4, 1, now(), '{}'::jsonb, '')`,
			pgx.Identifier{eventsTable}.Sanitize(),
		), desynced.UUID, desynced.Type, uuid.Must(uuid.NewV4()), "ucevent"); err != nil {
			t.Fatalf("inserting desynced event row: %v", err)
		}

		_, err := store.AppendStream(t.Context(), desynced, newEvent, eventstore.AppendStreamOptions{})
		if !errors.Is(err, eventstore.StreamVersionMismatchError{}) {
			t.Fatalf("want a stream version mismatch from the offset collision, got %v", err)
		}
	})

	t.Run("an id collision surfaces as corruption, not a version conflict", func(t *testing.T) {
		if _, err := store.AppendStream(t.Context(), typeid.NewV4("ucstream"), newEvent, eventstore.AppendStreamOptions{}); err != nil {
			t.Fatalf("appending first event: %v", err)
		}

		// Rewind the allocator so the next reservation collides on the id
		// primary key — whose name also contains "stream_offset".
		if _, err := db.Exec(t.Context(), fmt.Sprintf(
			`UPDATE %s SET last_position = 0`,
			pgx.Identifier{eventsTable + "_position_allocator"}.Sanitize(),
		)); err != nil {
			t.Fatalf("rewinding allocator: %v", err)
		}

		_, err := store.AppendStream(t.Context(), typeid.NewV4("ucstream"), newEvent, eventstore.AppendStreamOptions{})

		var pgErr *pgconn.PgError
		switch {
		case err == nil:
			t.Fatal("want the id collision to fail the append")
		case errors.Is(err, eventstore.StreamVersionMismatchError{}):
			t.Fatalf("want the id collision reported as corruption, got a version conflict: %v", err)
		case !errors.As(err, &pgErr) || pgErr.Code != "23505":
			t.Fatalf("want the underlying unique violation surfaced, got %v", err)
		}
	})
}

// A positionDriftingStrategy persists a different position than it reserved,
// as an out-of-date external strategy retaining database-generated IDs would.
type positionDriftingStrategy struct {
	*strategy.DefaultStrategy

	eventsTable string
}

func (s positionDriftingStrategy) AppendStreamStatement() (string, error) {
	return fmt.Sprintf(`
		INSERT INTO %s (id, event_id, stream_type, stream_id, event_type, timestamp, stream_offset, data, data_content_type, metadata)
		VALUES ($1 + 1000, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		RETURNING id
	`, pgx.Identifier{s.eventsTable}.Sanitize()), nil
}

// TestAppendStream_VerifiesPersistedPosition pins the append-statement
// contract: the persisted position must equal the reservation, so a strategy
// that diverges fails loudly on its first append instead of returning
// positions that differ from what readers will see.
func TestAppendStream_VerifiesPersistedPosition(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Parallel()

	db, err := createPostgresContainer(t)
	if err != nil {
		t.Fatalf("failed to create Postgres container: %v", err)
	}

	const eventsTable = "event_vp"

	base := must(strategy.NewDefaultStrategy(
		strategy.WithEventsTableName(eventsTable),
		strategy.WithStreamsTableName("stream_vp"),
	))

	if _, err := db.Exec(t.Context(), base.Schema()); err != nil {
		t.Fatalf("creating tables: %v", err)
	}

	store := must(pgeventstore.New(db, pgeventstore.WithStrategy(positionDriftingStrategy{
		DefaultStrategy: base,
		eventsTable:     eventsTable,
	})))

	_, err = store.AppendStream(t.Context(), typeid.NewV4("vpstream"),
		[]*eventstore.WritableEvent{{Type: "vpevent", Data: []byte(`{}`)}}, eventstore.AppendStreamOptions{})

	if err == nil || !strings.Contains(err.Error(), "persisted position") {
		t.Fatalf("want the drifting strategy refused with a persisted-position error, got %v", err)
	}
}
