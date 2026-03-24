package outbox_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	pgeventstore "github.com/go-estoria/estoria-contrib/postgres/eventstore"
	"github.com/go-estoria/estoria-contrib/postgres/eventstore/strategy"
	pgoutbox "github.com/go-estoria/estoria-contrib/postgres/outbox"
	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
)

// collectingHandler returns an ItemHandler that appends each received item to a shared slice.
// mu protects the slice so that goroutine-parallel tests remain race-free.
func collectingHandler(mu *sync.Mutex, items *[]*pgoutbox.Item) pgoutbox.ItemHandler {
	return func(_ context.Context, item *pgoutbox.Item) error {
		mu.Lock()
		defer mu.Unlock()
		*items = append(*items, item)
		return nil
	}
}

// appendEvents is a convenience wrapper that appends the given writable events to the stream
// and fatals the test on error.
func appendEvents(t *testing.T, ctx context.Context, es *pgeventstore.EventStore, streamID typeid.ID, events []*eventstore.WritableEvent) {
	t.Helper()
	if err := es.AppendStream(ctx, streamID, events, eventstore.AppendStreamOptions{}); err != nil {
		t.Fatalf("AppendStream(%s): %v", streamID.String(), err)
	}
}

// newEventStore is a helper that creates an event store with the given strategy and hooks,
// creating the DB schema and fataling the test on error.
func newEventStore(t *testing.T, ctx context.Context, db *sql.DB, strat pgeventstore.Strategy, hooks ...pgeventstore.TransactionHook) *pgeventstore.EventStore {
	t.Helper()
	if _, err := db.ExecContext(ctx, strat.Schema()); err != nil {
		t.Fatalf("creating event store schema: %v", err)
	}
	opts := []pgeventstore.EventStoreOption{pgeventstore.WithStrategy(strat)}
	if len(hooks) > 0 {
		opts = append(opts, pgeventstore.WithAppendTransactionHooks(hooks...))
	}
	es, err := pgeventstore.New(db, opts...)
	if err != nil {
		t.Fatalf("creating event store: %v", err)
	}
	return es
}

// newOutbox is a helper that creates an outbox with the given handler and options,
// creating the DB schema and fataling the test on error.
func newOutbox(t *testing.T, ctx context.Context, db *sql.DB, handler pgoutbox.ItemHandler, opts ...pgoutbox.Option) *pgoutbox.Outbox {
	t.Helper()
	ob, err := pgoutbox.New(db, handler, opts...)
	if err != nil {
		t.Fatalf("creating outbox: %v", err)
	}
	if _, err := db.ExecContext(ctx, ob.Schema()); err != nil {
		t.Fatalf("creating outbox schema: %v", err)
	}
	return ob
}

// ---------------------------------------------------------------------------
// TestOutbox_HandleEvents — Tests the TransactionHook behavior
// ---------------------------------------------------------------------------

func TestOutbox_HandleEvents(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("skipping integration test")
	}

	for _, tt := range []struct {
		name               string
		outboxOpts         []pgoutbox.Option
		createOutboxSchema bool
		withEvents         []*eventstore.WritableEvent
		wantOutboxCount    int
		wantAppendErr      bool
		wantStreamNotFound bool
	}{
		{
			name:               "inserts items for each appended event",
			createOutboxSchema: true,
			withEvents: []*eventstore.WritableEvent{
				{Type: "thing_happened", Data: []byte(`{"n":1}`)},
				{Type: "thing_happened", Data: []byte(`{"n":2}`)},
				{Type: "thing_happened", Data: []byte(`{"n":3}`)},
			},
			wantOutboxCount: 3,
		},
		{
			name: "rolls back entire transaction on hook failure",
			outboxOpts: []pgoutbox.Option{
				pgoutbox.WithTableName("nonexistent_outbox"),
			},
			// createOutboxSchema is false: the table is deliberately not created.
			withEvents: []*eventstore.WritableEvent{
				{Type: "thing_happened", Data: []byte(`{"x":1}`)},
			},
			wantAppendErr:      true,
			wantStreamNotFound: true,
		},
	} {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()

			db, err := createPostgresContainer(t, ctx)
			if err != nil {
				t.Fatalf("createPostgresContainer: %v", err)
			}

			strat := must(strategy.NewDefaultStrategy())
			if _, err := db.ExecContext(ctx, strat.Schema()); err != nil {
				t.Fatalf("creating event store schema: %v", err)
			}

			ob, err := pgoutbox.New(db,
				func(_ context.Context, _ *pgoutbox.Item) error { return nil },
				tt.outboxOpts...,
			)
			if err != nil {
				t.Fatalf("creating outbox: %v", err)
			}

			if tt.createOutboxSchema {
				if _, err := db.ExecContext(ctx, ob.Schema()); err != nil {
					t.Fatalf("creating outbox schema: %v", err)
				}
			}

			es := must(pgeventstore.New(db,
				pgeventstore.WithStrategy(strat),
				pgeventstore.WithAppendTransactionHooks(ob),
			))

			streamID := typeid.NewV4("mystream")
			appendErr := es.AppendStream(ctx, streamID, tt.withEvents, eventstore.AppendStreamOptions{})

			if tt.wantAppendErr {
				if appendErr == nil {
					t.Fatal("expected AppendStream to fail, but got nil error")
				}
			} else {
				if appendErr != nil {
					t.Fatalf("unexpected AppendStream error: %v", appendErr)
				}
			}

			if tt.wantStreamNotFound {
				_, readErr := es.ReadStream(ctx, streamID, eventstore.ReadStreamOptions{})
				if !errors.Is(readErr, eventstore.ErrStreamNotFound) {
					t.Errorf("expected ErrStreamNotFound after rollback, got: %v", readErr)
				}
				return
			}

			// Query the outbox table directly to verify row count and field values.
			rows, err := db.QueryContext(ctx,
				`SELECT id, event_id, event_type, stream_id, stream_type, stream_version, data
				 FROM outbox
				 ORDER BY id ASC`,
			)
			if err != nil {
				t.Fatalf("querying outbox table: %v", err)
			}
			defer rows.Close()

			type outboxRow struct {
				id            int64
				eventType     string
				streamType    string
				streamVersion int64
				data          []byte
			}

			var got []outboxRow
			for rows.Next() {
				var r outboxRow
				var eventUUID, streamUUID string
				if err := rows.Scan(&r.id, &eventUUID, &r.eventType, &streamUUID, &r.streamType, &r.streamVersion, &r.data); err != nil {
					t.Fatalf("scanning outbox row: %v", err)
				}
				if eventUUID == "" {
					t.Errorf("row %d: event_id is empty", r.id)
				}
				if streamUUID == "" {
					t.Errorf("row %d: stream_id is empty", r.id)
				}
				got = append(got, r)
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("iterating outbox rows: %v", err)
			}

			if len(got) != tt.wantOutboxCount {
				t.Fatalf("expected %d outbox rows, got %d", tt.wantOutboxCount, len(got))
			}

			for i, r := range got {
				if r.eventType != tt.withEvents[i].Type {
					t.Errorf("row %d: event_type = %q, want %q", i, r.eventType, tt.withEvents[i].Type)
				}
				if r.streamType != streamID.Type {
					t.Errorf("row %d: stream_type = %q, want %q", i, r.streamType, streamID.Type)
				}
				if r.streamVersion != int64(i+1) {
					t.Errorf("row %d: stream_version = %d, want %d", i, r.streamVersion, i+1)
				}
				if string(r.data) == "" {
					t.Errorf("row %d: data is empty", i)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TestOutbox_ProcessNext — Tests processing behavior
// ---------------------------------------------------------------------------

func TestOutbox_ProcessNext(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("skipping integration test")
	}

	// Each test case receives a fresh db and strategy and is responsible for
	// constructing the outbox (with its handler), event store, and all assertions.
	// This keeps the loop body trivially uniform while allowing full flexibility
	// per case (e.g., stateful handlers, variable post-processing assertions).
	for _, tt := range []struct {
		name string
		run  func(t *testing.T, ctx context.Context, db *sql.DB, strat pgeventstore.Strategy)
	}{
		{
			name: "processes items one at a time in order",
			run: func(t *testing.T, ctx context.Context, db *sql.DB, strat pgeventstore.Strategy) {
				t.Helper()

				var mu sync.Mutex
				var received []*pgoutbox.Item

				ob := newOutbox(t, ctx, db, collectingHandler(&mu, &received))
				es := newEventStore(t, ctx, db, strat, ob)

				streamID := typeid.NewV4("mystream")
				appendEvents(t, ctx, es, streamID, []*eventstore.WritableEvent{
					{Type: "first", Data: []byte(`{"seq":1}`)},
					{Type: "second", Data: []byte(`{"seq":2}`)},
				})

				// First call should deliver the first item.
				if err := ob.ProcessNext(ctx); err != nil {
					t.Fatalf("first ProcessNext: %v", err)
				}
				mu.Lock()
				if len(received) != 1 {
					mu.Unlock()
					t.Fatalf("after first ProcessNext: expected 1 item received, got %d", len(received))
				}
				firstItem := received[0]
				mu.Unlock()

				if firstItem.EventID.Type != "first" {
					t.Errorf("first item: EventID.Type = %q, want %q", firstItem.EventID.Type, "first")
				}
				if firstItem.StreamVersion != 1 {
					t.Errorf("first item: StreamVersion = %d, want 1", firstItem.StreamVersion)
				}

				// Second call should deliver the second item.
				if err := ob.ProcessNext(ctx); err != nil {
					t.Fatalf("second ProcessNext: %v", err)
				}
				mu.Lock()
				if len(received) != 2 {
					mu.Unlock()
					t.Fatalf("after second ProcessNext: expected 2 items received, got %d", len(received))
				}
				secondItem := received[1]
				mu.Unlock()

				if secondItem.EventID.Type != "second" {
					t.Errorf("second item: EventID.Type = %q, want %q", secondItem.EventID.Type, "second")
				}
				if secondItem.StreamVersion != 2 {
					t.Errorf("second item: StreamVersion = %d, want 2", secondItem.StreamVersion)
				}

				// Items must be delivered in insertion (ID) order.
				if firstItem.ID >= secondItem.ID {
					t.Errorf("expected first item ID (%d) < second item ID (%d)", firstItem.ID, secondItem.ID)
				}
			},
		},
		{
			name: "returns ErrNoItems when outbox is empty",
			run: func(t *testing.T, ctx context.Context, db *sql.DB, strat pgeventstore.Strategy) {
				t.Helper()

				// Only the outbox table is needed; no event store required.
				ob := newOutbox(t, ctx, db,
					func(_ context.Context, _ *pgoutbox.Item) error { return nil },
				)

				err := ob.ProcessNext(ctx)
				if !errors.Is(err, pgoutbox.ErrNoItems) {
					t.Errorf("expected ErrNoItems, got: %v", err)
				}
			},
		},
		{
			name: "returns ErrNoItems when all items are processed",
			run: func(t *testing.T, ctx context.Context, db *sql.DB, strat pgeventstore.Strategy) {
				t.Helper()

				ob := newOutbox(t, ctx, db,
					func(_ context.Context, _ *pgoutbox.Item) error { return nil },
				)
				es := newEventStore(t, ctx, db, strat, ob)

				streamID := typeid.NewV4("mystream")
				appendEvents(t, ctx, es, streamID, []*eventstore.WritableEvent{
					{Type: "e", Data: []byte(`{}`)},
					{Type: "e", Data: []byte(`{}`)},
				})

				// Drain all items.
				for {
					if err := ob.ProcessNext(ctx); errors.Is(err, pgoutbox.ErrNoItems) {
						break
					} else if err != nil {
						t.Fatalf("ProcessNext during drain: %v", err)
					}
				}

				// One more call must return ErrNoItems.
				if err := ob.ProcessNext(ctx); !errors.Is(err, pgoutbox.ErrNoItems) {
					t.Errorf("expected ErrNoItems after all items processed, got: %v", err)
				}
			},
		},
		{
			name: "handler error leaves item unprocessed for retry",
			run: func(t *testing.T, ctx context.Context, db *sql.DB, strat pgeventstore.Strategy) {
				t.Helper()

				// Stateful handler: fail on the first call, succeed on the second.
				failNext := true
				var mu sync.Mutex
				var received []*pgoutbox.Item

				handler := func(_ context.Context, item *pgoutbox.Item) error {
					mu.Lock()
					defer mu.Unlock()
					if failNext {
						failNext = false
						return fmt.Errorf("simulated handler failure")
					}
					received = append(received, item)
					return nil
				}

				ob := newOutbox(t, ctx, db, handler)
				es := newEventStore(t, ctx, db, strat, ob)

				streamID := typeid.NewV4("mystream")
				appendEvents(t, ctx, es, streamID, []*eventstore.WritableEvent{
					{Type: "important_event", Data: []byte(`{"value":"critical"}`)},
				})

				// First call: handler fails — ProcessNext should return a non-nil error.
				if err := ob.ProcessNext(ctx); err == nil {
					t.Fatal("expected ProcessNext to return an error when handler fails, but got nil")
				}

				// Second call: handler succeeds — same item should now be delivered.
				if err := ob.ProcessNext(ctx); err != nil {
					t.Fatalf("second ProcessNext (handler succeeds): %v", err)
				}

				mu.Lock()
				defer mu.Unlock()

				if len(received) != 1 {
					t.Fatalf("expected 1 item received on retry, got %d", len(received))
				}
				if received[0].EventID.Type != "important_event" {
					t.Errorf("retried item: EventID.Type = %q, want %q", received[0].EventID.Type, "important_event")
				}
				if string(received[0].Data) == "" {
					t.Errorf("retried item: Data is empty")
				}
			},
		},
		{
			name: "processes items across multiple streams in insertion order",
			run: func(t *testing.T, ctx context.Context, db *sql.DB, strat pgeventstore.Strategy) {
				t.Helper()

				var mu sync.Mutex
				var received []*pgoutbox.Item

				ob := newOutbox(t, ctx, db, collectingHandler(&mu, &received))
				es := newEventStore(t, ctx, db, strat, ob)

				streamA := typeid.NewV4("streamA")
				streamB := typeid.NewV4("streamB")
				streamC := typeid.NewV4("streamC")

				// Interleave appends across streams.
				appendEvents(t, ctx, es, streamA, []*eventstore.WritableEvent{{Type: "e", Data: []byte(`{"s":"A","n":1}`)}})
				appendEvents(t, ctx, es, streamB, []*eventstore.WritableEvent{{Type: "e", Data: []byte(`{"s":"B","n":1}`)}})
				appendEvents(t, ctx, es, streamC, []*eventstore.WritableEvent{{Type: "e", Data: []byte(`{"s":"C","n":1}`)}})
				appendEvents(t, ctx, es, streamA, []*eventstore.WritableEvent{{Type: "e", Data: []byte(`{"s":"A","n":2}`)}})
				appendEvents(t, ctx, es, streamB, []*eventstore.WritableEvent{{Type: "e", Data: []byte(`{"s":"B","n":2}`)}})

				const totalItems = 5
				for i := 0; i < totalItems; i++ {
					if err := ob.ProcessNext(ctx); err != nil {
						t.Fatalf("ProcessNext iteration %d: %v", i, err)
					}
				}

				mu.Lock()
				defer mu.Unlock()

				if len(received) != totalItems {
					t.Fatalf("expected %d items, got %d", totalItems, len(received))
				}

				// Verify strict ascending ID order.
				for i := 1; i < len(received); i++ {
					if received[i].ID <= received[i-1].ID {
						t.Errorf("items not in ascending ID order: item[%d].ID=%d, item[%d].ID=%d",
							i-1, received[i-1].ID, i, received[i].ID)
					}
				}
			},
		},
	} {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()

			db, err := createPostgresContainer(t, ctx)
			if err != nil {
				t.Fatalf("createPostgresContainer: %v", err)
			}

			strat := must(strategy.NewDefaultStrategy())

			tt.run(t, ctx, db, strat)
		})
	}
}

// ---------------------------------------------------------------------------
// TestOutbox_Run — Tests the polling loop
// ---------------------------------------------------------------------------

func TestOutbox_Run(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("skipping integration test")
	}

	for _, tt := range []struct {
		name      string
		numEvents int
	}{
		{
			name:      "processes all items and stops on context cancellation",
			numEvents: 4,
		},
	} {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			db, err := createPostgresContainer(t, ctx)
			if err != nil {
				t.Fatalf("createPostgresContainer: %v", err)
			}

			strat := must(strategy.NewDefaultStrategy())
			if _, err := db.ExecContext(ctx, strat.Schema()); err != nil {
				t.Fatalf("creating event store schema: %v", err)
			}

			var mu sync.Mutex
			var received []*pgoutbox.Item

			ob := must(pgoutbox.New(db,
				collectingHandler(&mu, &received),
				pgoutbox.WithPollInterval(50*time.Millisecond),
			))
			if _, err := db.ExecContext(ctx, ob.Schema()); err != nil {
				t.Fatalf("creating outbox schema: %v", err)
			}

			es := must(pgeventstore.New(db,
				pgeventstore.WithStrategy(strat),
				pgeventstore.WithAppendTransactionHooks(ob),
			))

			streamID := typeid.NewV4("mystream")
			events := make([]*eventstore.WritableEvent, tt.numEvents)
			for i := range events {
				events[i] = &eventstore.WritableEvent{
					Type: "e",
					Data: []byte(fmt.Sprintf(`{"n":%d}`, i+1)),
				}
			}
			appendEvents(t, ctx, es, streamID, events)

			runErr := make(chan error, 1)
			go func() {
				runErr <- ob.Run(ctx)
			}()

			// Poll until all items have been processed or we time out.
			deadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(deadline) {
				mu.Lock()
				n := len(received)
				mu.Unlock()
				if n >= tt.numEvents {
					break
				}
				time.Sleep(25 * time.Millisecond)
			}

			cancel()

			select {
			case err := <-runErr:
				if err != nil {
					t.Errorf("Run returned unexpected error: %v", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("Run did not stop after context cancellation")
			}

			mu.Lock()
			defer mu.Unlock()

			if len(received) != tt.numEvents {
				t.Errorf("expected %d items processed by Run, got %d", tt.numEvents, len(received))
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TestOutbox_RetryAndDeadLetter — Tests retry counting and dead-letter behavior
// ---------------------------------------------------------------------------

func TestOutbox_RetryAndDeadLetter(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("skipping integration test")
	}

	for _, tt := range []struct {
		name string
		run  func(t *testing.T, ctx context.Context, db *sql.DB, strat pgeventstore.Strategy)
	}{
		{
			name: "handler_failure_increments_retry_count",
			run: func(t *testing.T, ctx context.Context, db *sql.DB, strat pgeventstore.Strategy) {
				t.Helper()

				// Handler fails on the first call, succeeds on the second.
				callCount := 0
				var mu sync.Mutex
				handler := func(_ context.Context, item *pgoutbox.Item) error {
					mu.Lock()
					defer mu.Unlock()
					callCount++
					if callCount == 1 {
						return fmt.Errorf("simulated failure")
					}
					return nil
				}

				ob := newOutbox(t, ctx, db, handler)
				es := newEventStore(t, ctx, db, strat, ob)

				streamID := typeid.NewV4("mystream")
				appendEvents(t, ctx, es, streamID, []*eventstore.WritableEvent{
					{Type: "retryable_event", Data: []byte(`{"x":1}`)},
				})

				// First call: handler fails; retry count should be committed as 1.
				if err := ob.ProcessNext(ctx); err == nil {
					t.Fatal("expected ProcessNext to return error on first handler failure")
				}

				// Verify retry_count = 1 in the database.
				var retryCount int
				var failedAt *time.Time
				var itemID int64
				if err := db.QueryRowContext(ctx,
					`SELECT id, retry_count, failed_at FROM outbox WHERE event_type = 'retryable_event'`,
				).Scan(&itemID, &retryCount, &failedAt); err != nil {
					t.Fatalf("querying outbox item after first failure: %v", err)
				}
				if retryCount != 1 {
					t.Errorf("after first failure: retry_count = %d, want 1", retryCount)
				}
				if failedAt != nil {
					t.Errorf("after first failure: failed_at should be nil, got %v", failedAt)
				}

				// Second call: handler succeeds; item should now be processed.
				if err := ob.ProcessNext(ctx); err != nil {
					t.Fatalf("second ProcessNext (handler succeeds): %v", err)
				}

				// Verify processed_at is now set and retry_count is still 1.
				var processedAt *time.Time
				if err := db.QueryRowContext(ctx,
					`SELECT retry_count, processed_at, failed_at FROM outbox WHERE id = $1`, itemID,
				).Scan(&retryCount, &processedAt, &failedAt); err != nil {
					t.Fatalf("querying outbox item after second call: %v", err)
				}
				if retryCount != 1 {
					t.Errorf("after success: retry_count = %d, want 1", retryCount)
				}
				if processedAt == nil {
					t.Error("after success: processed_at should be set, got nil")
				}
				if failedAt != nil {
					t.Errorf("after success: failed_at should be nil, got %v", failedAt)
				}
			},
		},
		{
			name: "item_dead_lettered_after_max_retries",
			run: func(t *testing.T, ctx context.Context, db *sql.DB, strat pgeventstore.Strategy) {
				t.Helper()

				// Handler always fails.
				handler := func(_ context.Context, _ *pgoutbox.Item) error {
					return fmt.Errorf("always fails")
				}

				const maxRetries = 3
				ob := newOutbox(t, ctx, db, handler, pgoutbox.WithMaxRetries(maxRetries))
				es := newEventStore(t, ctx, db, strat, ob)

				streamID := typeid.NewV4("mystream")
				appendEvents(t, ctx, es, streamID, []*eventstore.WritableEvent{
					{Type: "doomed_event", Data: []byte(`{}`)},
				})

				// Call ProcessNext maxRetries times — each should return a handler error.
				for i := 0; i < maxRetries; i++ {
					if err := ob.ProcessNext(ctx); err == nil {
						t.Fatalf("call %d: expected error, got nil", i+1)
					}
				}

				// The (maxRetries+1)th call triggers dead-lettering.
				if err := ob.ProcessNext(ctx); err == nil {
					t.Fatal("expected error when dead-lettering item, got nil")
				}

				// Verify failed_at is now set in the database.
				var failedAt *time.Time
				var retryCount int
				if err := db.QueryRowContext(ctx,
					`SELECT retry_count, failed_at FROM outbox WHERE event_type = 'doomed_event'`,
				).Scan(&retryCount, &failedAt); err != nil {
					t.Fatalf("querying outbox item after dead letter: %v", err)
				}
				if failedAt == nil {
					t.Error("expected failed_at to be set after max retries exceeded, got nil")
				}
				if retryCount != maxRetries+1 {
					t.Errorf("retry_count = %d, want %d", retryCount, maxRetries+1)
				}

				// ProcessNext should now return ErrNoItems because the dead-lettered item is skipped.
				if err := ob.ProcessNext(ctx); !errors.Is(err, pgoutbox.ErrNoItems) {
					t.Errorf("expected ErrNoItems after dead-lettered item, got: %v", err)
				}
			},
		},
		{
			name: "max_retries_zero_means_infinite",
			run: func(t *testing.T, ctx context.Context, db *sql.DB, strat pgeventstore.Strategy) {
				t.Helper()

				// Handler always fails — with maxRetries=0 it should never be dead-lettered.
				handler := func(_ context.Context, _ *pgoutbox.Item) error {
					return fmt.Errorf("always fails")
				}

				ob := newOutbox(t, ctx, db, handler, pgoutbox.WithMaxRetries(0))
				es := newEventStore(t, ctx, db, strat, ob)

				streamID := typeid.NewV4("mystream")
				appendEvents(t, ctx, es, streamID, []*eventstore.WritableEvent{
					{Type: "infinite_retry_event", Data: []byte(`{}`)},
				})

				const iterations = 20
				for i := 0; i < iterations; i++ {
					err := ob.ProcessNext(ctx)
					if err == nil {
						t.Fatalf("iteration %d: expected error from always-failing handler, got nil", i+1)
					}
					// Must not be ErrNoItems — the item must still be visible.
					if errors.Is(err, pgoutbox.ErrNoItems) {
						t.Fatalf("iteration %d: got ErrNoItems, item was unexpectedly dead-lettered", i+1)
					}
				}

				// Verify the item is still alive (failed_at IS NULL) with a high retry count.
				var failedAt *time.Time
				var retryCount int
				if err := db.QueryRowContext(ctx,
					`SELECT retry_count, failed_at FROM outbox WHERE event_type = 'infinite_retry_event'`,
				).Scan(&retryCount, &failedAt); err != nil {
					t.Fatalf("querying outbox item: %v", err)
				}
				if failedAt != nil {
					t.Errorf("expected failed_at to remain nil with maxRetries=0, got %v", failedAt)
				}
				if retryCount != iterations {
					t.Errorf("retry_count = %d, want %d", retryCount, iterations)
				}
			},
		},
		{
			name: "dead_lettered_items_skipped",
			run: func(t *testing.T, ctx context.Context, db *sql.DB, strat pgeventstore.Strategy) {
				t.Helper()

				// Track which event type the handler is called with so we can confirm
				// event 1 is dead-lettered and event 2 is processed.
				var mu sync.Mutex
				processedTypes := make([]string, 0, 2)

				// Handler fails for "event_one", succeeds for "event_two".
				handler := func(_ context.Context, item *pgoutbox.Item) error {
					mu.Lock()
					defer mu.Unlock()
					if item.EventID.Type == "event_one" {
						return fmt.Errorf("event_one always fails")
					}
					processedTypes = append(processedTypes, item.EventID.Type)
					return nil
				}

				const maxRetries = 1
				ob := newOutbox(t, ctx, db, handler, pgoutbox.WithMaxRetries(maxRetries))
				es := newEventStore(t, ctx, db, strat, ob)

				streamID := typeid.NewV4("mystream")
				appendEvents(t, ctx, es, streamID, []*eventstore.WritableEvent{
					{Type: "event_one", Data: []byte(`{}`)},
					{Type: "event_two", Data: []byte(`{}`)},
				})

				// Call 1: event_one handler fails, retry_count becomes 1.
				if err := ob.ProcessNext(ctx); err == nil {
					t.Fatal("call 1: expected error for event_one, got nil")
				}

				// Call 2: event_one retry_count (1) reaches maxRetries (1), so
				// newRetryCount (2) > maxRetries (1) → dead-lettered.
				if err := ob.ProcessNext(ctx); err == nil {
					t.Fatal("call 2: expected error when dead-lettering event_one, got nil")
				}

				// Call 3: event_one is now skipped; event_two is claimed and processed.
				if err := ob.ProcessNext(ctx); err != nil {
					t.Fatalf("call 3: expected event_two to succeed, got: %v", err)
				}

				// Verify database state.
				var failedAt *time.Time
				var processedAt *time.Time
				var retryCount int

				if err := db.QueryRowContext(ctx,
					`SELECT retry_count, failed_at FROM outbox WHERE event_type = 'event_one'`,
				).Scan(&retryCount, &failedAt); err != nil {
					t.Fatalf("querying event_one: %v", err)
				}
				if failedAt == nil {
					t.Error("event_one: expected failed_at to be set, got nil")
				}

				if err := db.QueryRowContext(ctx,
					`SELECT processed_at FROM outbox WHERE event_type = 'event_two'`,
				).Scan(&processedAt); err != nil {
					t.Fatalf("querying event_two: %v", err)
				}
				if processedAt == nil {
					t.Error("event_two: expected processed_at to be set, got nil")
				}

				mu.Lock()
				defer mu.Unlock()
				if len(processedTypes) != 1 || processedTypes[0] != "event_two" {
					t.Errorf("handler processed types = %v, want [event_two]", processedTypes)
				}
			},
		},
		{
			name: "retry_count_visible_to_handler",
			run: func(t *testing.T, ctx context.Context, db *sql.DB, strat pgeventstore.Strategy) {
				t.Helper()

				// Record the RetryCount the handler sees on each invocation.
				var mu sync.Mutex
				seenRetryCounts := make([]int, 0, 3)

				// Handler fails when RetryCount < 2, succeeds otherwise.
				handler := func(_ context.Context, item *pgoutbox.Item) error {
					mu.Lock()
					seenRetryCounts = append(seenRetryCounts, item.RetryCount)
					shouldFail := item.RetryCount < 2
					mu.Unlock()
					if shouldFail {
						return fmt.Errorf("not enough retries yet (retry_count=%d)", item.RetryCount)
					}
					return nil
				}

				// maxRetries=5 gives plenty of budget so the item won't be dead-lettered.
				ob := newOutbox(t, ctx, db, handler, pgoutbox.WithMaxRetries(5))
				es := newEventStore(t, ctx, db, strat, ob)

				streamID := typeid.NewV4("mystream")
				appendEvents(t, ctx, es, streamID, []*eventstore.WritableEvent{
					{Type: "counted_event", Data: []byte(`{}`)},
				})

				// Call 1: handler sees RetryCount=0, fails.
				if err := ob.ProcessNext(ctx); err == nil {
					t.Fatal("call 1: expected handler failure, got nil")
				}
				// Call 2: handler sees RetryCount=1, fails.
				if err := ob.ProcessNext(ctx); err == nil {
					t.Fatal("call 2: expected handler failure, got nil")
				}
				// Call 3: handler sees RetryCount=2, succeeds.
				if err := ob.ProcessNext(ctx); err != nil {
					t.Fatalf("call 3: expected success, got: %v", err)
				}

				mu.Lock()
				defer mu.Unlock()

				if len(seenRetryCounts) != 3 {
					t.Fatalf("handler was called %d times, want 3", len(seenRetryCounts))
				}
				for i, want := range []int{0, 1, 2} {
					if seenRetryCounts[i] != want {
						t.Errorf("call %d: handler saw RetryCount=%d, want %d", i+1, seenRetryCounts[i], want)
					}
				}

				// The item should now be processed.
				var processedAt *time.Time
				if err := db.QueryRowContext(ctx,
					`SELECT processed_at FROM outbox WHERE event_type = 'counted_event'`,
				).Scan(&processedAt); err != nil {
					t.Fatalf("querying outbox item: %v", err)
				}
				if processedAt == nil {
					t.Error("expected processed_at to be set after successful third call, got nil")
				}
			},
		},
	} {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()

			db, err := createPostgresContainer(t, ctx)
			if err != nil {
				t.Fatalf("createPostgresContainer: %v", err)
			}

			strat := must(strategy.NewDefaultStrategy())

			tt.run(t, ctx, db, strat)
		})
	}
}

// ---------------------------------------------------------------------------
// TestOutbox_RunConcurrency — Tests the Run() concurrency guard
// ---------------------------------------------------------------------------

func TestOutbox_RunConcurrency(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Run("run_rejects_concurrent_call", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		db, err := createPostgresContainer(t, ctx)
		if err != nil {
			t.Fatalf("createPostgresContainer: %v", err)
		}

		// Use a long poll interval so the first Run() stays alive long enough for
		// the second call to arrive while running is still true.
		ob := newOutbox(t, ctx, db,
			func(_ context.Context, _ *pgoutbox.Item) error { return nil },
			pgoutbox.WithPollInterval(10*time.Second),
		)

		firstRunErr := make(chan error, 1)
		go func() {
			firstRunErr <- ob.Run(ctx)
		}()

		// Give the first goroutine time to reach CompareAndSwap and set running=true.
		// The CompareAndSwap is the very first operation in Run(), so a short sleep
		// is sufficient on any reasonably loaded machine.
		time.Sleep(50 * time.Millisecond)

		// Attempt the second concurrent Run() call in its own goroutine so that if
		// the guard fires it returns immediately rather than blocking the test.
		secondRunErr := make(chan error, 1)
		go func() {
			secondRunErr <- ob.Run(ctx)
		}()

		select {
		case err := <-secondRunErr:
			if err == nil {
				cancel()
				t.Fatal("second Run() call succeeded; concurrency guard may not be working")
			}
			// Got the expected rejection — cancel the first Run and verify it exits cleanly.
			cancel()
			select {
			case runErr := <-firstRunErr:
				if runErr != nil {
					t.Errorf("first Run returned unexpected error: %v", runErr)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("first Run did not stop after context cancellation")
			}
		case <-time.After(2 * time.Second):
			cancel()
			t.Fatal("second Run() did not return within 2s; concurrency guard may not be working")
		}
	})
}

// ---------------------------------------------------------------------------
// TestOutbox_Options — Tests configuration options
// ---------------------------------------------------------------------------

func TestOutbox_Options(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("skipping integration test")
	}

	for _, tt := range []struct {
		name string
		run  func(t *testing.T, ctx context.Context)
	}{
		{
			name: "custom table name",
			run: func(t *testing.T, ctx context.Context) {
				t.Helper()

				db, err := createPostgresContainer(t, ctx)
				if err != nil {
					t.Fatalf("createPostgresContainer: %v", err)
				}

				strat := must(strategy.NewDefaultStrategy())
				if _, err := db.ExecContext(ctx, strat.Schema()); err != nil {
					t.Fatalf("creating event store schema: %v", err)
				}

				var mu sync.Mutex
				var received []*pgoutbox.Item

				const customTable = "my_custom_outbox"

				ob := newOutbox(t, ctx, db,
					collectingHandler(&mu, &received),
					pgoutbox.WithTableName(customTable),
				)

				// Verify the custom table was actually created (not the default "outbox").
				var tableCount int
				if err := db.QueryRowContext(ctx,
					`SELECT count(*) FROM information_schema.tables WHERE table_name = $1`, customTable,
				).Scan(&tableCount); err != nil {
					t.Fatalf("querying information_schema: %v", err)
				}
				if tableCount != 1 {
					t.Fatalf("expected custom table %q to exist, but it does not", customTable)
				}

				es := must(pgeventstore.New(db,
					pgeventstore.WithStrategy(strat),
					pgeventstore.WithAppendTransactionHooks(ob),
				))

				streamID := typeid.NewV4("mystream")
				appendEvents(t, ctx, es, streamID, []*eventstore.WritableEvent{
					{Type: "custom_event", Data: []byte(`{"custom":true}`)},
				})

				if err := ob.ProcessNext(ctx); err != nil {
					t.Fatalf("ProcessNext: %v", err)
				}

				mu.Lock()
				defer mu.Unlock()

				if len(received) != 1 {
					t.Fatalf("expected 1 processed item, got %d", len(received))
				}
				if received[0].EventID.Type != "custom_event" {
					t.Errorf("item EventID.Type = %q, want %q", received[0].EventID.Type, "custom_event")
				}
			},
		},
		{
			name: "end to end with multiple streams",
			run: func(t *testing.T, ctx context.Context) {
				t.Helper()

				db, err := createPostgresContainer(t, ctx)
				if err != nil {
					t.Fatalf("createPostgresContainer: %v", err)
				}

				strat := must(strategy.NewDefaultStrategy())
				if _, err := db.ExecContext(ctx, strat.Schema()); err != nil {
					t.Fatalf("creating event store schema: %v", err)
				}

				var mu sync.Mutex
				var received []*pgoutbox.Item

				ob := newOutbox(t, ctx, db, collectingHandler(&mu, &received))
				es := must(pgeventstore.New(db,
					pgeventstore.WithStrategy(strat),
					pgeventstore.WithAppendTransactionHooks(ob),
				))

				type appendedEvent struct {
					streamID typeid.ID
					version  int64
					typ      string
				}

				streamX := typeid.NewV4("mystream")
				streamY := typeid.NewV4("mystream")

				writableX := []*eventstore.WritableEvent{
					{Type: "order_placed", Data: []byte(`{"order_id":"x1"}`)},
					{Type: "order_shipped", Data: []byte(`{"order_id":"x2"}`)},
				}
				writableY := []*eventstore.WritableEvent{
					{Type: "payment_received", Data: []byte(`{"amount":99}`)},
				}

				appendEvents(t, ctx, es, streamX, writableX)
				appendEvents(t, ctx, es, streamY, writableY)

				var want []appendedEvent
				for i, w := range writableX {
					want = append(want, appendedEvent{streamID: streamX, version: int64(i + 1), typ: w.Type})
				}
				for i, w := range writableY {
					want = append(want, appendedEvent{streamID: streamY, version: int64(i + 1), typ: w.Type})
				}

				// Process all items.
				for i := 0; i < len(want); i++ {
					if err := ob.ProcessNext(ctx); err != nil {
						t.Fatalf("ProcessNext iteration %d: %v", i, err)
					}
				}

				// Verify ErrNoItems once exhausted.
				if err := ob.ProcessNext(ctx); !errors.Is(err, pgoutbox.ErrNoItems) {
					t.Errorf("expected ErrNoItems after all items processed, got: %v", err)
				}

				mu.Lock()
				defer mu.Unlock()

				if len(received) != len(want) {
					t.Fatalf("expected %d processed items, got %d", len(want), len(received))
				}

				for i, item := range received {
					w := want[i]

					if item.EventID.UUID.IsNil() {
						t.Errorf("item %d: EventID.UUID is nil", i)
					}
					if item.EventID.Type != w.typ {
						t.Errorf("item %d: EventID.Type = %q, want %q", i, item.EventID.Type, w.typ)
					}
					if item.StreamID != w.streamID {
						t.Errorf("item %d: StreamID = %v, want %v", i, item.StreamID, w.streamID)
					}
					if item.StreamVersion != w.version {
						t.Errorf("item %d: StreamVersion = %d, want %d", i, item.StreamVersion, w.version)
					}
					if string(item.Data) == "" {
						t.Errorf("item %d: Data is empty", i)
					}
				}
			},
		},
	} {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t, t.Context())
		})
	}
}
