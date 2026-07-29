package outbox_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mongoeventstore "github.com/go-estoria/estoria-contrib/mongodb/eventstore"
	mongooutbox "github.com/go-estoria/estoria-contrib/mongodb/outbox"
	es "github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
)

// drainProcessNext calls ProcessNext until ErrNoItems (or a non-handler error).
func drainProcessNext(t *testing.T, ctx context.Context, ob *mongooutbox.Outbox) {
	t.Helper()
	for {
		err := ob.ProcessNext(ctx)
		if errors.Is(err, mongooutbox.ErrNoItems) {
			return
		}
		if err != nil {
			// Handler failures are surfaced as errors; keep draining other streams.
			continue
		}
	}
}

func TestOutbox_Producer_WritesInAppendTxn(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	t.Parallel()

	ctx := context.Background()
	client := createMongoDBContainer(t, ctx)

	var mu sync.Mutex
	var got []*mongooutbox.Item
	h := newHarness(t, ctx, client, collectingHandler(&mu, &got), nil)

	streamID := typeid.NewV4("user")
	if err := h.store.AppendStream(ctx, streamID, writableEvents("evt", 3), es.AppendStreamOptions{}); err != nil {
		t.Fatalf("appending: %v", err)
	}

	if got := h.countOutbox(t, ctx); got != 3 {
		t.Fatalf("expected 3 outbox items, got %d", got)
	}
}

func TestOutbox_Producer_RollbackOnAppendFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	t.Parallel()

	ctx := context.Background()
	client := createMongoDBContainer(t, ctx)

	var mu sync.Mutex
	var got []*mongooutbox.Item
	// A failing hook registered after the outbox forces the whole append (and the outbox writes) to roll back.
	failHook := mongoeventstore.TransactionHookFunc(func(_ context.Context, _ []*es.Event) error {
		return errors.New("boom")
	})
	h := newHarness(t, ctx, client, collectingHandler(&mu, &got), nil, failHook)

	streamID := typeid.NewV4("user")
	if err := h.store.AppendStream(ctx, streamID, writableEvents("evt", 3), es.AppendStreamOptions{}); err == nil {
		t.Fatalf("expected append to fail")
	}

	if got := h.countOutbox(t, ctx); got != 0 {
		t.Fatalf("expected 0 outbox items after rollback, got %d", got)
	}
}

func TestOutbox_ProcessNext_DeliversInOrderAndDeletes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	t.Parallel()

	ctx := context.Background()
	client := createMongoDBContainer(t, ctx)

	var mu sync.Mutex
	var got []*mongooutbox.Item
	h := newHarness(t, ctx, client, collectingHandler(&mu, &got), nil)

	streamID := typeid.NewV4("user")
	if err := h.store.AppendStream(ctx, streamID, writableEvents("evt", 5), es.AppendStreamOptions{}); err != nil {
		t.Fatalf("appending: %v", err)
	}

	drainProcessNext(t, ctx, h.outbox)

	if len(got) != 5 {
		t.Fatalf("expected 5 delivered items, got %d", len(got))
	}
	for i, item := range got {
		if item.StreamVersion != int64(i+1) {
			t.Errorf("delivery %d: expected version %d, got %d", i, i+1, item.StreamVersion)
		}
	}

	// Delete-on-ack: the outbox is empty and ProcessNext reports no work.
	if got := h.countOutbox(t, ctx); got != 0 {
		t.Errorf("expected outbox empty after delivery, got %d items", got)
	}
	if err := h.outbox.ProcessNext(ctx); !errors.Is(err, mongooutbox.ErrNoItems) {
		t.Errorf("expected ErrNoItems, got %v", err)
	}
}

func TestOutbox_PerStreamFIFO_ConcurrentWorkers(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	t.Parallel()

	ctx := context.Background()
	client := createMongoDBContainer(t, ctx)

	var mu sync.Mutex
	deliveredByStream := map[string][]int64{}
	handler := func(_ context.Context, item *mongooutbox.Item) error {
		mu.Lock()
		deliveredByStream[item.StreamID.String()] = append(deliveredByStream[item.StreamID.String()], item.StreamVersion)
		mu.Unlock()
		return nil
	}

	h := newHarness(t, ctx, client, handler, []mongooutbox.Option{mongooutbox.WithPollInterval(20 * time.Millisecond)})

	// Interleave appends across several streams.
	const numStreams = 5
	const perStream = 8
	streams := make([]typeid.ID, numStreams)
	for i := range streams {
		streams[i] = typeid.NewV4("user")
	}
	for v := range perStream {
		for _, s := range streams {
			if err := h.store.AppendStream(ctx, s, writableEvents("evt", 1), es.AppendStreamOptions{ExpectVersion: es.VersionPtr(int64(v))}); err != nil {
				t.Fatalf("appending to %s: %v", s, err)
			}
		}
	}

	// Run several concurrent workers over the same collections.
	runCtx, cancel := context.WithCancel(ctx)
	workers := make([]*mongooutbox.Outbox, 3)
	var wg sync.WaitGroup
	for i := range workers {
		ob, err := mongooutbox.New(h.outboxColl, h.streamColl, handler, mongooutbox.WithPollInterval(20*time.Millisecond))
		if err != nil {
			t.Fatalf("creating worker %d: %v", i, err)
		}
		workers[i] = ob
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = ob.Run(runCtx)
		}()
	}

	// Wait until every stream is fully delivered (or time out).
	deadline := time.After(30 * time.Second)
	for {
		mu.Lock()
		total := 0
		for _, vs := range deliveredByStream {
			total += len(vs)
		}
		mu.Unlock()
		if total >= numStreams*perStream {
			break
		}
		select {
		case <-deadline:
			cancel()
			wg.Wait()
			t.Fatalf("timed out waiting for delivery; got %d/%d", total, numStreams*perStream)
		case <-time.After(50 * time.Millisecond):
		}
	}
	cancel()
	wg.Wait()

	// Assert strict per-stream FIFO: each stream's versions are exactly 1..perStream in order.
	mu.Lock()
	defer mu.Unlock()
	for _, s := range streams {
		vs := deliveredByStream[s.String()]
		if len(vs) != perStream {
			t.Errorf("stream %s: expected %d deliveries, got %d (%v)", s, perStream, len(vs), vs)
			continue
		}
		for i, v := range vs {
			if v != int64(i+1) {
				t.Errorf("stream %s: out-of-order delivery at %d: got version %d (%v)", s, i, v, vs)
				break
			}
		}
	}
}

func TestOutbox_LeaseExpiry_Redelivery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	t.Parallel()

	ctx := context.Background()
	client := createMongoDBContainer(t, ctx)

	started := make(chan struct{})
	release := make(chan struct{})
	var deliveries int32
	// The first delivery simulates a crashed/stuck worker: it signals, then blocks holding the lease.
	// Later deliveries (after the lease expires) record normally.
	handler := func(_ context.Context, item *mongooutbox.Item) error {
		n := atomic.AddInt32(&deliveries, 1)
		if n == 1 {
			close(started)
			<-release
			return nil
		}
		return nil
	}

	leaseOpts := []mongooutbox.Option{mongooutbox.WithLeaseDuration(300 * time.Millisecond)}
	h := newHarness(t, ctx, client, handler, leaseOpts)

	streamID := typeid.NewV4("user")
	if err := h.store.AppendStream(ctx, streamID, writableEvents("evt", 1), es.AppendStreamOptions{}); err != nil {
		t.Fatalf("appending: %v", err)
	}

	// Worker A: claims the stream and blocks in the handler, holding the lease.
	go func() { _ = h.outbox.ProcessNext(ctx) }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never started")
	}

	// Wait for the lease to expire, then a second worker should re-deliver the same item.
	time.Sleep(500 * time.Millisecond)
	workerB, err := mongooutbox.New(h.outboxColl, h.streamColl, handler, leaseOpts...)
	if err != nil {
		t.Fatalf("creating worker B: %v", err)
	}
	if err := workerB.ProcessNext(ctx); err != nil {
		t.Fatalf("worker B ProcessNext: %v", err)
	}

	if got := atomic.LoadInt32(&deliveries); got < 2 {
		t.Errorf("expected at-least-once redelivery (>=2 deliveries), got %d", got)
	}

	close(release)
}

func TestOutbox_RetryAndFail_HaltsStream(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	t.Parallel()

	ctx := context.Background()
	client := createMongoDBContainer(t, ctx)

	var deliveredVersions []int64
	var mu sync.Mutex
	handler := func(_ context.Context, item *mongooutbox.Item) error {
		mu.Lock()
		deliveredVersions = append(deliveredVersions, item.StreamVersion)
		mu.Unlock()
		// Always fail on version 1 so the stream halts there.
		if item.StreamVersion == 1 {
			return errors.New("always fails")
		}
		return nil
	}

	h := newHarness(t, ctx, client, handler, []mongooutbox.Option{mongooutbox.WithMaxRetries(2)})

	streamID := typeid.NewV4("user")
	if err := h.store.AppendStream(ctx, streamID, writableEvents("evt", 2), es.AppendStreamOptions{}); err != nil {
		t.Fatalf("appending: %v", err)
	}

	// Drain several times: each pass retries version 1 until it permanently fails.
	for range 5 {
		drainProcessNext(t, ctx, h.outbox)
	}

	// Version 2 must never be delivered (the failed head halts the stream).
	mu.Lock()
	for _, v := range deliveredVersions {
		if v == 2 {
			mu.Unlock()
			t.Fatalf("version 2 was delivered despite a halted stream: %v", deliveredVersions)
		}
	}
	mu.Unlock()

	// Both items remain (delete-on-ack only happens on success; failed items are retained).
	if got := h.countOutbox(t, ctx); got != 2 {
		t.Errorf("expected 2 retained outbox items, got %d", got)
	}
}

func TestOutbox_EnsureIndexes_Idempotent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	t.Parallel()

	ctx := context.Background()
	client := createMongoDBContainer(t, ctx)

	var mu sync.Mutex
	var got []*mongooutbox.Item
	h := newHarness(t, ctx, client, collectingHandler(&mu, &got), nil)

	for range 3 {
		if err := h.outbox.EnsureIndexes(ctx); err != nil {
			t.Fatalf("EnsureIndexes: %v", err)
		}
	}
}
