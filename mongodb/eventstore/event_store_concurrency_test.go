package eventstore_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	es "github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
)

func TestEventStore_Integration_SameStreamRace(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Parallel()

	ctx := context.Background()

	mongoClient, err := createMongoDBContainer(t, ctx)
	if err != nil {
		t.Fatalf("failed to create MongoDB container: %v", err)
	}

	store := newTestStore(t, ctx, mongoClient)

	streamID := typeid.NewV4("racetype")

	// N goroutines all try to append at expected version 0. Exactly one must win.
	const n = 12
	var wg sync.WaitGroup
	results := make([]error, n)
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = store.AppendStream(ctx, streamID, writableEvents("evt", 1), es.AppendStreamOptions{ExpectVersion: es.VersionPtr(0)})
		}(i)
	}
	wg.Wait()

	successes := 0
	for _, err := range results {
		if err == nil {
			successes++
			continue
		}
		var mismatch es.StreamVersionMismatchError
		if !errors.As(err, &mismatch) {
			t.Errorf("expected StreamVersionMismatchError, got %v", err)
		}
	}
	if successes != 1 {
		t.Fatalf("expected exactly 1 success, got %d", successes)
	}

	// The stream must contain exactly one event at version 1.
	iter, err := store.ReadStream(ctx, streamID, es.ReadStreamOptions{})
	if err != nil {
		t.Fatalf("reading: %v", err)
	}
	events := drain(t, ctx, iter)
	if len(events) != 1 || events[0].StreamVersion != 1 {
		t.Fatalf("expected 1 event at version 1, got %d events", len(events))
	}
}

func TestEventStore_Integration_SameStreamSequentialContention(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Parallel()

	ctx := context.Background()

	mongoClient, err := createMongoDBContainer(t, ctx)
	if err != nil {
		t.Fatalf("failed to create MongoDB container: %v", err)
	}

	store := newTestStore(t, ctx, mongoClient)

	streamID := typeid.NewV4("contendtype")

	// N goroutines append a single event each without a version expectation. All must succeed
	// (no OCC), and the resulting stream must have a contiguous, unique, gap-free offset sequence.
	const n = 20
	var wg sync.WaitGroup
	errCh := make(chan error, n)
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := store.AppendStream(ctx, streamID, writableEvents("evt", 1), es.AppendStreamOptions{}); err != nil {
				errCh <- err
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Errorf("unexpected append error: %v", err)
	}

	iter, err := store.ReadStream(ctx, streamID, es.ReadStreamOptions{})
	if err != nil {
		t.Fatalf("reading: %v", err)
	}
	events := drain(t, ctx, iter)
	if len(events) != n {
		t.Fatalf("expected %d events, got %d", n, len(events))
	}
	for i, e := range events {
		if e.StreamVersion != int64(i+1) {
			t.Fatalf("event %d: expected version %d, got %d", i, i+1, e.StreamVersion)
		}
	}
}

func TestEventStore_Integration_DifferentStreamRace(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Parallel()

	ctx := context.Background()

	mongoClient, err := createMongoDBContainer(t, ctx)
	if err != nil {
		t.Fatalf("failed to create MongoDB container: %v", err)
	}

	store := newTestStore(t, ctx, mongoClient)

	// N goroutines each append to their own stream concurrently. All succeed; the global offsets
	// across the whole store must be unique, dense, and gap-free (the commit-order property).
	const n = 30
	var wg sync.WaitGroup
	errCh := make(chan error, n)
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			streamID := typeid.NewV4("multitype")
			if err := store.AppendStream(ctx, streamID, writableEvents("evt", 1), es.AppendStreamOptions{}); err != nil {
				errCh <- err
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Errorf("unexpected append error: %v", err)
	}

	iter, err := store.ReadAll(ctx, es.ReadStreamOptions{})
	if err != nil {
		t.Fatalf("reading all: %v", err)
	}
	events := drain(t, ctx, iter)
	if len(events) != n {
		t.Fatalf("expected %d events, got %d", n, len(events))
	}
	seen := map[int64]bool{}
	for i, e := range events {
		if e.GlobalPosition == nil {
			t.Fatalf("event %d has nil global position", i)
		}
		g := *e.GlobalPosition
		if g != int64(i+1) {
			t.Errorf("event %d: expected global position %d, got %d", i, i+1, g)
		}
		if seen[g] {
			t.Errorf("duplicate global position %d", g)
		}
		seen[g] = true
	}
}

func TestEventStore_Integration_StreamMustNotExistRace(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Parallel()

	ctx := context.Background()

	mongoClient, err := createMongoDBContainer(t, ctx)
	if err != nil {
		t.Fatalf("failed to create MongoDB container: %v", err)
	}

	store := newTestStore(t, ctx, mongoClient)

	streamID := typeid.NewV4("createtype")

	const n = 12
	var wg sync.WaitGroup
	results := make([]error, n)
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = store.AppendStream(ctx, streamID, writableEvents("evt", 1), es.AppendStreamOptions{StreamMustNotExist: true})
		}(i)
	}
	wg.Wait()

	successes := 0
	for _, err := range results {
		if err == nil {
			successes++
			continue
		}
		var mismatch es.StreamVersionMismatchError
		if !errors.As(err, &mismatch) {
			t.Errorf("expected StreamVersionMismatchError, got %v", err)
		}
	}
	if successes != 1 {
		t.Fatalf("expected exactly 1 creator to win, got %d", successes)
	}
}
