package eventstore_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-estoria/estoria-contrib/kurrentdb/eventstore"
	coreeventstore "github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
)

// TestReadAll_YieldsTipAtQuiescence pins the frontier's inclusive boundary. At a node
// with no other writers, the $all head IS the store's newest event, and a read opened
// then must still yield it: an exclusive frontier would hide the tip event from every
// read until an unrelated commit moved the head, so a caught-up consumer polling a
// quiet node would never see the newest event. The shared acceptance node cannot pin
// this — its projections' background writes usually sit above the store's tip.
func TestReadAll_YieldsTipAtQuiescence(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Parallel()

	// Background writers off: no projections, and statistics effectively silenced, so
	// once startup settles the only commits are this test's own appends.
	client, err := createKurrentContainerWithEnv(t, map[string]string{
		"KURRENTDB_RUN_PROJECTIONS":            "None",
		"KURRENTDB_START_STANDARD_PROJECTIONS": "false",
		"KURRENTDB_STATS_PERIOD_SEC":           "3600",
	})
	if err != nil {
		t.Fatalf("failed to create KurrentDB container: %v", err)
	}

	// Startup writes its own system records; the head must hold still across
	// consecutive samples before the test's appends can be known to be the tip.
	deadline := time.Now().Add(30 * time.Second)
	last := allHead(t, client)
	for stable := 0; stable < 4; {
		if time.Now().After(deadline) {
			t.Fatal("node never quiesced: the $all head kept moving")
		}

		time.Sleep(250 * time.Millisecond)

		if next := allHead(t, client); next == last {
			stable++
		} else {
			last, stable = next, 0
		}
	}

	store, err := eventstore.New(client)
	if err != nil {
		t.Fatalf("creating event store: %v", err)
	}

	written, err := store.AppendStream(t.Context(), typeid.NewV4("quiescent"), []*coreeventstore.WritableEvent{
		{Type: "qevent", Data: []byte(`{}`)},
		{Type: "qevent", Data: []byte(`{}`)},
		{Type: "qevent", Data: []byte(`{}`)},
	}, coreeventstore.AppendStreamOptions{})
	if err != nil {
		t.Fatalf("appending events: %v", err)
	}

	iter, err := store.ReadAll(t.Context(), coreeventstore.ReadAllOptions{})
	if err != nil {
		t.Fatalf("reading all events: %v", err)
	}
	defer iter.Close(t.Context())

	// The boundary is exercised only if the read's frontier IS the written tip. The
	// head is sampled after ReadAll captured its frontier and can only have moved
	// upward, so head == tip here brackets the capture: any background write would
	// surface as a mismatch and invalidate the run loudly instead of letting an
	// exclusive-boundary bug pass unexercised.
	if head, tip := allHead(t, client), uint64(*written[len(written)-1].GlobalPosition); head != tip {
		t.Fatalf("test invalidated: the $all head %d is not the written tip %d, so the frontier boundary was not exercised", head, tip)
	}

	events, err := coreeventstore.Collect(t.Context(), iter)
	if err != nil {
		t.Fatalf("collecting events: %v", err)
	}

	if len(events) != len(written) {
		t.Fatalf("want all %d events including the one at the $all head, got %d", len(written), len(events))
	}

	if got, want := events[len(events)-1].ID, written[len(written)-1].ID; got != want {
		t.Errorf("want the tip event %s yielded last, got %s", want, got)
	}
}

// TestReadAll_FailsClosedBelowFrontier pins the lag guard: a node that reports the end
// of $all below the frontier captured at ReadAll — a lagging follower after a
// reconnect, or a scavenged head — must fail the read rather than certify a false end
// of stream. A single-node container cannot lag, so the iterator is built directly with
// a frontier above the node's real head.
func TestReadAll_FailsClosedBelowFrontier(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Parallel()

	client, err := createKurrentContainer(t)
	if err != nil {
		t.Fatalf("failed to create KurrentDB container: %v", err)
	}

	iter := eventstore.AllIteratorWithFrontier(client, int64(allHead(t, client))+1_000_000)
	defer func() { _ = iter.Close(t.Context()) }()

	_, err = iter.Next(t.Context())
	switch {
	case err == nil:
		t.Fatal("want the drain to end in an error, got an event from an ownerless iterator")
	case errors.Is(err, coreeventstore.ErrEndOfEventStream):
		t.Fatal("want the read to fail closed below its frontier, got a clean end of stream")
	case !strings.Contains(err.Error(), "below the read's frontier"):
		t.Fatalf("want the below-frontier failure, got %v", err)
	}
}
