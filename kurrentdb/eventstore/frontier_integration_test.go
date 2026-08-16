package eventstore_test

import (
	"errors"
	"io"
	"testing"
	"time"

	"github.com/go-estoria/estoria-contrib/kurrentdb/eventstore"
	coreeventstore "github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
	"github.com/kurrent-io/KurrentDB-Client-Go/kurrentdb"
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

	headPosition := func(t *testing.T) uint64 {
		t.Helper()

		read, err := client.ReadAll(t.Context(), kurrentdb.ReadAllOptions{
			Direction: kurrentdb.Backwards,
			From:      kurrentdb.End{},
		}, 1)
		if err != nil {
			t.Fatalf("reading the $all head: %v", err)
		}
		defer read.Close()

		resolved, err := read.Recv()
		if errors.Is(err, io.EOF) {
			return 0
		} else if err != nil {
			t.Fatalf("receiving the $all head record: %v", err)
		}

		if resolved.Commit == nil {
			t.Fatal("$all head record has no commit position")
		}

		return *resolved.Commit
	}

	// Startup writes its own system records; the head must hold still across
	// consecutive samples before the test's appends can be known to be the tip.
	deadline := time.Now().Add(30 * time.Second)
	last := headPosition(t)
	for stable := 0; stable < 4; {
		if time.Now().After(deadline) {
			t.Fatal("node never quiesced: the $all head kept moving")
		}

		time.Sleep(250 * time.Millisecond)

		if next := headPosition(t); next == last {
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
