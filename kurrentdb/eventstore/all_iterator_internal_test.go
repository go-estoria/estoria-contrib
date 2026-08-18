package eventstore

import (
	"strings"
	"testing"
	"time"

	"github.com/go-estoria/estoria/typeid"
	guuid "github.com/google/uuid"
	"github.com/kurrent-io/KurrentDB-Client-Go/kurrentdb"
)

// rawRecord builds a $all record as the server would deliver it: the top-level commit
// position and the event's own (commit, prepare) log position.
func rawRecord(stream string, commit, prepare uint64) *kurrentdb.ResolvedEvent {
	return &kurrentdb.ResolvedEvent{
		Event: &kurrentdb.RecordedEvent{
			EventID:     guuid.New(),
			EventType:   "itevent",
			StreamID:    stream,
			Position:    kurrentdb.Position{Commit: commit, Prepare: prepare},
			CreatedDate: time.Unix(0, 0).UTC(),
			Data:        []byte(`{}`),
		},
		Commit: &commit,
	}
}

func consumeIterator(owned bool) *allStreamIterator {
	return &allStreamIterator{
		owns: func(string) (typeid.ID, bool) {
			return typeid.NewV4("itstream"), owned
		},
		windowSize:    64,
		bound:         -1,
		frontier:      1 << 40,
		cursor:        -1,
		cursorPrepare: -1,
		verified:      true,
		remaining:     -1,
	}
}

// TestAllIterator_Consume pins the per-record rules the transport loop cannot vary: the
// (commit, prepare) cursor order that legacy transaction groups require, the
// fail-closed guard on owned legacy records, the frontier stop, and the resume bound.
func TestAllIterator_Consume(t *testing.T) {
	t.Run("yields a modern owned record and advances the cursor", func(t *testing.T) {
		it := consumeIterator(true)

		event, result, err := it.consume(rawRecord("owned", 100, 100))
		if err != nil || result != consumeYield {
			t.Fatalf("want a yield, got result %v, err %v", result, err)
		}

		if got := *event.GlobalPosition; got != 100 {
			t.Errorf("want global position 100, got %d", got)
		}

		if it.cursor != 100 || it.cursorPrepare != 100 {
			t.Errorf("want the cursor advanced to (100, 100), got (%d, %d)", it.cursor, it.cursorPrepare)
		}
	})

	t.Run("skips the inclusive reopen duplicate", func(t *testing.T) {
		it := consumeIterator(true)
		it.cursor, it.cursorPrepare = 100, 100

		if _, result, err := it.consume(rawRecord("owned", 100, 100)); err != nil || result != consumeSkip {
			t.Fatalf("want the record at the cursor skipped, got result %v, err %v", result, err)
		}
	})

	t.Run("fails closed on an owned legacy-transaction record", func(t *testing.T) {
		it := consumeIterator(true)

		_, _, err := it.consume(rawRecord("owned", 200, 150))
		if err == nil || !strings.Contains(err.Error(), "legacy explicit transaction") {
			t.Fatalf("want the owned legacy record refused, got %v", err)
		}
	})

	t.Run("does not mistake a same-commit later-prepare record for already seen", func(t *testing.T) {
		// The cursor sits on a legacy group's earlier member. A commit-only cursor
		// would skip the rest of the group unseen; the (commit, prepare) order must
		// deliver the next member to the legacy guard instead.
		it := consumeIterator(true)
		it.cursor, it.cursorPrepare = 100, 90

		_, result, err := it.consume(rawRecord("owned", 100, 95))
		if err == nil || !strings.Contains(err.Error(), "legacy explicit transaction") {
			t.Fatalf("want the group's later member to reach the legacy guard, got result %v, err %v", result, err)
		}
	})

	t.Run("skips a foreign legacy-transaction record but advances past it", func(t *testing.T) {
		it := consumeIterator(false)

		if _, result, err := it.consume(rawRecord("foreign", 300, 250)); err != nil || result != consumeSkip {
			t.Fatalf("want the foreign record skipped without error, got result %v, err %v", result, err)
		}

		if it.cursor != 300 || it.cursorPrepare != 250 {
			t.Errorf("want the cursor advanced to (300, 250), got (%d, %d)", it.cursor, it.cursorPrepare)
		}
	})

	t.Run("ends the read terminally past the frontier", func(t *testing.T) {
		it := consumeIterator(true)
		it.frontier = 500

		if _, result, err := it.consume(rawRecord("owned", 501, 501)); err != nil || result != consumeEndOfRead {
			t.Fatalf("want the read complete past the frontier, got result %v, err %v", result, err)
		}
	})

	t.Run("skips owned records at or below the resume bound", func(t *testing.T) {
		it := consumeIterator(true)
		it.bound = 400

		if _, result, err := it.consume(rawRecord("owned", 350, 350)); err != nil || result != consumeSkip {
			t.Fatalf("want the below-bound record skipped, got result %v, err %v", result, err)
		}

		if it.cursor != 350 {
			t.Errorf("want the cursor advanced to 350, got %d", it.cursor)
		}
	})

	t.Run("rejects a record with no commit position", func(t *testing.T) {
		it := consumeIterator(true)
		record := rawRecord("owned", 100, 100)
		record.Commit = nil

		if _, _, err := it.consume(record); err == nil {
			t.Fatal("want a record with no commit position rejected")
		}
	})

	t.Run("rejects a commit position overflowing int64", func(t *testing.T) {
		it := consumeIterator(true)

		if _, _, err := it.consume(rawRecord("owned", 1<<63, 1<<63)); err == nil {
			t.Fatal("want an overflowing commit position rejected")
		}
	})
}

// TestAllIterator_WindowOptions pins the reopen position: a window resumes on the exact
// (commit, prepare) of the last record seen. Reopening from (cursor, cursor) instead
// would ask the server to skip a legacy transaction group's unseen later members —
// server-side, where no client guard can fire — and modern integration data, whose
// halves are always equal, cannot tell the difference.
func TestAllIterator_WindowOptions(t *testing.T) {
	t.Run("resumes on the exact (commit, prepare) of the last record seen", func(t *testing.T) {
		it := consumeIterator(false)
		it.cursor, it.cursorPrepare = 100, 90

		opts := it.windowOptions()
		position, ok := opts.From.(kurrentdb.Position)
		if !ok {
			t.Fatalf("want a position resume, got %T", opts.From)
		}

		if position.Commit != 100 || position.Prepare != 90 {
			t.Errorf("want the window reopened from (100, 90), got (%d, %d)", position.Commit, position.Prepare)
		}
	})

	t.Run("reads from the start before any record is seen", func(t *testing.T) {
		it := consumeIterator(false)

		if _, ok := it.windowOptions().From.(kurrentdb.Start); !ok {
			t.Errorf("want the first window to read from the start of $all, got %T", it.windowOptions().From)
		}
	})
}

// TestWithReadAllWindowSize_RejectsStarvingSizes pins the window-size floor: a window
// of one can never make progress, because every reopened window returns only the
// inclusive cursor record, which is skipped, and the full window reopens forever.
func TestWithReadAllWindowSize_RejectsStarvingSizes(t *testing.T) {
	for _, size := range []int64{-1, 0, 1} {
		if _, err := New(nil, WithReadAllWindowSize(size)); err == nil || !strings.Contains(err.Error(), "at least 2") {
			t.Errorf("want window size %d rejected, got %v", size, err)
		}
	}

	if _, err := New(nil, WithReadAllWindowSize(2)); err != nil {
		t.Errorf("want the minimum window size accepted, got %v", err)
	}
}
