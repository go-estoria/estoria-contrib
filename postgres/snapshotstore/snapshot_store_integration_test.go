package snapshotstore_test

import (
	"bytes"
	"errors"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-estoria/estoria"
	pgeventstore "github.com/go-estoria/estoria-contrib/postgres/eventstore"
	"github.com/go-estoria/estoria-contrib/postgres/eventstore/strategy"
	pgsnapshotstore "github.com/go-estoria/estoria-contrib/postgres/snapshotstore"
	"github.com/go-estoria/estoria/aggregatestore"
	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/snapshotstore"
	"github.com/go-estoria/estoria/typeid"
	"github.com/gofrs/uuid/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestSnapshotStore_Integration_CustomTable(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Parallel()

	pool, err := createPostgresContainer(t)
	if err != nil {
		t.Fatalf("failed to create Postgres container: %v", err)
	}

	store := newStore(t, pool, pgsnapshotstore.WithTableName("custom_snapshot"))

	aggregateID := typeid.NewV4("account")
	writeVersions(t, store, aggregateID, 1)

	if got := storedVersions(t, pool, "custom_snapshot", aggregateID); !slices.Equal(got, []int64{1}) {
		t.Errorf("want versions [1] in the custom table, got %v", got)
	}

	var defaultTable *string
	if err := pool.QueryRow(t.Context(), `SELECT to_regclass('aggregate_snapshot')::text`).Scan(&defaultTable); err != nil {
		t.Fatalf("looking up the default table: %v", err)
	}

	if defaultTable != nil {
		t.Errorf("want no default table, got %s", *defaultTable)
	}
}

func TestSnapshotStore_Integration_ReadsAndWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Parallel()

	pool, err := createPostgresContainer(t)
	if err != nil {
		t.Fatalf("failed to create Postgres container: %v", err)
	}

	store := newStore(t, pool)

	t.Run("a bounded read returns the newest snapshot at or below the bound", func(t *testing.T) {
		aggregateID := typeid.NewV4("account")
		writeVersions(t, store, aggregateID, 1, 2, 3)

		for _, tc := range []struct {
			maxVersion  int64
			wantVersion int64
		}{
			{maxVersion: 1, wantVersion: 1},
			{maxVersion: 2, wantVersion: 2},
			{maxVersion: 3, wantVersion: 3},
			{maxVersion: 10, wantVersion: 3},
			{maxVersion: 0, wantVersion: 3},
			{maxVersion: -1, wantVersion: 3},
		} {
			snap, err := store.ReadSnapshot(t.Context(), aggregateID, snapshotstore.ReadSnapshotOptions{MaxVersion: tc.maxVersion})
			if err != nil {
				t.Errorf("MaxVersion %d: reading snapshot: %v", tc.maxVersion, err)
				continue
			}

			if snap.AggregateVersion != tc.wantVersion {
				t.Errorf("MaxVersion %d: want version %d, got %d", tc.maxVersion, tc.wantVersion, snap.AggregateVersion)
			}
		}
	})

	t.Run("reads order by version, not by write order", func(t *testing.T) {
		aggregateID := typeid.NewV4("account")
		writeVersions(t, store, aggregateID, 3, 1)

		snap, err := store.ReadSnapshot(t.Context(), aggregateID, snapshotstore.ReadSnapshotOptions{})
		if err != nil {
			t.Fatalf("reading snapshot: %v", err)
		}

		if snap.AggregateVersion != 3 {
			t.Errorf("want version 3, got %d", snap.AggregateVersion)
		}
	})

	t.Run("rewriting a version replaces it", func(t *testing.T) {
		aggregateID := typeid.NewV4("account")

		wantTimestamp := time.Date(2026, time.August, 6, 12, 0, 0, 0, time.UTC)
		for _, snap := range []*snapshotstore.AggregateSnapshot{
			{
				AggregateID:      aggregateID,
				AggregateVersion: 1,
				Timestamp:        time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC),
				Data:             []byte(`{"first":true}`),
				DataContentType:  "application/x-first",
			},
			{
				AggregateID:      aggregateID,
				AggregateVersion: 1,
				Timestamp:        wantTimestamp,
				Data:             []byte(`{"second":true}`),
				DataContentType:  "application/x-second",
			},
		} {
			if err := store.WriteSnapshot(t.Context(), snap); err != nil {
				t.Fatalf("writing snapshot: %v", err)
			}
		}

		got, err := store.ReadSnapshot(t.Context(), aggregateID, snapshotstore.ReadSnapshotOptions{})
		if err != nil {
			t.Fatalf("reading snapshot: %v", err)
		}

		if want := `{"second":true}`; string(got.Data) != want {
			t.Errorf("want data %s, got %s", want, got.Data)
		}

		if want := "application/x-second"; got.DataContentType != want {
			t.Errorf("want data content type %q, got %q", want, got.DataContentType)
		}

		if !got.Timestamp.Equal(wantTimestamp) {
			t.Errorf("want timestamp %s, got %s", wantTimestamp, got.Timestamp)
		}
	})

	t.Run("a zero timestamp is assigned from the database clock", func(t *testing.T) {
		aggregateID := typeid.NewV4("account")
		writeVersions(t, store, aggregateID, 1)

		got, err := store.ReadSnapshot(t.Context(), aggregateID, snapshotstore.ReadSnapshotOptions{})
		if err != nil {
			t.Fatalf("reading snapshot: %v", err)
		}

		// The database clock is not the test's clock, so this allows for skew.
		if since := time.Since(got.Timestamp); since < -time.Minute || since > time.Minute {
			t.Errorf("want a timestamp within a minute of now, got %s", got.Timestamp)
		}
	})

	t.Run("timestamps are kept to the microsecond and returned in UTC", func(t *testing.T) {
		aggregateID := typeid.NewV4("account")

		if err := store.WriteSnapshot(t.Context(), &snapshotstore.AggregateSnapshot{
			AggregateID:      aggregateID,
			AggregateVersion: 1,
			Timestamp:        time.Date(2026, time.August, 5, 12, 0, 0, 123456789, time.FixedZone("UTC+1", 60*60)),
			Data:             []byte(`{}`),
		}); err != nil {
			t.Fatalf("writing snapshot: %v", err)
		}

		got, err := store.ReadSnapshot(t.Context(), aggregateID, snapshotstore.ReadSnapshotOptions{})
		if err != nil {
			t.Fatalf("reading snapshot: %v", err)
		}

		if want := time.Date(2026, time.August, 5, 11, 0, 0, 123456000, time.UTC); !got.Timestamp.Equal(want) {
			t.Errorf("want timestamp %s, got %s", want, got.Timestamp)
		}

		if got.Timestamp.Location() != time.UTC {
			t.Errorf("want a timestamp in UTC, got location %q", got.Timestamp.Location())
		}
	})

	t.Run("payloads round-trip byte for byte", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			data []byte
			want []byte
		}{
			{name: "binary", data: []byte{0x00, 0xff, 0xfe, '{', 0x00, 0x7f}, want: []byte{0x00, 0xff, 0xfe, '{', 0x00, 0x7f}},
			{name: "JSON that jsonb would rewrite", data: []byte(`{ "b":1,   "a":2, "a":3 }`), want: []byte(`{ "b":1,   "a":2, "a":3 }`)},
			{name: "empty", data: []byte{}, want: []byte{}},
			{name: "nil", data: nil, want: []byte{}},
		} {
			aggregateID := typeid.NewV4("account")

			if err := store.WriteSnapshot(t.Context(), &snapshotstore.AggregateSnapshot{
				AggregateID:      aggregateID,
				AggregateVersion: 1,
				Data:             slices.Clone(tc.data),
			}); err != nil {
				t.Errorf("%s: writing snapshot: %v", tc.name, err)
				continue
			}

			got, err := store.ReadSnapshot(t.Context(), aggregateID, snapshotstore.ReadSnapshotOptions{})
			if err != nil {
				t.Errorf("%s: reading snapshot: %v", tc.name, err)
				continue
			}

			if !bytes.Equal(got.Data, tc.want) {
				t.Errorf("%s: want data %x, got %x", tc.name, tc.want, got.Data)
			}
		}
	})

	t.Run("the largest version round-trips", func(t *testing.T) {
		aggregateID := typeid.NewV4("account")
		writeVersions(t, store, aggregateID, math.MaxInt64)

		got, err := store.ReadSnapshot(t.Context(), aggregateID, snapshotstore.ReadSnapshotOptions{})
		if err != nil {
			t.Fatalf("reading snapshot: %v", err)
		}

		if got.AggregateVersion != math.MaxInt64 {
			t.Errorf("want version %d, got %d", int64(math.MaxInt64), got.AggregateVersion)
		}
	})

	t.Run("invalid snapshots are rejected", func(t *testing.T) {
		id := uuid.Must(uuid.NewV4())

		for _, tc := range []struct {
			name string
			snap *snapshotstore.AggregateSnapshot
		}{
			{name: "nil snapshot", snap: nil},
			{name: "empty aggregate type", snap: &snapshotstore.AggregateSnapshot{AggregateID: typeid.ID{UUID: id}, AggregateVersion: 1}},
			{name: "version 0", snap: &snapshotstore.AggregateSnapshot{AggregateID: typeid.New("account", id), AggregateVersion: 0}},
			{name: "negative version", snap: &snapshotstore.AggregateSnapshot{AggregateID: typeid.New("account", id), AggregateVersion: -1}},
		} {
			err := store.WriteSnapshot(t.Context(), tc.snap)
			if err == nil {
				t.Errorf("%s: want an error, got nil", tc.name)
				continue
			}

			if pgErr := (*pgconn.PgError)(nil); errors.As(err, &pgErr) {
				t.Errorf("%s: want the store to reject the snapshot before the database does, got %v", tc.name, err)
			}
		}

		var rows int
		if err := pool.QueryRow(t.Context(), `SELECT count(*) FROM aggregate_snapshot WHERE aggregate_id = $1`, id).Scan(&rows); err != nil {
			t.Fatalf("counting rows: %v", err)
		}

		if rows != 0 {
			t.Errorf("want no rows for the rejected snapshots, got %d", rows)
		}
	})
}

func TestSnapshotStore_Integration_Retention(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Parallel()

	pool, err := createPostgresContainer(t)
	if err != nil {
		t.Fatalf("failed to create Postgres container: %v", err)
	}

	retainAll := newStore(t, pool)

	t.Run("keeps the newest n snapshots per aggregate", func(t *testing.T) {
		type step struct {
			write int64
			want  []int64
		}

		for _, tc := range []struct {
			maxSnapshots int64
			steps        []step
		}{
			{maxSnapshots: 1, steps: []step{{1, []int64{1}}, {2, []int64{2}}, {3, []int64{3}}}},
			{maxSnapshots: 2, steps: []step{{1, []int64{1}}, {2, []int64{1, 2}}, {3, []int64{2, 3}}, {4, []int64{3, 4}}}},
		} {
			store := newStore(t, pool, pgsnapshotstore.WithMaxSnapshots(tc.maxSnapshots))
			aggregateID := typeid.NewV4("account")

			for _, step := range tc.steps {
				writeVersions(t, store, aggregateID, step.write)

				if got := storedVersions(t, pool, "aggregate_snapshot", aggregateID); !slices.Equal(got, step.want) {
					t.Errorf("max %d, after writing %d: want versions %v, got %v", tc.maxSnapshots, step.write, step.want, got)
				}
			}
		}
	})

	t.Run("a stale write does not displace newer snapshots", func(t *testing.T) {
		store := newStore(t, pool, pgsnapshotstore.WithMaxSnapshots(2))
		aggregateID := typeid.NewV4("account")
		writeVersions(t, store, aggregateID, 4, 5, 3)

		if got, want := storedVersions(t, pool, "aggregate_snapshot", aggregateID), []int64{4, 5}; !slices.Equal(got, want) {
			t.Errorf("want versions %v, got %v", want, got)
		}
	})

	t.Run("pruning leaves another aggregate type with the same UUID alone", func(t *testing.T) {
		store := newStore(t, pool, pgsnapshotstore.WithMaxSnapshots(1))
		aggregateID := typeid.NewV4("account")
		sibling := typeid.New("ledger", aggregateID.UUID)

		writeVersions(t, retainAll, sibling, 1, 2, 3)
		writeVersions(t, store, aggregateID, 1, 2, 3)

		if got, want := storedVersions(t, pool, "aggregate_snapshot", sibling), []int64{1, 2, 3}; !slices.Equal(got, want) {
			t.Errorf("want sibling versions %v, got %v", want, got)
		}

		if got, want := storedVersions(t, pool, "aggregate_snapshot", aggregateID), []int64{3}; !slices.Equal(got, want) {
			t.Errorf("want versions %v, got %v", want, got)
		}
	})

	t.Run("the first write with a limit prunes an existing backlog", func(t *testing.T) {
		store := newStore(t, pool, pgsnapshotstore.WithMaxSnapshots(2))
		aggregateID := typeid.NewV4("account")

		writeVersions(t, retainAll, aggregateID, 1, 2, 3, 4, 5)
		writeVersions(t, store, aggregateID, 6)

		if got, want := storedVersions(t, pool, "aggregate_snapshot", aggregateID), []int64{5, 6}; !slices.Equal(got, want) {
			t.Errorf("want versions %v, got %v", want, got)
		}
	})

	t.Run("concurrent writers leave exactly the newest n snapshots", func(t *testing.T) {
		const (
			writers           = 16
			versionsPerWriter = 50
		)

		logger := newWarnCounter()
		store := newStore(t, pool, pgsnapshotstore.WithMaxSnapshots(2), pgsnapshotstore.WithLogger(logger))
		aggregateID := typeid.NewV4("account")

		var (
			wg          sync.WaitGroup
			writeErrors atomic.Int64
		)

		for writer := range writers {
			wg.Go(func() {
				for i := range versionsPerWriter {
					if err := store.WriteSnapshot(t.Context(), &snapshotstore.AggregateSnapshot{
						AggregateID:      aggregateID,
						AggregateVersion: int64(i*writers + writer + 1),
						Data:             []byte(`{}`),
					}); err != nil {
						writeErrors.Add(1)
					}
				}
			})
		}

		wg.Wait()

		if n := writeErrors.Load(); n != 0 {
			t.Errorf("want no write errors, got %d", n)
		}

		if n := logger.warnings.Load(); n != 0 {
			t.Errorf("want no pruning failures, got %d", n)
		}

		const newest = writers * versionsPerWriter
		if got, want := storedVersions(t, pool, "aggregate_snapshot", aggregateID), []int64{newest - 1, newest}; !slices.Equal(got, want) {
			t.Errorf("want versions %v, got %v", want, got)
		}
	})

	// A trigger that rejects every delete makes pruning fail on demand. The table is
	// the subtest's own, so the trigger cannot reach the others.
	t.Run("a pruning failure is logged, not returned, and the next prune catches up", func(t *testing.T) {
		logger := newWarnCounter()
		store := newStore(t, pool,
			pgsnapshotstore.WithTableName("prune_failure_snapshot"),
			pgsnapshotstore.WithMaxSnapshots(1),
			pgsnapshotstore.WithLogger(logger),
		)

		if _, err := pool.Exec(t.Context(), `
			CREATE FUNCTION reject_delete() RETURNS trigger LANGUAGE plpgsql AS $$
			BEGIN
				RAISE EXCEPTION 'deletes are rejected';
			END
			$$;
			CREATE TRIGGER reject_delete BEFORE DELETE ON prune_failure_snapshot
				FOR EACH ROW EXECUTE FUNCTION reject_delete();
		`); err != nil {
			t.Fatalf("creating trigger: %v", err)
		}

		aggregateID := typeid.NewV4("account")
		writeVersions(t, store, aggregateID, 1, 2, 3)

		if got, want := storedVersions(t, pool, "prune_failure_snapshot", aggregateID), []int64{1, 2, 3}; !slices.Equal(got, want) {
			t.Errorf("with deletes rejected: want versions %v, got %v", want, got)
		}

		if got, want := logger.warnings.Load(), int64(2); got != want {
			t.Errorf("want %d logged pruning failures, got %d", want, got)
		}

		if _, err := pool.Exec(t.Context(), `DROP TRIGGER reject_delete ON prune_failure_snapshot`); err != nil {
			t.Fatalf("dropping trigger: %v", err)
		}

		writeVersions(t, store, aggregateID, 4)

		if got, want := storedVersions(t, pool, "prune_failure_snapshot", aggregateID), []int64{4}; !slices.Equal(got, want) {
			t.Errorf("after deletes are allowed again: want versions %v, got %v", want, got)
		}
	})
}

// account is the state of a minimal aggregate whose version always equals its
// balance, so a load's version and balance cross-check each other.
type account struct {
	Balance int64
}

type deposited struct{}

func (deposited) EventType() string                 { return "deposited" }
func (deposited) New() estoria.DomainEvent[account] { return &deposited{} }
func (deposited) ApplyTo(a account) account         { a.Balance++; return a }

func TestSnapshotStore_Integration_Composition(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Parallel()

	pool, err := createPostgresContainer(t)
	if err != nil {
		t.Fatalf("failed to create Postgres container: %v", err)
	}

	eventStore := newEventStore(t, pool)
	accounts := newAccountStore(t, eventStore, newStore(t, pool))

	t.Run("a stream truncated at a snapshot loads from it", func(t *testing.T) {
		id := saveDeposits(t, accounts, 7)

		if err := eventStore.DeleteStream(t.Context(), typeid.New("account", id), eventstore.DeleteStreamOptions{ToVersion: 6}); err != nil {
			t.Fatalf("truncating stream: %v", err)
		}

		assertLoads(t, accounts, id, 0, 7)
	})

	t.Run("a load to a past version starts from the newest snapshot within it", func(t *testing.T) {
		id := saveDeposits(t, accounts, 7)

		// Without events 1 through 3, every load below has to start from a snapshot,
		// and only the right one replays to the right version.
		if err := eventStore.DeleteStream(t.Context(), typeid.New("account", id), eventstore.DeleteStreamOptions{ToVersion: 3}); err != nil {
			t.Fatalf("truncating stream: %v", err)
		}

		for _, toVersion := range []int64{3, 4, 5, 6, 7} {
			assertLoads(t, accounts, id, toVersion, toVersion)
		}
	})

	t.Run("a deleted stream loads from its snapshots until they are deleted", func(t *testing.T) {
		id := saveDeposits(t, accounts, 7)

		if err := eventStore.DeleteStream(t.Context(), typeid.New("account", id), eventstore.DeleteStreamOptions{}); err != nil {
			t.Fatalf("deleting stream: %v", err)
		}

		assertLoads(t, accounts, id, 0, 6)

		if _, err := pool.Exec(t.Context(),
			`DELETE FROM aggregate_snapshot WHERE aggregate_type = $1 AND aggregate_id = $2`, "account", id,
		); err != nil {
			t.Fatalf("deleting snapshots: %v", err)
		}

		if _, err := accounts.Load(t.Context(), id, nil); !errors.Is(err, aggregatestore.ErrAggregateNotFound) {
			t.Errorf("want ErrAggregateNotFound once the snapshots are deleted, got %v", err)
		}
	})
}

func newEventStore(t *testing.T, pool *pgxpool.Pool) *pgeventstore.EventStore {
	t.Helper()

	strat, err := strategy.NewDefaultStrategy()
	if err != nil {
		t.Fatalf("tc setup: failed to create strategy: %v", err)
	}

	if _, err := pool.Exec(t.Context(), strat.Schema()); err != nil {
		t.Fatalf("tc setup: failed to create event tables: %v", err)
	}

	eventStore, err := pgeventstore.New(pool, pgeventstore.WithStrategy(strat))
	if err != nil {
		t.Fatalf("tc setup: failed to create EventStore: %v", err)
	}

	return eventStore
}

// newAccountStore snapshots accounts every third version.
func newAccountStore(t *testing.T, eventStore eventstore.Store, snapshots snapshotstore.SnapshotStore) aggregatestore.Store[account] {
	t.Helper()

	inner, err := aggregatestore.New(eventStore, "account",
		func(uuid.UUID) account { return account{} },
		aggregatestore.WithEventTypes[account](deposited{}),
	)
	if err != nil {
		t.Fatalf("tc setup: failed to create aggregate store: %v", err)
	}

	store, err := aggregatestore.NewSnapshottingStore[account](inner, snapshots, snapshotstore.EventCountSnapshotPolicy{N: 3})
	if err != nil {
		t.Fatalf("tc setup: failed to create snapshotting store: %v", err)
	}

	return store
}

// saveDeposits creates an account and saves n deposits one at a time, so the
// snapshot policy sees every version.
func saveDeposits(t *testing.T, accounts aggregatestore.Store[account], n int) uuid.UUID {
	t.Helper()

	id := uuid.Must(uuid.NewV4())
	aggregate := accounts.New(id)

	for range n {
		aggregate.Append(deposited{})

		if err := accounts.Save(t.Context(), aggregate, nil); err != nil {
			t.Fatalf("saving deposit: %v", err)
		}
	}

	return id
}

// assertLoads loads the account to toVersion, or to its latest version when
// toVersion is 0, and checks that it arrives at wantVersion.
func assertLoads(t *testing.T, accounts aggregatestore.Store[account], id uuid.UUID, toVersion, wantVersion int64) {
	t.Helper()

	aggregate, err := accounts.Load(t.Context(), id, &aggregatestore.LoadOptions{ToVersion: toVersion})
	if err != nil {
		t.Errorf("ToVersion %d: loading account: %v", toVersion, err)
		return
	}

	if aggregate.Version() != wantVersion || aggregate.State().Balance != wantVersion {
		t.Errorf("ToVersion %d: want version and balance %d, got version %d and balance %d",
			toVersion, wantVersion, aggregate.Version(), aggregate.State().Balance)
	}
}
