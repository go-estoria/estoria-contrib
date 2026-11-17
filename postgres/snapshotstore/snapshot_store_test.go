package snapshotstore_test

import (
	"strings"
	"testing"

	"github.com/go-estoria/estoria"
	pgsnapshotstore "github.com/go-estoria/estoria-contrib/postgres/snapshotstore"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestNew_RequiresPool(t *testing.T) {
	t.Parallel()

	if _, err := pgsnapshotstore.New(nil); err == nil {
		t.Error("want an error creating a SnapshotStore without a pool, got nil")
	}
}

// The options are applied directly rather than through New, whose nil-pool check
// would otherwise mask which validation produced the error.
func TestWithTableName_ValidatesIdentifiers(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		wantErr bool
	}{
		{name: "custom_snapshot", wantErr: false},
		{name: "_snapshot1", wantErr: false},
		{name: strings.Repeat("s", 63), wantErr: false},
		{name: strings.Repeat("s", 64), wantErr: true},
		{name: "", wantErr: true},
		{name: "1snapshot", wantErr: true},
		{name: "snapshot;drop table event", wantErr: true},
		{name: "snapshot name", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := pgsnapshotstore.WithTableName(tc.name)(&pgsnapshotstore.SnapshotStore{})
			if gotErr := err != nil; gotErr != tc.wantErr {
				t.Errorf("want error %t for table name %q, got %v", tc.wantErr, tc.name, err)
			}
		})
	}
}

func TestWithMaxSnapshots_RequiresAtLeastOne(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		n       int64
		wantErr bool
	}{
		{n: 1, wantErr: false},
		{n: 2, wantErr: false},
		{n: 0, wantErr: true},
		{n: -1, wantErr: true},
	} {
		err := pgsnapshotstore.WithMaxSnapshots(tc.n)(&pgsnapshotstore.SnapshotStore{})
		if gotErr := err != nil; gotErr != tc.wantErr {
			t.Errorf("want error %t for max snapshots %d, got %v", tc.wantErr, tc.n, err)
		}
	}
}

func TestWithLogger_RequiresLogger(t *testing.T) {
	t.Parallel()

	if err := pgsnapshotstore.WithLogger(nil)(&pgsnapshotstore.SnapshotStore{}); err == nil {
		t.Error("want an error for a nil logger, got nil")
	}

	if err := pgsnapshotstore.WithLogger(estoria.GetLogger())(&pgsnapshotstore.SnapshotStore{}); err != nil {
		t.Errorf("want no error for a logger, got %v", err)
	}
}

// pgxpool connects lazily, so a pool for an unreachable address is enough to
// construct a store whose Schema is only rendered, never executed.
func TestSchema_NamesTheConfiguredTable(t *testing.T) {
	t.Parallel()

	pool, err := pgxpool.New(t.Context(), "postgres://user:password@127.0.0.1:1/none")
	if err != nil {
		t.Fatalf("creating pool: %v", err)
	}

	t.Cleanup(pool.Close)

	for _, tc := range []struct {
		name      string
		opts      []pgsnapshotstore.Option
		wantTable string
	}{
		{name: "default", wantTable: `"aggregate_snapshot"`},
		{name: "custom", opts: []pgsnapshotstore.Option{pgsnapshotstore.WithTableName("custom_snapshot")}, wantTable: `"custom_snapshot"`},
	} {
		store, err := pgsnapshotstore.New(pool, tc.opts...)
		if err != nil {
			t.Fatalf("%s: creating store: %v", tc.name, err)
		}

		if want := "CREATE TABLE IF NOT EXISTS " + tc.wantTable + " ("; !strings.Contains(store.Schema(), want) {
			t.Errorf("%s: want schema to contain %q, got:\n%s", tc.name, want, store.Schema())
		}
	}
}
