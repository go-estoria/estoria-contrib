package eventstore_test

import (
	"errors"
	"fmt"
	"testing"

	pgeventstore "github.com/go-estoria/estoria-contrib/postgres/eventstore"
	"github.com/go-estoria/estoria-contrib/postgres/eventstore/strategy"
	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
	"github.com/gofrs/uuid/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// legacyBigserialSchema is the schema this strategy shipped before the
// position allocator, verbatim: global positions were bigserial values
// allocated at insert. The migration tests must start from this exact
// artifact rather than anything derived from current code.
func legacyBigserialSchema(events, streams string) string {
	return fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id                bigserial    PRIMARY KEY,
			stream_id         uuid         NOT NULL,
			stream_type       varchar(255) NOT NULL,
			event_id          uuid         NOT NULL,
			event_type        varchar(255) NOT NULL,
			stream_offset     bigint       NOT NULL,
			timestamp         timestamptz  NOT NULL,
			data              jsonb,
			data_content_type text         NOT NULL DEFAULT '',
			metadata          jsonb,

			CONSTRAINT %s UNIQUE (stream_id, stream_type, stream_offset),

			CHECK (stream_offset > 0)
		);

		CREATE TABLE IF NOT EXISTS %s (
			stream_type   varchar(255) NOT NULL,
			stream_id     uuid         NOT NULL,
			last_offset   bigint       NOT NULL DEFAULT 0,

			PRIMARY KEY (stream_type, stream_id)
		);
	`,
		pgx.Identifier{events}.Sanitize(),
		pgx.Identifier{events + "_stream_offset_unique"}.Sanitize(),
		pgx.Identifier{streams}.Sanitize(),
	)
}

// TestSchema_MigratesLegacyBigserial pins the migration from the bigserial
// schema: the allocator must seed at or above every position ever observable
// or allocated — the greater of MAX(id) and the sequence high-water — the
// migration must be reapplication-safe without reseeding or exclusive locks,
// and old-style writers relying on the dropped default must fail closed.
func TestSchema_MigratesLegacyBigserial(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Parallel()

	db, err := createPostgresContainer(t)
	if err != nil {
		t.Fatalf("failed to create Postgres container: %v", err)
	}

	newEvent := []*eventstore.WritableEvent{{Type: "lgevent", Data: []byte(`{}`)}}

	// insertLegacy writes an event row the way the old writer did: id from
	// the bigserial default, advancing the sequence.
	insertLegacy := func(t *testing.T, events string, stream typeid.ID, offset int) {
		t.Helper()

		if _, err := db.Exec(t.Context(), fmt.Sprintf(`
			INSERT INTO %s (stream_id, stream_type, event_id, event_type, stream_offset, timestamp, data, data_content_type)
			VALUES ($1, $2, $3, $4, $5, now(), '{}'::jsonb, '')`,
			pgx.Identifier{events}.Sanitize(),
		), stream.UUID, stream.Type, uuid.Must(uuid.NewV4()), "lgevent", offset); err != nil {
			t.Fatalf("inserting legacy event at offset %d: %v", offset, err)
		}
	}

	setStreamOffset := func(t *testing.T, streams string, stream typeid.ID, offset int) {
		t.Helper()

		if _, err := db.Exec(t.Context(), fmt.Sprintf(`
			INSERT INTO %s (stream_type, stream_id, last_offset) VALUES ($1, $2, $3)
			ON CONFLICT (stream_type, stream_id) DO UPDATE SET last_offset = $3`,
			pgx.Identifier{streams}.Sanitize(),
		), stream.Type, stream.UUID, offset); err != nil {
			t.Fatalf("setting stream offset: %v", err)
		}
	}

	t.Run("seeds above a sequence that outran MAX(id)", func(t *testing.T) {
		const (
			events  = "event_lga"
			streams = "stream_lga"
		)

		if _, err := db.Exec(t.Context(), legacyBigserialSchema(events, streams)); err != nil {
			t.Fatalf("creating legacy schema: %v", err)
		}

		stream := typeid.NewV4("lgstream")
		for offset := 1; offset <= 5; offset++ {
			insertLegacy(t, events, stream, offset)
		}
		setStreamOffset(t, streams, stream, 5)

		// A StreamDeleter-style removal of the highest rows: consumers can
		// hold checkpoints at 5 while MAX(id) drops to 3. Seeding from
		// MAX(id) alone would reuse positions 4 and 5 below them.
		if _, err := db.Exec(t.Context(), fmt.Sprintf(
			`DELETE FROM %s WHERE id IN (4, 5)`, pgx.Identifier{events}.Sanitize(),
		)); err != nil {
			t.Fatalf("deleting high rows: %v", err)
		}

		strat := must(strategy.NewDefaultStrategy(
			strategy.WithEventsTableName(events),
			strategy.WithStreamsTableName(streams),
		))

		if _, err := db.Exec(t.Context(), strat.Schema()); err != nil {
			t.Fatalf("migrating schema: %v", err)
		}

		store := must(pgeventstore.New(db, pgeventstore.WithStrategy(strat)))

		written, err := store.AppendStream(t.Context(), stream, newEvent, eventstore.AppendStreamOptions{})
		if err != nil {
			t.Fatalf("appending after migration: %v", err)
		}

		if got := *written[0].GlobalPosition; got != 6 {
			t.Fatalf("want the first post-migration position above the sequence high-water at 6, got %d", got)
		}

		// A consumer checkpointed at the old high-water misses nothing.
		resumed := readAllEventsAfter(t, store, 5)
		if len(resumed) != 1 || resumed[0].ID != written[0].ID {
			t.Fatalf("want a resume above the old high-water to yield exactly the new event, got %d events", len(resumed))
		}

		// Reapplication is idempotent: no reseed and no exclusive lock.
		// Deleting the newest event first drops both database-derived floors —
		// MAX(id) to 3 and the sequence high-water at 5 — below the allocator
		// at 6, so any reseed moves the allocator and fails the assertions
		// below. Reapplying while another session holds ACCESS SHARE on the
		// events table, under a short lock timeout, proves no ACCESS EXCLUSIVE
		// is taken: the guarded migration skips the default drop, where an
		// unconditional ALTER would block and time out.
		if _, err := db.Exec(t.Context(), fmt.Sprintf(
			`DELETE FROM %s WHERE id = 6`, pgx.Identifier{events}.Sanitize(),
		)); err != nil {
			t.Fatalf("deleting the newest event: %v", err)
		}

		lockTx, err := db.Begin(t.Context())
		if err != nil {
			t.Fatalf("beginning the lock-holding transaction: %v", err)
		}
		defer func() { _ = lockTx.Rollback(t.Context()) }()

		if _, err := lockTx.Exec(t.Context(), fmt.Sprintf(
			`LOCK TABLE %s IN ACCESS SHARE MODE`, pgx.Identifier{events}.Sanitize(),
		)); err != nil {
			t.Fatalf("taking ACCESS SHARE on the events table: %v", err)
		}

		reapplyTx, err := db.Begin(t.Context())
		if err != nil {
			t.Fatalf("beginning the reapplication transaction: %v", err)
		}
		defer func() { _ = reapplyTx.Rollback(t.Context()) }()

		if _, err := reapplyTx.Exec(t.Context(), `SET LOCAL lock_timeout = '1s'`); err != nil {
			t.Fatalf("setting lock timeout: %v", err)
		}

		if _, err := reapplyTx.Exec(t.Context(), strat.Schema()); err != nil {
			t.Fatalf("want reapplication to take no exclusive lock while a reader holds ACCESS SHARE, got: %v", err)
		}

		if err := reapplyTx.Commit(t.Context()); err != nil {
			t.Fatalf("committing the reapplication: %v", err)
		}

		if err := lockTx.Rollback(t.Context()); err != nil {
			t.Fatalf("releasing the ACCESS SHARE lock: %v", err)
		}

		var lastPosition int64
		if err := db.QueryRow(t.Context(),
			`SELECT last_position FROM `+pgx.Identifier{events + "_position_allocator"}.Sanitize(),
		).Scan(&lastPosition); err != nil {
			t.Fatalf("reading allocator: %v", err)
		}

		if lastPosition != 6 {
			t.Fatalf("want reapplication to leave the allocator at 6, got %d", lastPosition)
		}

		again, err := store.AppendStream(t.Context(), stream, newEvent, eventstore.AppendStreamOptions{})
		if err != nil {
			t.Fatalf("appending after reapplication: %v", err)
		}

		if got := *again[0].GlobalPosition; got != 7 {
			t.Fatalf("want the next position 7 after reapplication, got %d", got)
		}

		// The bigserial default is gone, and old-style writers fail closed.
		var hasDefault bool
		if err := db.QueryRow(t.Context(),
			`SELECT atthasdef FROM pg_attribute WHERE attrelid = $1::regclass AND attname = 'id'`,
			pgx.Identifier{events}.Sanitize(),
		).Scan(&hasDefault); err != nil {
			t.Fatalf("checking id default: %v", err)
		}

		if hasDefault {
			t.Fatal("want the bigserial default dropped by the migration")
		}

		_, err = db.Exec(t.Context(), fmt.Sprintf(`
			INSERT INTO %s (stream_id, stream_type, event_id, event_type, stream_offset, timestamp, data, data_content_type)
			VALUES ($1, $2, $3, $4, 99, now(), '{}'::jsonb, '')`,
			pgx.Identifier{events}.Sanitize(),
		), stream.UUID, stream.Type, uuid.Must(uuid.NewV4()), "lgevent")

		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "23502" {
			t.Fatalf("want an old-style insert to fail closed with a not-null violation, got %v", err)
		}
	})

	t.Run("seeds above MAX(id) written past the sequence", func(t *testing.T) {
		const (
			events  = "event_lgb"
			streams = "stream_lgb"
		)

		if _, err := db.Exec(t.Context(), legacyBigserialSchema(events, streams)); err != nil {
			t.Fatalf("creating legacy schema: %v", err)
		}

		stream := typeid.NewV4("lgstream")

		// A row written with an explicit id — a bulk import, say — advances
		// MAX(id) without touching the sequence.
		if _, err := db.Exec(t.Context(), fmt.Sprintf(`
			INSERT INTO %s (id, stream_id, stream_type, event_id, event_type, stream_offset, timestamp, data, data_content_type)
			VALUES (100, $1, $2, $3, $4, 1, now(), '{}'::jsonb, '')`,
			pgx.Identifier{events}.Sanitize(),
		), stream.UUID, stream.Type, uuid.Must(uuid.NewV4()), "lgevent"); err != nil {
			t.Fatalf("inserting explicit-id event: %v", err)
		}
		setStreamOffset(t, streams, stream, 1)

		strat := must(strategy.NewDefaultStrategy(
			strategy.WithEventsTableName(events),
			strategy.WithStreamsTableName(streams),
		))

		if _, err := db.Exec(t.Context(), strat.Schema()); err != nil {
			t.Fatalf("migrating schema: %v", err)
		}

		store := must(pgeventstore.New(db, pgeventstore.WithStrategy(strat)))

		written, err := store.AppendStream(t.Context(), stream, newEvent, eventstore.AppendStreamOptions{})
		if err != nil {
			t.Fatalf("appending after migration: %v", err)
		}

		if got := *written[0].GlobalPosition; got != 101 {
			t.Fatalf("want the first post-migration position above MAX(id) at 101, got %d", got)
		}

		resumed := readAllEventsAfter(t, store, 100)
		if len(resumed) != 1 || resumed[0].ID != written[0].ID {
			t.Fatalf("want a resume above MAX(id) to yield exactly the new event, got %d events", len(resumed))
		}
	})
}
