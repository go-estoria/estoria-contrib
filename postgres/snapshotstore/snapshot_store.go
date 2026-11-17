// Package snapshotstore provides a Postgres-backed aggregate snapshot store.
package snapshotstore

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/go-estoria/estoria"
	"github.com/go-estoria/estoria/snapshotstore"
	"github.com/go-estoria/estoria/typeid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// SnapshotStore persists aggregate snapshots in a Postgres table, one row per
// retained snapshot, keyed by aggregate type, aggregate ID and version. Payloads
// are returned byte for byte. Timestamps are kept to the microsecond and returned
// in UTC, and a zero Timestamp is assigned from the database clock. Writing a
// version that is already stored replaces it, and writing a version older than
// the newest is accepted without changing what an unbounded read returns.
type SnapshotStore struct {
	pool         *pgxpool.Pool
	tableName    string
	maxSnapshots int64
	log          estoria.Logger
}

var _ snapshotstore.SnapshotStore = (*SnapshotStore)(nil)

// New creates a new snapshot store using the provided pgx connection pool. The
// table Schema describes must exist before the store is used.
func New(pool *pgxpool.Pool, opts ...Option) (*SnapshotStore, error) {
	if pool == nil {
		return nil, errors.New("pool is required")
	}

	s := &SnapshotStore{
		pool:      pool,
		tableName: "aggregate_snapshot",
		log:       estoria.GetLogger().WithGroup("snapshotstore"),
	}

	for _, opt := range opts {
		if err := opt(s); err != nil {
			return nil, fmt.Errorf("applying option: %w", err)
		}
	}

	return s, nil
}

// Schema returns the SQL statement required to create the snapshot table.
func (s *SnapshotStore) Schema() string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    aggregate_type    varchar(255) NOT NULL,
    aggregate_id      uuid         NOT NULL,
    aggregate_version bigint       NOT NULL,
    timestamp         timestamptz  NOT NULL,
    data              bytea        NOT NULL,
    data_content_type text         NOT NULL DEFAULT '',

    PRIMARY KEY (aggregate_type, aggregate_id, aggregate_version),

    CHECK (aggregate_version > 0)
);
`, pgx.Identifier{s.tableName}.Sanitize())
}

// ReadSnapshot returns the aggregate's newest snapshot, or its newest at or below
// opts.MaxVersion when that is positive, or snapshotstore.ErrSnapshotNotFound if
// there is none.
func (s *SnapshotStore) ReadSnapshot(ctx context.Context, aggregateID typeid.ID, opts snapshotstore.ReadSnapshotOptions) (*snapshotstore.AggregateSnapshot, error) {
	maxVersion := opts.MaxVersion
	if maxVersion <= 0 {
		maxVersion = math.MaxInt64
	}

	query := fmt.Sprintf(
		`SELECT aggregate_version, timestamp, data, data_content_type FROM %s
		WHERE aggregate_type = $1 AND aggregate_id = $2 AND aggregate_version <= $3
		ORDER BY aggregate_version DESC
		LIMIT 1`,
		pgx.Identifier{s.tableName}.Sanitize(),
	)

	snap := &snapshotstore.AggregateSnapshot{AggregateID: aggregateID}
	if err := s.pool.QueryRow(ctx, query, aggregateID.Type, aggregateID.UUID, maxVersion).
		Scan(&snap.AggregateVersion, &snap.Timestamp, &snap.Data, &snap.DataContentType); errors.Is(err, pgx.ErrNoRows) {
		return nil, snapshotstore.ErrSnapshotNotFound
	} else if err != nil {
		return nil, fmt.Errorf("querying snapshot: %w", err)
	}

	snap.Timestamp = snap.Timestamp.UTC()

	return snap, nil
}

// WriteSnapshot stores snap, replacing any snapshot of the same aggregate at the
// same version. With WithMaxSnapshots set, it then prunes the aggregate's older
// snapshots; a pruning failure is logged, not returned.
func (s *SnapshotStore) WriteSnapshot(ctx context.Context, snap *snapshotstore.AggregateSnapshot) error {
	switch {
	case snap == nil:
		return errors.New("snapshot is required")
	case snap.AggregateID.Type == "":
		return errors.New("aggregate type is required")
	case snap.AggregateVersion < 1:
		return fmt.Errorf("aggregate version must be at least 1, got %d", snap.AggregateVersion)
	}

	var timestamp any
	if !snap.Timestamp.IsZero() {
		timestamp = snap.Timestamp
	}

	data := snap.Data
	if data == nil {
		data = []byte{}
	}

	query := fmt.Sprintf(
		`INSERT INTO %s (aggregate_type, aggregate_id, aggregate_version, timestamp, data, data_content_type)
		VALUES ($1, $2, $3, COALESCE($4::timestamptz, now()), $5, $6)
		ON CONFLICT (aggregate_type, aggregate_id, aggregate_version)
		DO UPDATE SET timestamp = EXCLUDED.timestamp, data = EXCLUDED.data, data_content_type = EXCLUDED.data_content_type`,
		pgx.Identifier{s.tableName}.Sanitize(),
	)

	if _, err := s.pool.Exec(ctx, query,
		snap.AggregateID.Type, snap.AggregateID.UUID, snap.AggregateVersion, timestamp, data, snap.DataContentType,
	); err != nil {
		return fmt.Errorf("upserting snapshot: %w", err)
	}

	if s.maxSnapshots > 0 {
		if err := s.prune(ctx, snap.AggregateID); err != nil {
			s.log.Warn("failed to prune old snapshots",
				"aggregate_id", snap.AggregateID,
				"max_snapshots", s.maxSnapshots,
				"error", err)
		}
	}

	return nil
}

// prune deletes the aggregate's snapshots older than its newest s.maxSnapshots.
func (s *SnapshotStore) prune(ctx context.Context, aggregateID typeid.ID) error {
	table := pgx.Identifier{s.tableName}.Sanitize()
	query := fmt.Sprintf(
		`DELETE FROM %s
		WHERE aggregate_type = $1 AND aggregate_id = $2 AND aggregate_version <= (
			SELECT aggregate_version FROM %s
			WHERE aggregate_type = $1 AND aggregate_id = $2
			ORDER BY aggregate_version DESC
			OFFSET $3 LIMIT 1
		)`,
		table, table,
	)

	if _, err := s.pool.Exec(ctx, query, aggregateID.Type, aggregateID.UUID, s.maxSnapshots); err != nil {
		return fmt.Errorf("deleting snapshots: %w", err)
	}

	return nil
}
