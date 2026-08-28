// Package checkpointstore provides a SQLite-backed projection checkpoint store.
package checkpointstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/go-estoria/estoria/projection"
	"github.com/go-estoria/estoria/projection/checkpointstore"
)

// timestampFormat is the canonical format used to serialize checkpoint update
// times, matching the event store's timestamp serialization. Timestamps are
// stored in UTC.
const timestampFormat = time.RFC3339Nano

// CheckpointStore persists projection checkpoints in a SQLite table, one row
// per projection ID. The database may be shared by multiple processes on one
// host; SQLite's file locks serialize writers across all of them, and network
// filesystems — whose locking SQLite cannot rely on — are unsupported.
// Connection configuration is the caller's: WAL journal mode lets checkpoint
// reads run while saves commit, and a busy_timeout turns write-lock contention
// — concurrent processors saving checkpoints, or overlapping deletes during
// projection retirement — into bounded waiting rather than immediate
// SQLITE_BUSY errors. Using WAL with more than one connection requires SQLite
// 3.51.3+ or a build carrying the WAL-reset fix (such as 3.50.7 or 3.44.6);
// older versions can corrupt the database under this workload. Shared-cache
// connections reading uncommitted data are unsupported.
type CheckpointStore struct {
	db        *sql.DB
	tableName string
}

var _ checkpointstore.Store = (*CheckpointStore)(nil)

// New creates a new checkpoint store using the provided database handle.
func New(db *sql.DB, opts ...Option) (*CheckpointStore, error) {
	if db == nil {
		return nil, errors.New("db is required")
	}

	s := &CheckpointStore{
		db:        db,
		tableName: "projection_checkpoint",
	}

	for _, opt := range opts {
		if err := opt(s); err != nil {
			return nil, fmt.Errorf("applying option: %w", err)
		}
	}

	return s, nil
}

// Schema returns the SQL statement required to create the checkpoint table.
func (s *CheckpointStore) Schema() string {
	return fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			projection_name    TEXT    NOT NULL,
			projection_version INTEGER NOT NULL,
			position           INTEGER NOT NULL,
			updated_at         TEXT    NOT NULL,

			PRIMARY KEY (projection_name, projection_version)
		);
	`, quoteIdentifier(s.tableName))
}

// Load returns the projection's checkpoint, or
// checkpointstore.ErrCheckpointNotFound if none has been saved.
func (s *CheckpointStore) Load(ctx context.Context, id projection.ID) (checkpointstore.Checkpoint, error) {
	query := fmt.Sprintf(
		`SELECT position, updated_at FROM %s WHERE projection_name = ? AND projection_version = ?`,
		quoteIdentifier(s.tableName),
	)

	var (
		position  int64
		updatedAt string
	)
	if err := s.db.QueryRowContext(ctx, query, id.Name, id.Version).
		Scan(&position, &updatedAt); errors.Is(err, sql.ErrNoRows) {
		return checkpointstore.Checkpoint{}, checkpointstore.ErrCheckpointNotFound
	} else if err != nil {
		return checkpointstore.Checkpoint{}, fmt.Errorf("querying checkpoint: %w", err)
	}

	ts, err := time.Parse(timestampFormat, updatedAt)
	if err != nil {
		return checkpointstore.Checkpoint{}, fmt.Errorf("parsing checkpoint timestamp: %w", err)
	}

	return checkpointstore.Checkpoint{
		ProjectionID: id,
		Position:     position,
		UpdatedAt:    ts,
	}, nil
}

// Save records position as the projection's checkpoint, assigning updated_at
// even when the position is unchanged.
func (s *CheckpointStore) Save(ctx context.Context, id projection.ID, position int64) error {
	query := fmt.Sprintf(`
		INSERT INTO %s (projection_name, projection_version, position, updated_at)
		VALUES (?, ?, ?, ?)
		ON CONFLICT (projection_name, projection_version)
		DO UPDATE SET position = excluded.position, updated_at = excluded.updated_at
	`, quoteIdentifier(s.tableName))

	updatedAt := time.Now().UTC().Format(timestampFormat)
	if _, err := s.db.ExecContext(ctx, query, id.Name, id.Version, position, updatedAt); err != nil {
		return fmt.Errorf("upserting checkpoint: %w", err)
	}

	return nil
}

// Delete removes the projection's checkpoint, or reports
// checkpointstore.ErrCheckpointNotFound if none exists.
func (s *CheckpointStore) Delete(ctx context.Context, id projection.ID) error {
	query := fmt.Sprintf(
		`DELETE FROM %s WHERE projection_name = ? AND projection_version = ?`,
		quoteIdentifier(s.tableName),
	)

	result, err := s.db.ExecContext(ctx, query, id.Name, id.Version)
	if err != nil {
		return fmt.Errorf("deleting checkpoint: %w", err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("counting deleted checkpoints: %w", err)
	}

	if affected == 0 {
		return checkpointstore.ErrCheckpointNotFound
	}

	return nil
}
