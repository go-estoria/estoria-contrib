// Package checkpointstore provides a Postgres-backed projection checkpoint store.
package checkpointstore

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-estoria/estoria/projection"
	"github.com/go-estoria/estoria/projection/checkpointstore"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CheckpointStore persists projection checkpoints in a Postgres table, one row
// per projection ID.
type CheckpointStore struct {
	pool      *pgxpool.Pool
	tableName string
}

var _ checkpointstore.Store = (*CheckpointStore)(nil)

// New creates a new checkpoint store using the provided pgx connection pool.
func New(pool *pgxpool.Pool, opts ...Option) (*CheckpointStore, error) {
	if pool == nil {
		return nil, errors.New("pool is required")
	}

	s := &CheckpointStore{
		pool:      pool,
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
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    projection_name    text        NOT NULL,
    projection_version integer     NOT NULL,
    position           bigint      NOT NULL,
    updated_at         timestamptz NOT NULL,

    PRIMARY KEY (projection_name, projection_version)
);
`, pgx.Identifier{s.tableName}.Sanitize())
}

// Load returns the projection's checkpoint, or
// checkpointstore.ErrCheckpointNotFound if none has been saved.
func (s *CheckpointStore) Load(ctx context.Context, id projection.ID) (checkpointstore.Checkpoint, error) {
	query := fmt.Sprintf(
		`SELECT position, updated_at FROM %s WHERE projection_name = $1 AND projection_version = $2`,
		pgx.Identifier{s.tableName}.Sanitize(),
	)

	checkpoint := checkpointstore.Checkpoint{ProjectionID: id}
	if err := s.pool.QueryRow(ctx, query, id.Name, id.Version).
		Scan(&checkpoint.Position, &checkpoint.UpdatedAt); errors.Is(err, pgx.ErrNoRows) {
		return checkpointstore.Checkpoint{}, checkpointstore.ErrCheckpointNotFound
	} else if err != nil {
		return checkpointstore.Checkpoint{}, fmt.Errorf("querying checkpoint: %w", err)
	}

	checkpoint.UpdatedAt = checkpoint.UpdatedAt.UTC()

	return checkpoint, nil
}

// Save records position as the projection's checkpoint, assigning updated_at
// from the database clock even when the position is unchanged.
func (s *CheckpointStore) Save(ctx context.Context, id projection.ID, position int64) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (projection_name, projection_version, position, updated_at)
		VALUES ($1, $2, $3, now())
		ON CONFLICT (projection_name, projection_version)
		DO UPDATE SET position = EXCLUDED.position, updated_at = now()`,
		pgx.Identifier{s.tableName}.Sanitize(),
	)

	if _, err := s.pool.Exec(ctx, query, id.Name, id.Version, position); err != nil {
		return fmt.Errorf("upserting checkpoint: %w", err)
	}

	return nil
}

// Delete removes the projection's checkpoint, or reports
// checkpointstore.ErrCheckpointNotFound if none exists.
func (s *CheckpointStore) Delete(ctx context.Context, id projection.ID) error {
	query := fmt.Sprintf(
		`DELETE FROM %s WHERE projection_name = $1 AND projection_version = $2`,
		pgx.Identifier{s.tableName}.Sanitize(),
	)

	tag, err := s.pool.Exec(ctx, query, id.Name, id.Version)
	if err != nil {
		return fmt.Errorf("deleting checkpoint: %w", err)
	}

	if tag.RowsAffected() == 0 {
		return checkpointstore.ErrCheckpointNotFound
	}

	return nil
}
