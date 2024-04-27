package strategy

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/go-estoria/estoria"
	"go.jetpack.io/typeid"
)

type SingleTableStrategy struct {
	db    *sql.DB
	table string
	log   *slog.Logger
}

func NewSingleTableStrategy(db *sql.DB, table string) (*SingleTableStrategy, error) {
	if db == nil {
		return nil, fmt.Errorf("database is required")
	} else if table == "" {
		return nil, fmt.Errorf("table is required")
	}

	return &SingleTableStrategy{
		db:    db,
		table: table,
		log:   slog.Default().WithGroup("eventstore"),
	}, nil
}

func (s *SingleTableStrategy) GetStreamIterator(
	ctx context.Context,
	streamID typeid.AnyID,
	opts estoria.ReadStreamOptions,
) (estoria.EventStreamIterator, error) {
	sortDirection := "ASC"
	if opts.Direction == estoria.Reverse {
		sortDirection = "DESC"
	}

	s.log.Debug("querying events", "stream_id", streamID)

	limitClause := ""
	if opts.Count > 0 {
		limitClause = fmt.Sprintf("LIMIT %d", opts.Count)
	}

	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT event_id, event_type, timestamp, data
		FROM %s
		WHERE stream_type = $1 AND stream_id = $2
		ORDER BY version %s
		%s
		OFFSET $3
	`, s.table, sortDirection, limitClause), streamID.Prefix(), streamID.Suffix(), opts.Offset)
	if err != nil {
		return nil, fmt.Errorf("querying events: %w", err)
	}

	return &streamIterator{
		streamID: streamID,
		rows:     rows,
	}, nil
}

func (s *SingleTableStrategy) InsertStreamEvents(
	tx *sql.Tx,
	streamID typeid.AnyID,
	events []estoria.EventStoreEvent,
	opts estoria.AppendStreamOptions,
) (sql.Result, error) {
	latestVersion, err := s.getLatestVersion(tx, streamID)
	if err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			s.log.Error("error rolling back transaction", "error", rollbackErr)
		}

		return nil, fmt.Errorf("getting latest version: %w", err)
	}

	if opts.ExpectVersion > 0 && latestVersion != opts.ExpectVersion {
		return nil, fmt.Errorf("expected version %d, but stream has version %d", opts.ExpectVersion, latestVersion)
	}

	stmt, err := tx.Prepare(fmt.Sprintf(`
		INSERT INTO %s (event_id, stream_type, stream_id, event_type, timestamp, version, data)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`, s.table))
	if err != nil {
		return nil, fmt.Errorf("preparing statement: %w", err)
	}
	defer stmt.Close()

	version := latestVersion
	for _, event := range events {
		version++
		_, err := stmt.Exec(
			event.ID().Suffix(),
			event.StreamID().Prefix(),
			event.StreamID().Suffix(),
			event.ID().Prefix(),
			event.Timestamp(),
			version,
			event.Data(),
		)
		if err != nil {
			return nil, fmt.Errorf("executing statement: %w", err)
		}
	}

	return nil, nil

}

func (s *SingleTableStrategy) getLatestVersion(tx *sql.Tx, streamID typeid.AnyID) (int64, error) {
	var version int64
	if err := tx.QueryRow(fmt.Sprintf(`
		SELECT COALESCE(MAX(version), 0)
		FROM %s
		WHERE stream_type = $1 AND stream_id = $2
	`, s.table), streamID.Prefix(), streamID.Suffix()).Scan(&version); err != nil {
		return 0, fmt.Errorf("querying latest version: %w", err)
	}

	return version, nil
}
