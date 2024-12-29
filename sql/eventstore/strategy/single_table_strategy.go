package strategy

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/go-estoria/estoria"
	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
)

type SQLDB interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

type SQLTx interface {
	QueryRow(query string, args ...any) *sql.Row
	Rollback() error
	Prepare(query string) (*sql.Stmt, error)
}

type SingleTableStrategy struct {
	db        SQLDB
	tableName string
	log       estoria.Logger
}

type InsertStreamEventsResult struct {
	InsertedEvents []*eventstore.Event
}

func NewSingleTableStrategy(db SQLDB, tableName string) (*SingleTableStrategy, error) {
	if db == nil {
		return nil, fmt.Errorf("database is required")
	} else if tableName == "" {
		return nil, fmt.Errorf("table is required")
	}

	return &SingleTableStrategy{
		db:        db,
		tableName: tableName,
		log:       estoria.GetLogger().WithGroup("eventstore"),
	}, nil
}

func (s *SingleTableStrategy) GetStreamIterator(
	ctx context.Context,
	streamID typeid.UUID,
	opts eventstore.ReadStreamOptions,
) (eventstore.StreamIterator, error) {
	sortDirection := "ASC"
	if opts.Direction == eventstore.Reverse {
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
	`, s.tableName, sortDirection, limitClause), streamID.TypeName(), streamID.Value(), opts.Offset)
	if err != nil {
		return nil, fmt.Errorf("querying events: %w", err)
	}

	return &streamIterator{
		streamID: streamID,
		rows:     rows,
	}, nil
}

func (s *SingleTableStrategy) InsertStreamEvents(
	tx SQLTx,
	streamID typeid.UUID,
	events []*eventstore.WritableEvent,
	opts eventstore.AppendStreamOptions,
) (*InsertStreamEventsResult, error) {
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
	`, s.tableName))
	if err != nil {
		return nil, fmt.Errorf("preparing statement: %w", err)
	}
	defer stmt.Close()

	now := time.Now()

	version := latestVersion

	fullEvents := make([]*eventstore.Event, len(events))
	for i, we := range events {
		version++
		fullEvents[i] = &eventstore.Event{
			ID:            we.ID,
			StreamID:      streamID,
			StreamVersion: version,
			Timestamp:     now,
			Data:          we.Data,
		}

		_, err := stmt.Exec(
			we.ID.Value(),
			streamID.TypeName(),
			streamID.Value(),
			we.ID.TypeName(),
			now,
			version,
			we.Data,
		)
		if err != nil {
			return nil, fmt.Errorf("executing statement: %w", err)
		}
	}

	return &InsertStreamEventsResult{
		InsertedEvents: fullEvents,
	}, nil
}

func (s *SingleTableStrategy) getLatestVersion(tx SQLTx, streamID typeid.UUID) (int64, error) {
	var version int64
	if err := tx.QueryRow(fmt.Sprintf(`
		SELECT COALESCE(MAX(version), 0)
		FROM %s
		WHERE stream_type = $1 AND stream_id = $2
	`, s.tableName), streamID.TypeName(), streamID.Value()).Scan(&version); err != nil {
		return 0, fmt.Errorf("querying latest version: %w", err)
	}

	return version, nil
}
