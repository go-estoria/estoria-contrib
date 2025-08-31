package strategy

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"

	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
	"github.com/gofrs/uuid/v5"
)

type SingleTableStrategy struct {
	tableName string
}

func NewSingleTableStrategy(opts ...SingleTableStrategyOption) *SingleTableStrategy {
	strategy := &SingleTableStrategy{
		tableName: "events",
	}

	for _, opt := range opts {
		opt(strategy)
	}

	return strategy
}

// ReadStreamQuery returns a SQL query for reading events from a specific stream.
// The query must be designed to expect exactly two parameters: the stream type and the stream ID.
func (s *SingleTableStrategy) ReadStreamQuery(streamID typeid.UUID, opts eventstore.ReadStreamOptions) (string, error) {
	direction := "ASC"
	if opts.Direction == eventstore.Reverse {
		direction = "DESC"
	}

	offsetClause := ""
	if opts.Offset > 0 {
		offsetClause = fmt.Sprintf("OFFSET %d", opts.Offset)
	}

	limitClause := ""
	if opts.Count > 0 {
		limitClause = fmt.Sprintf("LIMIT %d", opts.Count)
	}

	return fmt.Sprintf(`
		SELECT
			stream_id,
			stream_type,
			event_id,
			event_type,
			timestamp,
			stream_offset,
			data
		FROM %s
		WHERE
			stream_type = $1
			AND
			stream_id = $2
		ORDER BY
			stream_offset %s
		%s
		%s
	`, s.tableName, direction, offsetClause, limitClause), nil
}

// ScanEventRow scans a single event row from the given SQL rows and returns an event.
func (s *SingleTableStrategy) ScanEventRow(rows *sql.Rows) (*eventstore.Event, error) {
	var (
		e          eventstore.Event
		streamID   uuid.UUID
		streamType string
		eventID    uuid.UUID
		eventType  string
	)
	if err := rows.Scan(
		&streamID,
		&streamType,
		&eventID,
		&eventType,
		&e.Timestamp,
		&e.StreamVersion,
		&e.Data,
	); err != nil {
		return nil, fmt.Errorf("scanning event row: %w", err)
	}

	e.ID = typeid.FromUUID(eventType, eventID)
	e.StreamID = typeid.FromUUID(streamType, streamID)

	return &e, nil
}

// AppendStreamStatement returns a SQL statement for appending an event to a stream.
func (s *SingleTableStrategy) AppendStreamStatement(_ []typeid.UUID) (string, error) {
	return fmt.Sprintf(`
		INSERT INTO "%s" (
			event_id,
			stream_type,
			stream_id,
			event_type,
			timestamp,
			stream_offset,
			data
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`, s.tableName), nil
}

func (s *SingleTableStrategy) AppendStreamExecArgs(event *eventstore.Event) []any {
	return []any{
		event.ID.Value(),
		event.StreamID.TypeName(),
		event.StreamID.Value(),
		event.ID.TypeName(),
		event.Timestamp,
		event.StreamVersion,
		event.Data,
	}
}

type SingleTableStrategyOption func(*SingleTableStrategy)

// WithTableName sets a custom table name for the event store.
//
// The default table name is "events".
func WithTableName(name string) SingleTableStrategyOption {
	return func(s *SingleTableStrategy) {
		s.tableName = name
	}
}

// Schema returns the complete SQL schema for the event store.
func (s *SingleTableStrategy) Schema() string {
	return fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id            bigserial    PRIMARY KEY,
			stream_id     uuid         NOT NULL,
			stream_type   varchar(255) NOT NULL,
			event_id      uuid         NOT NULL,
			event_type    varchar(255) NOT NULL,
			timestamp     timestamptz  NOT NULL,
			stream_offset bigint       NOT NULL,
			data          bytea,

			UNIQUE (stream_id, stream_type, stream_offset),

			CHECK (stream_offset > 0)
		)
	`, s.tableName)
}

// Finds the highest offset for the given stream.
func (s *SingleTableStrategy) GetHighestOffset(ctx context.Context, tx *sql.Tx, streamID typeid.UUID) (int64, error) {
	var offset int64
	if err := tx.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT stream_offset
		FROM "%s"
		WHERE stream_type = $1 AND stream_id = $2
		ORDER BY stream_offset DESC
		LIMIT 1
	`, s.tableName), streamID.TypeName(), streamID.Value()).Scan(&offset); errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	} else if err != nil {
		return 0, fmt.Errorf("querying highest offset: %w", err)
	}

	return offset, nil
}

// validateTableName validates that the given table name is a valid SQL identifier.
func validateTableName(name string) error {
	if ok, err := regexp.Match(`^[A-Za-z_][A-Za-z0-9_$]{0,62}$`, []byte(name)); err != nil {
		return fmt.Errorf("validating table name: %w", err)
	} else if !ok {
		return errors.New("table name must be a valid SQL identifier")
	}

	return nil
}
