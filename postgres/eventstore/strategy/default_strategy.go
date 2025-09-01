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

const (
	defaultEventsTableName  = "event"
	defaultStreamsTableName = "stream"
)

// DefaultStrategy is a strategy for storing all events in a single database table,
// with a separate table for stream metadata.
type DefaultStrategy struct {
	eventsTableName  string
	streamsTableName string
}

// NewDefaultStrategy creates a new DefaultStrategy with optional options.
func NewDefaultStrategy(opts ...DefaultStrategyOption) (*DefaultStrategy, error) {
	strategy := &DefaultStrategy{
		eventsTableName:  defaultEventsTableName,
		streamsTableName: defaultStreamsTableName,
	}

	for _, opt := range opts {
		opt(strategy)
	}

	if err := validateTableName(strategy.eventsTableName); err != nil {
		return nil, fmt.Errorf("invalid events table name: %w", err)
	} else if err := validateTableName(strategy.streamsTableName); err != nil {
		return nil, fmt.Errorf("invalid streams table name: %w", err)
	}

	return strategy, nil
}

// ReadStreamQuery returns a SQL query for reading events from a specific stream.
// The query must be designed to expect exactly two parameters: the stream type and the stream ID.
func (s *DefaultStrategy) ReadStreamQuery(streamID typeid.UUID, opts eventstore.ReadStreamOptions) (string, []any, error) {
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
		FROM "%s"
		WHERE
			stream_type = $1
			AND
			stream_id = $2
		ORDER BY
			stream_offset %s
		%s
		%s
	`, s.eventsTableName, direction, offsetClause, limitClause), []any{
			streamID.TypeName(),
			streamID.Value(),
		}, nil
}

// ScanEventRow scans a single event row from the given SQL rows and returns an event.
func (s *DefaultStrategy) ScanEventRow(rows *sql.Rows) (*eventstore.Event, error) {
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

// NextHighwaterMark reserves and returns the next highwater mark (stream offset) for the given stream ID.
// It uses the provided transactional context to ensure atomicity.
func (s *DefaultStrategy) NextHighwaterMark(ctx context.Context, tx *sql.Tx, streamID typeid.UUID, numEvents int) (int64, error) {
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO "%s" (
			stream_type,
			stream_id,
			last_offset
		)
		VALUES ($1, $2, 0)
		ON CONFLICT (stream_type, stream_id) DO NOTHING`,
		s.streamsTableName,
	), streamID.TypeName(), streamID.Value()); err != nil {
		return 0, fmt.Errorf("upserting stream metadata: %w", err)
	}

	var newOffset int64
	if err := tx.QueryRowContext(ctx, fmt.Sprintf(`
		UPDATE "%s"
		SET
			last_offset = last_offset + $3
		WHERE
			stream_type = $1
			AND
			stream_id = $2
		RETURNING last_offset`,
		s.streamsTableName,
	), streamID.TypeName(), streamID.Value(), numEvents).Scan(&newOffset); err != nil {
		return 0, fmt.Errorf("bumping last_offset: %w", err)
	}
	return newOffset, nil
}

// AppendStreamStatement returns a SQL statement for appending an event to a stream.
func (s *DefaultStrategy) AppendStreamStatement(_ []typeid.UUID) (string, error) {
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
	`, s.eventsTableName), nil
}

// AppendStreamExecArgs returns the arguments for executing the append statement for the given event.
func (s *DefaultStrategy) AppendStreamExecArgs(event *eventstore.Event) []any {
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

// DefaultStrategyOption is a function option that configures a DefaultStrategy.
type DefaultStrategyOption func(*DefaultStrategy)

// WithTableName sets a custom table name for the table that stores events.
//
// The default is "event".
func WithTableName(name string) DefaultStrategyOption {
	return func(s *DefaultStrategy) {
		s.eventsTableName = name
	}
}

// WithStreamsTableName sets a custom table name for the table that stores stream metadata.
//
// The default is "stream".
func WithStreamsTableName(name string) DefaultStrategyOption {
	return func(s *DefaultStrategy) {
		s.streamsTableName = name
	}
}

// Schema returns the complete SQL schema for the event store.
func (s *DefaultStrategy) Schema() string {
	return fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS "%s" (
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
		);

		CREATE TABLE IF NOT EXISTS "%s" (
			stream_type   varchar(255) NOT NULL,
			stream_id     uuid         NOT NULL,
			last_offset   bigint       NOT NULL DEFAULT 0,

			PRIMARY KEY (stream_type, stream_id)
		);
	`, s.eventsTableName, s.streamsTableName)
}

// validateTableName validates that the given table name is a valid SQL identifier.
func validateTableName(name string) error {
	if ok, err := regexp.Match(`^[A-Za-z_][A-Za-z0-9_$]{0,62}$`, []byte(name)); err != nil {
		return fmt.Errorf("matching regex: %w", err)
	} else if !ok {
		return errors.New("table name must be a valid SQL identifier")
	}

	return nil
}
