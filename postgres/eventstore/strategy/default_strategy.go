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
	"github.com/lib/pq"
)

const (
	defaultEventsTableName  = "event"
	defaultStreamsTableName = "stream"
)

// tableNameRE is the pre-compiled regex used to validate SQL table identifiers.
var tableNameRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}$`)

// directionSQL maps read direction values to their SQL ORDER BY keywords.
var directionSQL = map[eventstore.ReadStreamDirection]string{
	eventstore.Forward: "ASC",
	eventstore.Reverse: "DESC",
}

// DefaultStrategy is a strategy that stores all events in a single database table,
// with a separate table for storing stream metadata.
//
// By default, the events table is named "event" and the streams table is named "stream".
// These can be overridden by passing options to NewDefaultStrategy.
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
		if err := opt(strategy); err != nil {
			return nil, fmt.Errorf("applying option: %w", err)
		}
	}

	return strategy, nil
}

// ReadStreamQuery returns a SQL query for reading events from a specific stream.
// The query must be designed to expect exactly two parameters: the stream type and the stream ID.
func (s *DefaultStrategy) ReadStreamQuery(streamID typeid.ID, opts eventstore.ReadStreamOptions) (string, []any, error) {
	direction, ok := directionSQL[opts.Direction]
	if !ok {
		direction = "ASC"
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
	`, pq.QuoteIdentifier(s.eventsTableName), direction, offsetClause, limitClause),
		[]any{
			streamID.Type,
			streamID.UUID,
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

	e.ID = typeid.New(eventType, eventID)
	e.StreamID = typeid.New(streamType, streamID)

	return &e, nil
}

// NextHighwaterMark reserves and returns the next highwater mark (stream offset) for the given stream ID.
// It uses the provided transactional context to ensure atomicity.
func (s *DefaultStrategy) NextHighwaterMark(ctx context.Context, tx *sql.Tx, streamID typeid.ID, numEvents int) (int64, error) {
	var newOffset int64
	if err := tx.QueryRowContext(ctx, fmt.Sprintf(`
		INSERT INTO %s (stream_type, stream_id, last_offset)
		VALUES ($1, $2, $3)
		ON CONFLICT (stream_type, stream_id)
		DO UPDATE SET last_offset = %s.last_offset + $3
		RETURNING last_offset`,
		pq.QuoteIdentifier(s.streamsTableName),
		pq.QuoteIdentifier(s.streamsTableName),
	), streamID.Type, streamID.UUID, numEvents).Scan(&newOffset); err != nil {
		return 0, fmt.Errorf("reserving stream offsets: %w", err)
	}
	return newOffset, nil
}

// AppendStreamStatement returns a SQL statement for appending an event to a stream.
func (s *DefaultStrategy) AppendStreamStatement() (string, error) {
	return fmt.Sprintf(`
		INSERT INTO %s (
			event_id,
			stream_type,
			stream_id,
			event_type,
			timestamp,
			stream_offset,
			data
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`, pq.QuoteIdentifier(s.eventsTableName)), nil
}

// AppendStreamExecArgs returns the arguments for executing the append statement for the given event.
func (s *DefaultStrategy) AppendStreamExecArgs(event *eventstore.Event) []any {
	return []any{
		event.ID.UUID,
		event.StreamID.Type,
		event.StreamID.UUID,
		event.ID.Type,
		event.Timestamp,
		event.StreamVersion,
		event.Data,
	}
}

// DefaultStrategyOption is a functional option that configures a DefaultStrategy.
type DefaultStrategyOption func(*DefaultStrategy) error

// WithEventsTableName sets a custom table name for the table that stores events.
//
// The default is "event".
func WithEventsTableName(name string) DefaultStrategyOption {
	return func(s *DefaultStrategy) error {
		if err := validateTableName(name); err != nil {
			return fmt.Errorf("invalid events table name: %w", err)
		}
		s.eventsTableName = name
		return nil
	}
}

// WithStreamsTableName sets a custom table name for the table that stores stream metadata.
//
// The default is "stream".
func WithStreamsTableName(name string) DefaultStrategyOption {
	return func(s *DefaultStrategy) error {
		if err := validateTableName(name); err != nil {
			return fmt.Errorf("invalid streams table name: %w", err)
		}
		s.streamsTableName = name
		return nil
	}
}

// Schema returns the complete SQL schema for the event store.
func (s *DefaultStrategy) Schema() string {
	return fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id            bigserial    PRIMARY KEY,
			stream_id     uuid         NOT NULL,
			stream_type   varchar(255) NOT NULL,
			event_id      uuid         NOT NULL,
			event_type    varchar(255) NOT NULL,
			stream_offset bigint       NOT NULL,
			timestamp     timestamptz  NOT NULL,
			data          jsonb,

			CONSTRAINT event_stream_offset_unique UNIQUE (stream_id, stream_type, stream_offset),

			CHECK (stream_offset > 0)
		);

		CREATE TABLE IF NOT EXISTS %s (
			stream_type   varchar(255) NOT NULL,
			stream_id     uuid         NOT NULL,
			last_offset   bigint       NOT NULL DEFAULT 0,

			PRIMARY KEY (stream_type, stream_id)
		);
	`, pq.QuoteIdentifier(s.eventsTableName), pq.QuoteIdentifier(s.streamsTableName))
}

// ListStreams returns metadata for all streams in the event store.
func (s *DefaultStrategy) ListStreams(ctx context.Context, db *sql.DB) ([]StreamMetadata, error) {
	query := fmt.Sprintf(`
		SELECT
			stream_type,
			stream_id,
			last_offset
		FROM %s
	`, pq.QuoteIdentifier(s.streamsTableName))

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("querying streams: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var streams []StreamMetadata
	for rows.Next() {
		var (
			streamType string
			streamID   uuid.UUID
			lastOffset int64
		)
		if err := rows.Scan(&streamType, &streamID, &lastOffset); err != nil {
			return nil, fmt.Errorf("scanning stream row: %w", err)
		}

		streams = append(streams, StreamMetadata{
			StreamID:   typeid.New(streamType, streamID),
			LastOffset: lastOffset,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating stream rows: %w", err)
	}

	return streams, nil
}

// ReadAll returns a SQL rows result set for reading all events in the event store.
func (s *DefaultStrategy) ReadAll(ctx context.Context, db *sql.DB, opts eventstore.ReadStreamOptions) (*sql.Rows, error) {
	direction, ok := directionSQL[opts.Direction]
	if !ok {
		direction = "ASC"
	}

	offsetClause := ""
	if opts.Offset > 0 {
		offsetClause = fmt.Sprintf("OFFSET %d", opts.Offset)
	}

	limitClause := ""
	if opts.Count > 0 {
		limitClause = fmt.Sprintf("LIMIT %d", opts.Count)
	}

	query := fmt.Sprintf(`
		SELECT
			stream_id,
			stream_type,
			event_id,
			event_type,
			timestamp,
			stream_offset,
			data
		FROM %s
		ORDER BY
			id %s
		%s
		%s
	`, pq.QuoteIdentifier(s.eventsTableName), direction, offsetClause, limitClause)

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("querying all events: %w", err)
	}

	return rows, nil
}

// validateTableName validates that the given table name is a valid SQL identifier.
func validateTableName(name string) error {
	if !tableNameRE.MatchString(name) {
		return errors.New("table name must be a valid SQL identifier")
	}
	return nil
}
