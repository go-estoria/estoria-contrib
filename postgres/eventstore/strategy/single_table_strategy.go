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

// SingleTableStrategy is a strategy for storing all events in a single database table.
type SingleTableStrategy struct {
	eventsTableName  string
	streamsTableName string
}

// NewSingleTableStrategy creates a new SingleTableStrategy with optional options.
func NewSingleTableStrategy(opts ...SingleTableStrategyOption) (*SingleTableStrategy, error) {
	strategy := &SingleTableStrategy{
		eventsTableName:  defaultEventsTableName,
		streamsTableName: defaultStreamsTableName,
	}

	for _, opt := range opts {
		opt(strategy)
	}

	if err := validateTableName(strategy.eventsTableName); err != nil {
		return nil, fmt.Errorf("invalid table name: %w", err)
	}

	return strategy, nil
}

// ReadStreamQuery returns a SQL query for reading events from a specific stream.
// The query must be designed to expect exactly two parameters: the stream type and the stream ID.
func (s *SingleTableStrategy) ReadStreamQuery(streamID typeid.UUID, opts eventstore.ReadStreamOptions) (string, []any, error) {
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

func (s *SingleTableStrategy) NextHighwaterMark(ctx context.Context, tx *sql.Tx, streamID typeid.UUID, numEvents int) (int64, error) {
	var nextOffset = int64(numEvents)

	// reserve a block of offsets for the stream
	res, err := tx.QueryContext(ctx, fmt.Sprintf(`
		UPDATE "%s"
		SET
			last_offset = last_offset + $3
		WHERE
			stream_type = $1
			AND
			stream_id = $2
		RETURNING last_offset;
	`, s.streamsTableName), streamID.TypeName(), streamID.Value(), numEvents)
	if err != nil {
		return 0, fmt.Errorf("updating last offset: %w", err)
	}

	defer func() { _ = res.Close() }()

	if !res.Next() {
		if err := res.Err(); err != nil {
			return 0, fmt.Errorf("preparing row: %w", err)
		}

		// stream metadata does not exist yet, insert it and carry on
		if _, err := tx.ExecContext(ctx, fmt.Sprintf(`
			INSERT INTO "%s" (
				stream_type,
				stream_id,
				last_offset
			)
			VALUES ($1, $2, $3);
		`, s.streamsTableName), streamID.TypeName(), streamID.Value(), numEvents); err != nil {
			return 0, fmt.Errorf("inserting new stream metadata: %w", err)
		}

	} else if err := res.Scan(&nextOffset); err != nil {
		return 0, fmt.Errorf("scanning updated offset: %w", err)
	}

	return nextOffset, nil
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
	`, s.eventsTableName), nil
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
		s.eventsTableName = name
	}
}

func WithStreamsTableName(name string) SingleTableStrategyOption {
	return func(s *SingleTableStrategy) {
		s.streamsTableName = name
	}
}

// Schema returns the complete SQL schema for the event store.
func (s *SingleTableStrategy) Schema() string {
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
