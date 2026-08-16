package strategy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"

	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
	"github.com/gofrs/uuid/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultEventsTableName  = "event"
	defaultStreamsTableName = "stream"
)

// tableNameRE is the pre-compiled regex used to validate SQL table identifiers.
var tableNameRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}$`)

const (
	sortAscending  = "ASC"
	sortDescending = "DESC"
)

// directionSQL maps read direction values to their SQL ORDER BY keywords.
var directionSQL = map[eventstore.ReadStreamDirection]string{
	eventstore.Forward: sortAscending,
	eventstore.Reverse: sortDescending,
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
// The base query takes two parameters: $1=stream_type, $2=stream_id.
// When AfterVersion > 0, a third parameter $3 is added for the version boundary.
func (s *DefaultStrategy) ReadStreamQuery(streamID typeid.ID, opts eventstore.ReadStreamOptions) (string, []any, error) {
	direction, ok := directionSQL[opts.Direction]
	if !ok {
		direction = sortAscending
	}

	args := []any{
		streamID.Type,
		streamID.UUID,
	}

	versionClause := ""
	if opts.AfterVersion > 0 {
		args = append(args, opts.AfterVersion)
		if opts.Direction == eventstore.Reverse {
			versionClause = "AND stream_offset <= $3"
		} else {
			versionClause = "AND stream_offset > $3"
		}
	}

	limitClause := ""
	if opts.Count > 0 {
		limitClause = fmt.Sprintf("LIMIT %d", opts.Count)
	}

	return fmt.Sprintf(`
		SELECT
			id,
			stream_id,
			stream_type,
			event_id,
			event_type,
			timestamp,
			stream_offset,
			data,
			data_content_type,
			metadata
		FROM %s
		WHERE
			stream_type = $1
			AND stream_id = $2
			%s
		ORDER BY
			stream_offset %s
		%s
	`, quoteIdent(s.eventsTableName), versionClause, direction, limitClause),
		args, nil
}

// ScanEventRow scans a single event row from the given pgx rows and returns an event.
func (s *DefaultStrategy) ScanEventRow(rows pgx.Rows) (*eventstore.Event, error) {
	var (
		e              eventstore.Event
		globalPosition int64
		streamID       uuid.UUID
		streamType     string
		eventID        uuid.UUID
		eventType      string
		metadata       []byte
	)
	if err := rows.Scan(
		&globalPosition,
		&streamID,
		&streamType,
		&eventID,
		&eventType,
		&e.Timestamp,
		&e.StreamVersion,
		&e.Data,
		&e.DataContentType,
		&metadata,
	); err != nil {
		return nil, fmt.Errorf("scanning event row: %w", err)
	}

	e.GlobalPosition = &globalPosition
	e.ID = typeid.New(eventType, eventID)
	e.StreamID = typeid.New(streamType, streamID)

	if metadata != nil {
		if err := json.Unmarshal(metadata, &e.Metadata); err != nil {
			return nil, fmt.Errorf("unmarshaling event metadata: %w", err)
		}
	}

	return &e, nil
}

// NextHighwaterMark reserves and returns the next highwater mark (stream offset) for the given stream ID.
// It uses the provided transactional context to ensure atomicity.
func (s *DefaultStrategy) NextHighwaterMark(ctx context.Context, tx pgx.Tx, streamID typeid.ID, numEvents int) (int64, error) {
	var newOffset int64
	if err := tx.QueryRow(ctx, fmt.Sprintf(`
		INSERT INTO %s (stream_type, stream_id, last_offset)
		VALUES ($1, $2, $3)
		ON CONFLICT (stream_type, stream_id)
		DO UPDATE SET last_offset = %s.last_offset + $3
		RETURNING last_offset`,
		quoteIdent(s.streamsTableName),
		quoteIdent(s.streamsTableName),
	), streamID.Type, streamID.UUID, numEvents).Scan(&newOffset); err != nil {
		return 0, fmt.Errorf("reserving stream offsets: %w", err)
	}
	return newOffset, nil
}

// ReserveGlobalPositions reserves a contiguous range of numEvents global
// positions within the transaction and returns the first. The reservation
// updates the single allocator row, whose lock is then held until the
// transaction commits or rolls back: a later reservation cannot proceed — and
// so cannot become visible — while an earlier one is unresolved, which is
// what makes published positions a stable prefix. A rolled-back reservation
// is returned to the allocator rather than left as a gap.
func (s *DefaultStrategy) ReserveGlobalPositions(ctx context.Context, tx pgx.Tx, numEvents int) (int64, error) {
	var last int64
	if err := tx.QueryRow(ctx, fmt.Sprintf(
		`UPDATE %s SET last_position = last_position + $1 RETURNING last_position`,
		quoteIdent(s.allocatorTableName()),
	), numEvents).Scan(&last); errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("position allocator %q has no row; apply the current schema before writing", s.allocatorTableName())
	} else if err != nil {
		return 0, fmt.Errorf("updating position allocator: %w", err)
	}

	return last - int64(numEvents) + 1, nil
}

// allocatorTableName returns the name of the single-row table from which the
// events table's global positions are reserved.
func (s *DefaultStrategy) allocatorTableName() string {
	return s.eventsTableName + "_position_allocator"
}

// StreamExists reports whether the given stream exists, regardless of how many events it
// holds or which of them a read's options would match.
//
// A row in the streams table is created only by NextHighwaterMark, within the same
// transaction that appends the events, so its presence is authoritative.
func (s *DefaultStrategy) StreamExists(ctx context.Context, pool *pgxpool.Pool, streamID typeid.ID) (bool, error) {
	var exists int
	if err := pool.QueryRow(ctx, fmt.Sprintf(`
		SELECT 1
		FROM %s
		WHERE
			stream_type = $1
			AND stream_id = $2`,
		quoteIdent(s.streamsTableName),
	), streamID.Type, streamID.UUID).Scan(&exists); errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf("querying stream: %w", err)
	}

	return true, nil
}

// DeleteStream deletes events from a stream within the given transaction. Whether the
// stream exists is decided by its streams-table row, never by its event count — a
// truncated-empty stream holds no event rows yet exists — and an absent row reports
// eventstore.ErrStreamNotFound. With ToVersion 0 both the events and the streams-table
// row are deleted, so a subsequent append recreates the stream from version 1; with
// ToVersion > 0 only events at or below the bound are deleted and the row's last_offset
// survives, so appends continue from the existing tip even when truncation emptied the
// stream.
func (s *DefaultStrategy) DeleteStream(ctx context.Context, tx pgx.Tx, streamID typeid.ID, opts eventstore.DeleteStreamOptions) error {
	var exists int
	if err := tx.QueryRow(ctx, fmt.Sprintf(`
		SELECT 1
		FROM %s
		WHERE
			stream_type = $1
			AND stream_id = $2`,
		quoteIdent(s.streamsTableName),
	), streamID.Type, streamID.UUID).Scan(&exists); errors.Is(err, pgx.ErrNoRows) {
		return eventstore.ErrStreamNotFound
	} else if err != nil {
		return fmt.Errorf("querying stream: %w", err)
	}

	versionClause := ""
	args := []any{streamID.Type, streamID.UUID}
	if opts.ToVersion > 0 {
		versionClause = "AND stream_offset <= $3"
		args = append(args, opts.ToVersion)
	}

	if _, err := tx.Exec(ctx, fmt.Sprintf(`
		DELETE FROM %s
		WHERE
			stream_type = $1
			AND stream_id = $2
			%s`,
		quoteIdent(s.eventsTableName), versionClause,
	), args...); err != nil {
		return fmt.Errorf("deleting events: %w", err)
	}

	if opts.ToVersion == 0 {
		if _, err := tx.Exec(ctx, fmt.Sprintf(`
			DELETE FROM %s
			WHERE
				stream_type = $1
				AND stream_id = $2`,
			quoteIdent(s.streamsTableName),
		), streamID.Type, streamID.UUID); err != nil {
			return fmt.Errorf("deleting stream: %w", err)
		}
	}

	return nil
}

// AppendStreamStatement returns a SQL statement for appending an event to a
// stream. The event's global position is supplied explicitly as the first
// argument, reserved via ReserveGlobalPositions within the same transaction.
func (s *DefaultStrategy) AppendStreamStatement() (string, error) {
	return fmt.Sprintf(`
		INSERT INTO %s (
			id,
			event_id,
			stream_type,
			stream_id,
			event_type,
			timestamp,
			stream_offset,
			data,
			data_content_type,
			metadata
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`, quoteIdent(s.eventsTableName)), nil
}

// AppendStreamExecArgs returns the arguments for executing the append
// statement for the given event, whose GlobalPosition must already be set.
func (s *DefaultStrategy) AppendStreamExecArgs(event *eventstore.Event) []any {
	var metadataArg any
	if event.Metadata != nil {
		// json.Marshal cannot fail for map[string]string — all keys and values are valid JSON strings.
		metadataArg, _ = json.Marshal(event.Metadata)
	}
	return []any{
		*event.GlobalPosition,
		event.ID.UUID,
		event.StreamID.Type,
		event.StreamID.UUID,
		event.ID.Type,
		event.Timestamp,
		event.StreamVersion,
		event.Data,
		event.DataContentType,
		metadataArg,
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

// Schema returns the complete SQL schema for the event store, idempotent for
// both fresh databases and databases created by earlier versions of this
// strategy. The uniqueness constraint's name is derived from the events table
// name so that stores with different table names can share a database.
//
// Global positions come from the allocator table, one row per events table,
// reserved inside each append transaction. For databases migrating from the
// earlier bigserial schema, the allocator is seeded conservatively at or
// above every position ever observable or allocated: the greater of MAX(id)
// and the abandoned serial sequence's last_value, which PostgreSQL keeps at
// or above every value it ever handed out; last_value counts as allocated
// even when is_called is false, over-reserving by at most one. MAX(id) alone
// would be unsafe — deleting a stream can remove the highest-positioned rows
// while consumers hold checkpoints above the new maximum, and a reused
// position below such a checkpoint would be skipped forever; skipping unused
// sequence values is harmless. The bigserial default is then dropped, so
// writers from earlier versions fail closed instead of bypassing the
// allocator: drain them before migrating, and never run old and new writers
// together.
func (s *DefaultStrategy) Schema() string {
	events := quoteIdent(s.eventsTableName)
	allocator := quoteIdent(s.allocatorTableName())

	return fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id                bigint       PRIMARY KEY,
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

		CREATE TABLE IF NOT EXISTS %s (
			only_row      boolean PRIMARY KEY DEFAULT true CHECK (only_row),
			last_position bigint  NOT NULL
		);

		ALTER TABLE %s ALTER COLUMN id DROP DEFAULT;

		DO $$
		DECLARE
			seq_name text := pg_get_serial_sequence('%s', 'id');
			seq_high bigint := 0;
		BEGIN
			IF seq_name IS NOT NULL THEN
				EXECUTE format('SELECT last_value FROM %%s', seq_name) INTO seq_high;
			END IF;

			INSERT INTO %s (only_row, last_position)
			SELECT true, GREATEST(COALESCE((SELECT MAX(id) FROM %s), 0), seq_high)
			WHERE NOT EXISTS (SELECT 1 FROM %s);
		END
		$$;
	`,
		events,
		quoteIdent(s.eventsTableName+"_stream_offset_unique"),
		quoteIdent(s.streamsTableName),
		allocator,
		events,
		events,
		allocator,
		events,
		allocator,
	)
}

// ListStreams returns metadata for all streams in the event store.
func (s *DefaultStrategy) ListStreams(ctx context.Context, pool *pgxpool.Pool) ([]StreamMetadata, error) {
	query := fmt.Sprintf(`
		SELECT
			stream_type,
			stream_id,
			last_offset
		FROM %s
	`, quoteIdent(s.streamsTableName))

	rows, err := pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("querying streams: %w", err)
	}
	defer rows.Close()

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

// ReadAll returns a SQL rows result set for reading all events in the event store in
// ascending global (id) order, with AfterPosition as an exclusive lower bound on id and
// Count > 0 limiting the number of rows.
func (s *DefaultStrategy) ReadAll(ctx context.Context, pool *pgxpool.Pool, opts eventstore.ReadAllOptions) (pgx.Rows, error) {
	var args []any
	afterClause := ""
	if opts.AfterPosition > 0 {
		args = append(args, opts.AfterPosition)
		afterClause = "WHERE id > $1"
	}

	limitClause := ""
	if opts.Count > 0 {
		limitClause = fmt.Sprintf("LIMIT %d", opts.Count)
	}

	query := fmt.Sprintf(`
		SELECT
			id,
			stream_id,
			stream_type,
			event_id,
			event_type,
			timestamp,
			stream_offset,
			data,
			data_content_type,
			metadata
		FROM %s
		%s
		ORDER BY
			id ASC
		%s
	`, quoteIdent(s.eventsTableName), afterClause, limitClause)

	rows, err := pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("querying all events: %w", err)
	}

	return rows, nil
}

// quoteIdent returns the given identifier wrapped in double quotes,
// equivalent to pq.QuoteIdentifier for a single-part identifier.
func quoteIdent(name string) string {
	return pgx.Identifier{name}.Sanitize()
}

// validateTableName validates that the given table name is a valid SQL identifier.
func validateTableName(name string) error {
	if !tableNameRE.MatchString(name) {
		return errors.New("table name must be a valid SQL identifier")
	}
	return nil
}
