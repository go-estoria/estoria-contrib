package eventstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/go-estoria/estoria"
	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
	"github.com/gofrs/uuid/v5"
)

const (
	DefaultTableName string = "events"
)

// Database is an interface for starting transactions and performing queries.
type Database interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// Transaction is an interface for executing queries within a transactional context.
// It abstracts *sql.Tx by exposing prepare, query, and exec methods, plus commit/rollback.
type Transaction interface {
	Prepare(query string) (*sql.Stmt, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	Commit() error
	Rollback() error
}

// An EventStore stores and retrieves events using Postgres as the underlying storage.
type EventStore struct {
	db            Database
	tableName     string
	log           estoria.Logger
	appendTxHooks []TransactionHook
}

var _ eventstore.StreamReader = (*EventStore)(nil)
var _ eventstore.StreamWriter = (*EventStore)(nil)

// Event represents an event in the event store.
type Event struct {
	eventstore.Event
	GlobalOffset int64
}

// StreamInfo represents information about a single stream in the event store.
type StreamInfo struct {
	// StreamID is the typed ID of the stream.
	StreamID typeid.UUID

	// Offset is the stream-specific offset of the most recent event in the stream.
	// Thus, it also represents the number of events in the stream.
	Offset int64

	// GlobalOffset is the global offset of the most recent event in the stream
	// among all events in the event store.
	GlobalOffset int64
}

// String returns a string representation of a StreamInfo.
func (i StreamInfo) String() string {
	return fmt.Sprintf("stream {ID: %s, Offset: %d, GlobalOffset: %d}", i.StreamID, i.Offset, i.GlobalOffset)
}

// TransactionHook is invoked during a write transaction and receives the transactional context.
type TransactionHook func(tx Transaction, events []*eventstore.Event) error

// New creates a new event store using the given database connection.
func New(db Database, opts ...EventStoreOption) (*EventStore, error) {
	if db == nil {
		return nil, fmt.Errorf("database is required")
	}

	eventStore := &EventStore{
		db:        db,
		tableName: DefaultTableName,
		log:       estoria.GetLogger().WithGroup("eventstore"),
	}

	for _, opt := range opts {
		if err := opt(eventStore); err != nil {
			return nil, fmt.Errorf("applying option: %w", err)
		}
	}

	if err := validateTableName(eventStore.tableName); err != nil {
		return nil, fmt.Errorf("invalid table name: %w", err)
	}

	if err := eventStore.ensureEventsTable(context.Background()); err != nil {
		return nil, fmt.Errorf("creating %s table: %w", eventStore.tableName, err)
	}

	return eventStore, nil
}

// AddTransactionalHook adds a hook to be executed within the transaction when appending events.
// If an error is returned from any hook, the transaction will be aborted.
func (s *EventStore) AddTransactionalHooks(hooks ...TransactionHook) {
	s.appendTxHooks = append(s.appendTxHooks, hooks...)
}

// ListStreams returns a list of metadata for all streams in the event store.
func (s *EventStore) ListStreams(ctx context.Context) ([]StreamInfo, error) {
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT stream_id, stream_type, MAX(stream_offset), MAX(global_offset)
		FROM %s
		GROUP BY stream_id, stream_type
	`, s.tableName))
	if err != nil {
		return nil, fmt.Errorf("querying streams: %w", err)
	}
	defer rows.Close()

	streams := []StreamInfo{}
	for rows.Next() {
		var (
			info       = StreamInfo{}
			streamID   uuid.UUID
			streamType string
		)
		if err := rows.Scan(&streamID, &streamType, &info.Offset, &info.GlobalOffset); err != nil {
			return nil, fmt.Errorf("decoding streams: %w", err)
		}

		info.StreamID = typeid.FromUUID(streamType, streamID)

		streams = append(streams, info)
	}

	return streams, nil
}

// ReadAll returns an iterator for reading all events in the event store.
func (s *EventStore) ReadAll(ctx context.Context, opts eventstore.ReadStreamOptions) (eventstore.StreamIterator, error) {
	s.log.Debug("reading events from Postgres event store",
		"offset", opts.Offset,
		"count", opts.Count,
		"direction", opts.Direction,
	)

	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT stream_id, stream_type, event_id, event_type, timestamp, stream_offset, global_offset, data
		FROM %s
		ORDER BY global_offset ASC
		OFFSET $1
	`, s.tableName), opts.Offset)
	if err != nil {
		return nil, fmt.Errorf("querying events: %w", err)
	}

	return &streamIterator{
		rows: rows,
	}, nil
}

// ReadStream returns an iterator for reading events from the specified stream.
func (s *EventStore) ReadStream(ctx context.Context, streamID typeid.UUID, opts eventstore.ReadStreamOptions) (eventstore.StreamIterator, error) {
	s.log.Debug("reading events from Postgres stream",
		"stream_id", streamID.String(),
		"offset", opts.Offset,
		"count", opts.Count,
		"direction", opts.Direction,
	)

	limitClause := ""
	if opts.Count > 0 {
		limitClause = fmt.Sprintf("LIMIT %d", opts.Count)
	}

	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT stream_id, stream_type, event_id, event_type, timestamp, stream_offset, global_offset, data
		FROM %s
		WHERE stream_type = $1 AND stream_id = $2
		ORDER BY stream_offset ASC
		%s
		OFFSET $3
	`, s.tableName, limitClause), streamID.TypeName(), streamID.Value(), opts.Offset)
	if err != nil {
		return nil, fmt.Errorf("querying stream events: %w", err)
	}

	return &streamIterator{
		rows: rows,
	}, nil
}

// AppendStream appends events to the specified stream.
func (s *EventStore) AppendStream(ctx context.Context, streamID typeid.UUID, events []*eventstore.WritableEvent, opts eventstore.AppendStreamOptions) error {
	s.log.Debug("appending events to Postgres stream", "stream_id", streamID.String(), "events", len(events))

	tx, err := s.db.BeginTx(ctx, nil) // TODO: add transaction options
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}

	defer func() {
		if err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				s.log.Error("error rolling back transaction", "error", rollbackErr, "cause", err)
			}
		}
	}()

	offset, err := s.getHighestOffset(ctx, tx, streamID)
	if err != nil {
		return fmt.Errorf("getting highest offset: %w", err)
	}

	globalOffset, err := s.getHighestGlobalOffset(ctx, tx)
	if err != nil {
		return fmt.Errorf("getting highest global offset: %w", err)
	}

	if opts.ExpectVersion > 0 && offset != opts.ExpectVersion {
		return fmt.Errorf("expected offset %d, but stream has offset %d", opts.ExpectVersion, offset)
	}

	stmt, err := tx.Prepare(fmt.Sprintf(`
		INSERT INTO "%s" (event_id, stream_type, stream_id, event_type, timestamp, stream_offset, global_offset, data)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, s.tableName))
	if err != nil {
		return fmt.Errorf("preparing statement: %w", err)
	}
	defer func() { _ = stmt.Close() }()

	now := time.Now().UTC()
	fullEvents := make([]*eventstore.Event, len(events))
	for i, we := range events {
		if we.Timestamp.IsZero() {
			we.Timestamp = now
		}

		fullEvents[i] = &eventstore.Event{
			ID:            we.ID,
			StreamID:      streamID,
			StreamVersion: offset + int64(i) + 1,
			Timestamp:     we.Timestamp,
			Data:          we.Data,
		}

		_, err := stmt.Exec(
			fullEvents[i].ID.Value(),
			fullEvents[i].StreamID.TypeName(),
			fullEvents[i].StreamID.Value(),
			fullEvents[i].ID.TypeName(),
			fullEvents[i].Timestamp,
			fullEvents[i].StreamVersion,
			globalOffset+int64(i)+1,
			fullEvents[i].Data,
		)
		if err != nil {
			return fmt.Errorf("executing statement: %w", err)
		}
	}

	for _, hook := range s.appendTxHooks {
		if err := hook(tx, fullEvents); err != nil {
			return fmt.Errorf("executing transaction hook: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	return nil
}

// Creates the events table if it does not already exist.
func (s *EventStore) ensureEventsTable(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id BIGSERIAL PRIMARY KEY,
			stream_id uuid NOT NULL,
			stream_type varchar(255) NOT NULL,
			event_id uuid NOT NULL,
			event_type varchar(255) NOT NULL,
			timestamp timestamptz NOT NULL,
			stream_offset bigint NOT NULL,
			global_offset bigint NOT NULL,
			data bytea,

			UNIQUE (stream_id, stream_type, stream_offset),
			UNIQUE (global_offset),

			CHECK (stream_offset > 0),
			CHECK (global_offset > 0)
		)
	`, s.tableName))
	return err
}

// Finds the highest offset for the given stream.
func (s *EventStore) getHighestOffset(ctx context.Context, tx Transaction, streamID typeid.UUID) (int64, error) {
	s.log.Debug("finding highest offset for stream", "stream_id", streamID)
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

	s.log.Debug("got highest offset for stream", "stream_id", streamID, "offset", offset)
	return offset, nil
}

// Finds the highest global offset among all events in the event store.
func (s *EventStore) getHighestGlobalOffset(ctx context.Context, tx Transaction) (int64, error) {
	s.log.Debug("finding highest global offset in event store")
	var offset int64
	if err := tx.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT global_offset
		FROM "%s"
		ORDER BY global_offset DESC
		LIMIT 1
	`, s.tableName)).Scan(&offset); errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	} else if err != nil {
		return 0, fmt.Errorf("querying highest global offset: %w", err)
	}

	s.log.Debug("got highest global offset in event store", "offset", offset)
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
