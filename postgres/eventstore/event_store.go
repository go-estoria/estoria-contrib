package eventstore

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/go-estoria/estoria"
	"github.com/go-estoria/estoria-contrib/postgres/eventstore/strategy"
	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
)

// Strategy is an interface for defining lower-level query and append mechanics.
//
// Strategies define the specific SQL schema and behavior to use when storing and retrieving events
// from the database. Different strategies may be used to support different database schemas or
// optimizations, such as per-stream event tables or high-throughput append strategies.
type Strategy interface {
	// ReadStreamQuery builds a query for reading events from a stream.
	ReadStreamQuery(streamID typeid.ID, opts eventstore.ReadStreamOptions) (string, []any, error)

	// ScanEventRow scans a single event row from the provided sql.Rows.
	ScanEventRow(rows *sql.Rows) (*eventstore.Event, error)

	// NextHighwaterMark returns the next highwater mark (i.e. the next highest stream version).
	NextHighwaterMark(ctx context.Context, tx *sql.Tx, streamID typeid.ID, numEvents int) (int64, error)

	// AppendStreamStatement returns a SQL statement for appending events to a stream.
	AppendStreamStatement() (string, error)

	// AppendStreamExecArgs returns the arguments to pass when executing an append statement for an individual event.
	AppendStreamExecArgs(event *eventstore.Event) []any

	// Schema returns the SQL schema used by a strategy.
	Schema() string
}

// EventStore stores and retrieves events using Postgres as the underlying storage.
type EventStore struct {
	db            *sql.DB
	strategy      Strategy
	log           estoria.Logger
	txOpts        *sql.TxOptions
	appendTxHooks []TransactionHook
}

var _ eventstore.StreamReader = (*EventStore)(nil)
var _ eventstore.StreamWriter = (*EventStore)(nil)

// A TransactionHook is invoked during a write transaction, after the events have been writen,
// and receives both the transactional context and the full set of events pending insertion in
// the transaction.
//
// If an error is returned, the entire append transaction will be aborted.
//
// Transaction hooks can be used to perform post-processing of events that must succeed or fail atomically
// with the event append operation, such as inserting items into to an outbox table.
type TransactionHook interface {
	HandleEvents(ctx context.Context, tx *sql.Tx, events []*eventstore.Event) error
}

// TransactionHookFunc is a functional adapter for TransactionHook.
type TransactionHookFunc func(ctx context.Context, tx *sql.Tx, events []*eventstore.Event) error

// HandleEvents implements TransactionHook.HandleEvents.
func (f TransactionHookFunc) HandleEvents(ctx context.Context, tx *sql.Tx, events []*eventstore.Event) error {
	return f(ctx, tx, events)
}

// New creates a new event store using the provided database connection.
func New(db *sql.DB, opts ...EventStoreOption) (*EventStore, error) {
	if db == nil {
		return nil, fmt.Errorf("database is required")
	}

	eventStore := &EventStore{
		db:  db,
		log: estoria.GetLogger().WithGroup("eventstore"),
	}

	for _, opt := range opts {
		if err := opt(eventStore); err != nil {
			return nil, fmt.Errorf("applying option: %w", err)
		}
	}

	if eventStore.strategy == nil {
		strategy, err := strategy.NewDefaultStrategy()
		if err != nil {
			return nil, fmt.Errorf("creating default strategy: %w", err)
		}

		eventStore.strategy = strategy
	}

	return eventStore, nil
}

// ReadStream returns an iterator for reading events from the specified stream.
func (s *EventStore) ReadStream(ctx context.Context, streamID typeid.ID, opts eventstore.ReadStreamOptions) (eventstore.StreamIterator, error) {
	s.log.Debug("reading events from Postgres stream",
		"stream_id", streamID.String(),
		"offset", opts.Offset,
		"count", opts.Count,
		"direction", opts.Direction,
	)

	query, args, err := s.strategy.ReadStreamQuery(streamID, opts)
	if err != nil {
		return nil, fmt.Errorf("building query: %w", err)
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("querying stream events: %w", err)
	}

	// no rows means the stream doesn't exist
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("preparing stream events results: %w", err)
		}

		return nil, eventstore.ErrStreamNotFound
	}

	// calling .Next() advanced the cursor, so scan the first row now
	first, err := s.strategy.ScanEventRow(rows)
	if err != nil {
		return nil, fmt.Errorf("scanning event row: %w", err)
	}

	return &streamIterator{
		strategy: s.strategy,
		rows:     rows,
		first:    first,
	}, nil
}

// AppendStream appends events to the specified stream.
func (s *EventStore) AppendStream(ctx context.Context, streamID typeid.ID, events []*eventstore.WritableEvent, opts eventstore.AppendStreamOptions) error {
	s.log.Debug("appending events to Postgres stream", "stream_id", streamID.String(), "events", len(events))

	tx, err := s.db.BeginTx(ctx, s.txOpts)
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

	newMaxOffset, err := s.strategy.NextHighwaterMark(ctx, tx, streamID, len(events))
	if err != nil {
		return fmt.Errorf("getting highest offset: %w", err)
	}

	currentOffset := newMaxOffset - int64(len(events))
	if opts.ExpectVersion > 0 && currentOffset != opts.ExpectVersion {
		return eventstore.StreamVersionMismatchError{
			StreamID:        streamID,
			ExpectedVersion: opts.ExpectVersion,
			ActualVersion:   currentOffset,
		}
	}

	stmtQuery, err := s.strategy.AppendStreamStatement()
	if err != nil {
		return fmt.Errorf("building append statement: %w", err)
	}

	stmt, err := tx.Prepare(stmtQuery)
	if err != nil {
		return fmt.Errorf("preparing statement: %w", err)
	}
	defer func() {
		if err := stmt.Close(); err != nil {
			s.log.Error("closing statement", "error", err)
		}
	}()

	now := time.Now().UTC()

	fullEvents := make([]*eventstore.Event, len(events))
	for i, we := range events {
		fullEvents[i] = &eventstore.Event{
			ID:            typeid.NewV4(we.Type),
			StreamID:      streamID,
			StreamVersion: currentOffset + int64(i) + 1,
			Timestamp:     now,
			Data:          we.Data,
		}

		if _, err := stmt.ExecContext(ctx, s.strategy.AppendStreamExecArgs(fullEvents[i])...); err != nil {
			return fmt.Errorf("executing statement: %w", err)
		}
	}

	for _, hook := range s.appendTxHooks {
		if err := hook.HandleEvents(ctx, tx, fullEvents); err != nil {
			return fmt.Errorf("executing transaction hook: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	return nil
}

// StreamLister is an interface for strategies that support listing streams.
type StreamLister interface {
	ListStreams(*sql.DB) ([]strategy.StreamMetadata, error)
}

// ListStreams returns info for all streams in the event store.
//
// Note that not all strategies may support listing streams, in which case an error will be returned.
func (s *EventStore) ListStreams() ([]strategy.StreamMetadata, error) {
	lister, ok := s.strategy.(StreamLister)
	if !ok {
		return nil, fmt.Errorf("strategy does not support listing streams")
	}

	return lister.ListStreams(s.db)
}

// AllReader is an interface for strategies that support reading all events across all streams.
type AllReader interface {
	ReadAll(context.Context, *sql.DB, eventstore.ReadStreamOptions) (*sql.Rows, error)
}

// ReadAll returns an iterator for reading all events in the event store, across all streams.
//
// Note that not all strategies may support reading all events, in which case an error will be returned.
func (s *EventStore) ReadAll(ctx context.Context, opts eventstore.ReadStreamOptions) (eventstore.StreamIterator, error) {
	reader, ok := s.strategy.(AllReader)
	if !ok {
		return nil, fmt.Errorf("strategy does not support reading all events")
	}

	rows, err := reader.ReadAll(ctx, s.db, opts)
	if err != nil {
		return nil, fmt.Errorf("reading all events: %w", err)
	}

	// no rows means there are no events
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("preparing all events results: %w", err)
		}

		return nil, eventstore.ErrStreamNotFound
	}

	// calling .Next() advanced the cursor, so scan the first row now
	first, err := s.strategy.ScanEventRow(rows)
	if err != nil {
		return nil, fmt.Errorf("scanning event row: %w", err)
	}

	return &streamIterator{
		strategy: s.strategy,
		rows:     rows,
		first:    first,
	}, nil
}
