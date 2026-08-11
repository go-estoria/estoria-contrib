package eventstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-estoria/estoria"
	"github.com/go-estoria/estoria-contrib/postgres/eventstore/strategy"
	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Strategy is an interface for defining lower-level query and append mechanics.
//
// Strategies define the specific SQL schema and behavior to use when storing and retrieving events
// from the database. Different strategies may be used to support different database schemas or
// optimizations, such as per-stream event tables or high-throughput append strategies.
type Strategy interface {
	// ReadStreamQuery builds a query for reading events from a stream.
	ReadStreamQuery(streamID typeid.ID, opts eventstore.ReadStreamOptions) (string, []any, error)

	// ScanEventRow scans a single event row from the provided pgx.Rows.
	ScanEventRow(rows pgx.Rows) (*eventstore.Event, error)

	// NextHighwaterMark returns the next highwater mark (i.e. the next highest stream version).
	NextHighwaterMark(ctx context.Context, tx pgx.Tx, streamID typeid.ID, numEvents int) (int64, error)

	// AppendStreamStatement returns a SQL statement for appending events to a stream.
	AppendStreamStatement() (string, error)

	// AppendStreamExecArgs returns the arguments to pass when executing an append statement for an individual event.
	AppendStreamExecArgs(event *eventstore.Event) []any

	// Schema returns the SQL schema used by a strategy.
	Schema() string
}

// EventStore stores and retrieves events using Postgres as the underlying storage.
type EventStore struct {
	pool              *pgxpool.Pool
	strategy          Strategy
	log               estoria.Logger
	txOpts            pgx.TxOptions
	appendTxHooks     []TransactionHook
	maxEventDataBytes int
}

var (
	_ eventstore.StreamReader = (*EventStore)(nil)
	_ eventstore.StreamWriter = (*EventStore)(nil)
	_ eventstore.GlobalReader = (*EventStore)(nil)
)

// A TransactionHook is invoked during a write transaction, after the events have been written,
// and receives both the transactional context and the full set of events pending insertion in
// the transaction.
//
// If an error is returned, the entire append transaction will be aborted.
//
// Transaction hooks can be used to perform post-processing of events that must succeed or fail atomically
// with the event append operation, such as inserting items into to an outbox table.
type TransactionHook interface {
	HandleEvents(ctx context.Context, tx pgx.Tx, events []*eventstore.Event) error
}

// TransactionHookFunc is a functional adapter for TransactionHook.
type TransactionHookFunc func(ctx context.Context, tx pgx.Tx, events []*eventstore.Event) error

// HandleEvents implements TransactionHook.HandleEvents.
func (f TransactionHookFunc) HandleEvents(ctx context.Context, tx pgx.Tx, events []*eventstore.Event) error {
	return f(ctx, tx, events)
}

// New creates a new event store using the provided pgx connection pool.
func New(pool *pgxpool.Pool, opts ...EventStoreOption) (*EventStore, error) {
	if pool == nil {
		return nil, errors.New("pool is required")
	}

	eventStore := &EventStore{
		pool: pool,
		log:  estoria.GetLogger().WithGroup("eventstore"),
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
	if streamID.Type == "" {
		return nil, errors.New("stream type is required")
	}

	s.log.Debug("reading events from Postgres stream",
		"stream_id", streamID.String(),
		"after_version", opts.AfterVersion,
		"count", opts.Count,
		"direction", opts.Direction,
	)

	query, args, err := s.strategy.ReadStreamQuery(streamID, opts)
	if err != nil {
		return nil, fmt.Errorf("building query: %w", err)
	}

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("querying stream events: %w", err)
	}

	// No rows has two meanings: the stream holds no events at all, or the read was filtered
	// and nothing matched. Only the first is ErrStreamNotFound.
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, fmt.Errorf("preparing stream events results: %w", err)
		}
		rows.Close()

		// An unfiltered read that matched nothing saw the whole stream: it is absent.
		if opts.AfterVersion == 0 {
			return nil, eventstore.ErrStreamNotFound
		}

		exists, err := s.streamExists(ctx, streamID)
		if err != nil {
			return nil, err
		} else if !exists {
			return nil, eventstore.ErrStreamNotFound
		}

		return emptyStreamIterator{}, nil
	}

	// calling .Next() advanced the cursor, so scan the first row now
	first, err := s.strategy.ScanEventRow(rows)
	if err != nil {
		rows.Close()
		return nil, fmt.Errorf("scanning event row: %w", err)
	}

	return &streamIterator{
		strategy: s.strategy,
		rows:     rows,
		first:    first,
	}, nil
}

// AppendStream appends events to the specified stream.
func (s *EventStore) AppendStream(ctx context.Context, streamID typeid.ID, events []*eventstore.WritableEvent, opts eventstore.AppendStreamOptions) (_ []*eventstore.Event, retErr error) {
	if len(events) == 0 {
		return nil, nil
	}

	if streamID.Type == "" {
		return nil, errors.New("stream type is required")
	}

	if s.maxEventDataBytes > 0 {
		for i, we := range events {
			if len(we.Data) > s.maxEventDataBytes {
				return nil, fmt.Errorf("event %d data size %d exceeds maximum of %d bytes", i, len(we.Data), s.maxEventDataBytes)
			}
		}
	}

	s.log.Debug("appending events to Postgres stream", "stream_id", streamID.String(), "events", len(events))

	tx, err := s.pool.BeginTx(ctx, s.txOpts)
	if err != nil {
		return nil, fmt.Errorf("beginning transaction: %w", err)
	}

	defer func() {
		if retErr != nil {
			if rollbackErr := tx.Rollback(ctx); rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
				s.log.Error("error rolling back transaction", "error", rollbackErr, "cause", retErr)
			}
		}
	}()

	if opts.ExpectVersion != nil && opts.StreamMustNotExist {
		return nil, errors.New("ExpectVersion and StreamMustNotExist are mutually exclusive")
	}

	newMaxOffset, err := s.strategy.NextHighwaterMark(ctx, tx, streamID, len(events))
	if err != nil {
		return nil, fmt.Errorf("getting highest offset: %w", err)
	}

	currentOffset := newMaxOffset - int64(len(events))

	if opts.StreamMustNotExist && currentOffset > 0 {
		return nil, eventstore.StreamVersionMismatchError{
			StreamID:        streamID,
			ExpectedVersion: 0,
			ActualVersion:   currentOffset,
		}
	}

	if opts.ExpectVersion != nil && *opts.ExpectVersion != currentOffset {
		return nil, eventstore.StreamVersionMismatchError{
			StreamID:        streamID,
			ExpectedVersion: *opts.ExpectVersion,
			ActualVersion:   currentOffset,
		}
	}

	stmtQuery, err := s.strategy.AppendStreamStatement()
	if err != nil {
		return nil, fmt.Errorf("building append statement: %w", err)
	}

	// Postgres timestamptz holds microseconds; truncate so the returned events
	// carry the timestamp a subsequent read yields.
	now := time.Now().UTC().Truncate(time.Microsecond)

	fullEvents := make([]*eventstore.Event, len(events))
	for i, we := range events {
		fullEvents[i] = &eventstore.Event{
			ID:              typeid.NewV4(we.Type),
			StreamID:        streamID,
			StreamVersion:   currentOffset + int64(i) + 1,
			Timestamp:       now,
			Data:            we.Data,
			DataContentType: we.DataContentType,
			Metadata:        we.Metadata,
		}

		var globalPos int64
		if err := tx.QueryRow(ctx, stmtQuery, s.strategy.AppendStreamExecArgs(fullEvents[i])...).Scan(&globalPos); err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "23505" {
				// The only unique constraint on the events table is (stream_id, stream_type, stream_offset).
				// A violation here means a concurrent writer inserted at the same offset.
				// When a concurrent write races with this one, we report currentOffset as both
				// expected and actual: we don't know the true actual version from the DB, and
				// reporting ExpectedVersion=0 when no explicit ExpectVersion was specified would
				// be misleading.
				return nil, eventstore.StreamVersionMismatchError{
					StreamID:        streamID,
					ExpectedVersion: currentOffset,
					ActualVersion:   currentOffset,
				}
			}
			return nil, fmt.Errorf("executing statement: %w", err)
		}
		fullEvents[i].GlobalPosition = &globalPos
	}

	for _, hook := range s.appendTxHooks {
		if err := hook.HandleEvents(ctx, tx, fullEvents); err != nil {
			return nil, fmt.Errorf("executing transaction hook: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("committing transaction: %w", err)
	}

	return fullEvents, nil
}

// StreamLister is an interface for strategies that support listing streams.
type StreamLister interface {
	ListStreams(ctx context.Context, pool *pgxpool.Pool) ([]strategy.StreamMetadata, error)
}

// ListStreams returns info for all streams in the event store.
//
// Note that not all strategies may support listing streams, in which case an error will be returned.
func (s *EventStore) ListStreams(ctx context.Context) ([]strategy.StreamMetadata, error) {
	lister, ok := s.strategy.(StreamLister)
	if !ok {
		return nil, errors.New("strategy does not support listing streams")
	}

	return lister.ListStreams(ctx, s.pool)
}

// StreamExistenceChecker is an interface for strategies that can report whether a stream
// exists independently of any read filter.
type StreamExistenceChecker interface {
	StreamExists(ctx context.Context, pool *pgxpool.Pool, streamID typeid.ID) (bool, error)
}

// streamExists reports whether the given stream exists.
//
// Strategies that do not implement StreamExistenceChecker get the conservative answer,
// true: a filtered read matching no events is not evidence that the stream is absent, so
// ReadStream returns an empty iterator rather than claiming a not-found it cannot prove.
func (s *EventStore) streamExists(ctx context.Context, streamID typeid.ID) (bool, error) {
	checker, ok := s.strategy.(StreamExistenceChecker)
	if !ok {
		return true, nil
	}

	exists, err := checker.StreamExists(ctx, s.pool, streamID)
	if err != nil {
		return false, fmt.Errorf("checking whether stream exists: %w", err)
	}

	return exists, nil
}

// AllReader is an interface for strategies that support reading all events across all streams.
type AllReader interface {
	ReadAll(context.Context, *pgxpool.Pool, eventstore.ReadAllOptions) (pgx.Rows, error)
}

// ReadAll creates an iterator over events from all streams in ascending global order,
// implementing eventstore.GlobalReader. Global positions are values of the events table's
// auto-incrementing id column: gaps can occur, repeats cannot. A read with nothing to
// yield returns an empty iterator rather than an error; strategies that do not implement
// AllReader return an error.
func (s *EventStore) ReadAll(ctx context.Context, opts eventstore.ReadAllOptions) (eventstore.StreamIterator, error) {
	reader, ok := s.strategy.(AllReader)
	if !ok {
		return nil, errors.New("strategy does not support reading all events")
	}

	rows, err := reader.ReadAll(ctx, s.pool, opts)
	if err != nil {
		return nil, fmt.Errorf("reading all events: %w", err)
	}

	// no rows means there are no events; return an empty iterator
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, fmt.Errorf("preparing all events results: %w", err)
		}
		rows.Close()
		return emptyStreamIterator{}, nil
	}

	// calling .Next() advanced the cursor, so scan the first row now
	first, err := s.strategy.ScanEventRow(rows)
	if err != nil {
		rows.Close()
		return nil, fmt.Errorf("scanning event row: %w", err)
	}

	return &streamIterator{
		strategy: s.strategy,
		rows:     rows,
		first:    first,
	}, nil
}
