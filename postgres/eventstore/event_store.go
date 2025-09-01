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
type Strategy interface {
	AppendStreamExecArgs(event *eventstore.Event) []any
	AppendStreamStatement(ids []typeid.UUID) (string, error)
	NextHighwaterMark(ctx context.Context, tx *sql.Tx, streamID typeid.UUID, numEvents int) (int64, error)
	ReadStreamQuery(streamID typeid.UUID, opts eventstore.ReadStreamOptions) (string, []any, error)
	ScanEventRow(rows *sql.Rows) (*eventstore.Event, error)
}

// EventStore stores and retrieves events using Postgres as the underlying storage.
type EventStore struct {
	db            *sql.DB
	strategy      Strategy
	log           estoria.Logger
	appendTxHooks []TransactionHook
}

var _ eventstore.StreamReader = (*EventStore)(nil)
var _ eventstore.StreamWriter = (*EventStore)(nil)

// TransactionHook is invoked during a write transaction and receives the transactional context.
type TransactionHook func(tx *sql.Tx, events []*eventstore.Event) error

// New creates a new event store using the given database connection.
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
func (s *EventStore) ReadStream(ctx context.Context, streamID typeid.UUID, opts eventstore.ReadStreamOptions) (eventstore.StreamIterator, error) {
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

	return &streamIterator{
		strategy: s.strategy,
		rows:     rows,
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

	newMaxOffset, err := s.strategy.NextHighwaterMark(ctx, tx, streamID, len(events))
	if err != nil {
		return fmt.Errorf("getting highest offset: %w", err)
	}

	currentOffset := newMaxOffset - int64(len(events))
	if opts.ExpectVersion > 0 && currentOffset != opts.ExpectVersion {
		return fmt.Errorf("expected offset %d, but stream has offset %d", opts.ExpectVersion, currentOffset)
	}

	ids := make([]typeid.UUID, len(events))
	for i, e := range events {
		ids[i] = e.ID
	}

	stmtQuery, err := s.strategy.AppendStreamStatement(ids)
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
		if we.Timestamp.IsZero() {
			we.Timestamp = now
		}

		fullEvents[i] = &eventstore.Event{
			ID:            we.ID,
			StreamID:      streamID,
			StreamVersion: currentOffset + int64(i) + 1,
			Timestamp:     we.Timestamp,
			Data:          we.Data,
		}

		_, err := stmt.Exec(s.strategy.AppendStreamExecArgs(fullEvents[i])...)
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

// AddTransactionalHook adds a hook to be executed within the transaction when appending events.
// If an error is returned from any hook, the transaction will be aborted.
func (s *EventStore) AddTransactionalHooks(hooks ...TransactionHook) {
	s.appendTxHooks = append(s.appendTxHooks, hooks...)
}
