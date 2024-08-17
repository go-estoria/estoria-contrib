package eventstore

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/go-estoria/estoria-contrib/postgres/eventstore/strategy"
	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
)

type SQLDB interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}

type EventStore struct {
	db            SQLDB
	strategy      Strategy
	log           *slog.Logger
	appendTxHooks []TransactionHook
}

var _ eventstore.StreamReader = (*EventStore)(nil)
var _ eventstore.StreamWriter = (*EventStore)(nil)

type TransactionHook func(tx *sql.Tx, events []*eventstore.Event) error

type Strategy interface {
	GetStreamIterator(
		ctx context.Context,
		streamID typeid.UUID,
		opts eventstore.ReadStreamOptions,
	) (eventstore.StreamIterator, error)
	InsertStreamEvents(
		tx strategy.SQLTx,
		streamID typeid.UUID,
		events []*eventstore.Event,
		opts eventstore.AppendStreamOptions,
	) (sql.Result, error)
}

// NewEventStore creates a new event store using the given database connection.
func NewEventStore(db SQLDB, opts ...EventStoreOption) (*EventStore, error) {
	eventStore := &EventStore{
		db: db,
	}

	for _, opt := range opts {
		if err := opt(eventStore); err != nil {
			return nil, fmt.Errorf("applying option: %w", err)
		}
	}

	if eventStore.log == nil {
		eventStore.log = slog.Default().WithGroup("eventstore")
	}

	if eventStore.strategy == nil {
		strat, err := strategy.NewSingleTableStrategy(db, "events")
		if err != nil {
			return nil, fmt.Errorf("creating default strategy: %w", err)
		}

		eventStore.strategy = strat
	}

	return eventStore, nil
}

// AddTransactionalHook adds a hook to be executed within the transaction when appending events.
// If an error is returned from any hook, the transaction will be aborted.
func (s *EventStore) AddTransactionalHook(hook TransactionHook) {
	s.appendTxHooks = append(s.appendTxHooks, hook)
}

// ReadStream returns an iterator for reading events from the specified stream.
func (s *EventStore) ReadStream(ctx context.Context, streamID typeid.UUID, opts eventstore.ReadStreamOptions) (eventstore.StreamIterator, error) {
	s.log.Debug("reading events from Postgres stream", "stream_id", streamID.String())

	iter, err := s.strategy.GetStreamIterator(ctx, streamID, opts)
	if err != nil {
		return nil, fmt.Errorf("getting stream cursor: %w", err)
	}

	return iter, nil
}

// AppendStream appends events to the specified stream.
func (s *EventStore) AppendStream(ctx context.Context, streamID typeid.UUID, events []*eventstore.Event, opts eventstore.AppendStreamOptions) error {
	s.log.Debug("appending events to Postgres stream", "stream_id", streamID.String(), "events", len(events))

	_, txErr := doInTransaction(ctx, s.db, func(tx *sql.Tx) (sql.Result, error) {
		s.log.Debug("inserting events", "events", len(events))
		if _, err := s.strategy.InsertStreamEvents(tx, streamID, events, opts); err != nil {
			return nil, fmt.Errorf("inserting events: %w", err)
		}

		for _, hook := range s.appendTxHooks {
			if err := hook(tx, events); err != nil {
				return nil, fmt.Errorf("executing transaction hook: %w", err)
			}
		}

		return nil, nil
	})
	if txErr != nil {
		return fmt.Errorf("inserting events: %w", txErr)
	}

	return nil
}

// Executes the given function within a transaction.
func doInTransaction(ctx context.Context, db SQLDB, f func(tx *sql.Tx) (sql.Result, error)) (any, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("beginning transaction: %w", err)
	}

	result, err := f(tx)
	if err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return nil, fmt.Errorf("rolling back transaction: %s (original transaction error: %w)", rollbackErr, err)
		}

		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("committing transaction: %w", err)
	}

	return result, nil
}
