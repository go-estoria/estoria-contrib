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

type SQLDatabase interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

type EventStore struct {
	db            SQLDatabase
	strategy      Strategy
	log           estoria.Logger
	appendTxHooks []TransactionHook
}

type Event struct {
	eventstore.Event
	GlobalOffset int64
}

var _ eventstore.StreamReader = (*EventStore)(nil)
var _ eventstore.StreamWriter = (*EventStore)(nil)

type TransactionHook func(tx *sql.Tx, events []*eventstore.Event) error

type Strategy interface {
	// ExecuteInsertTransaction executes the given function within a new session suitable for inserting events.
	// The function is executed within a transaction and is invoked with a session context, a collection,
	// the current offset of the stream, and the global offset.
	ExecuteInsertTransaction(
		ctx context.Context,
		streamID typeid.UUID,
		inTxnFn func(txn *sql.Tx, table string, offset int64, globalOffset int64) ([]sql.Result, error),
	) error
	GetStreamRows(
		ctx context.Context,
		streamID typeid.UUID,
		opts eventstore.ReadStreamOptions,
	) (*sql.Rows, error)
}

// New creates a new event store using the given database connection.
func New(db SQLDatabase, opts ...EventStoreOption) (*EventStore, error) {
	eventStore := &EventStore{
		db: db,
	}

	for _, opt := range opts {
		if err := opt(eventStore); err != nil {
			return nil, fmt.Errorf("applying option: %w", err)
		}
	}

	if eventStore.log == nil {
		eventStore.log = estoria.GetLogger().WithGroup("eventstore")
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
	s.log.Debug("reading events from Postgres stream",
		"stream_id", streamID.String(),
		"offset", opts.Offset,
		"count", opts.Count,
		"direction", opts.Direction,
	)

	rows, err := s.strategy.GetStreamRows(ctx, streamID, opts)
	if err != nil {
		return nil, fmt.Errorf("getting stream cursor: %w", err)
	}

	return &streamIterator{
		streamID: streamID,
		rows:     rows,
	}, nil
}

// AppendStream appends events to the specified stream.
func (s *EventStore) AppendStream(ctx context.Context, streamID typeid.UUID, events []*eventstore.WritableEvent, opts eventstore.AppendStreamOptions) error {
	s.log.Debug("appending events to Postgres stream", "stream_id", streamID.String(), "events", len(events))

	return s.strategy.ExecuteInsertTransaction(ctx, streamID,
		func(txn *sql.Tx, table string, offset int64, globalOffset int64) ([]sql.Result, error) {
			if opts.ExpectVersion > 0 && offset != opts.ExpectVersion {
				return nil, fmt.Errorf("expected offset %d, but stream has offset %d", opts.ExpectVersion, offset)
			}

			stmt, err := txn.Prepare(fmt.Sprintf(`
				INSERT INTO "%s" (event_id, stream_type, stream_id, event_type, timestamp, stream_offset, global_offset, data)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			`, table))
			if err != nil {
				return nil, fmt.Errorf("preparing statement: %w", err)
			}
			defer stmt.Close()

			now := time.Now()
			results := make([]sql.Result, len(events))
			for i, we := range events {
				if we.Timestamp.IsZero() {
					we.Timestamp = now
				}

				res, err := stmt.Exec(
					we.ID.Value(),
					streamID.TypeName(),
					streamID.Value(),
					we.ID.TypeName(),
					we.Timestamp,
					offset+int64(i)+1,
					globalOffset+int64(i)+1,
					we.Data,
				)
				if err != nil {
					return nil, fmt.Errorf("executing statement: %w", err)
				}

				results[i] = res
			}

			return results, nil
		},
	)
}
