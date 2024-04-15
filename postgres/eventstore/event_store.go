package eventstore

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/go-estoria/estoria"
	"github.com/go-estoria/estoria-contrib/postgres/eventstore/strategy"
	"go.jetpack.io/typeid"
)

type EventStore struct {
	db       *sql.DB
	strategy Strategy
	log      *slog.Logger
}

var _ estoria.EventStreamReader = (*EventStore)(nil)
var _ estoria.EventStreamWriter = (*EventStore)(nil)

type Strategy interface {
	GetStreamIterator(
		ctx context.Context,
		streamID typeid.AnyID,
		opts estoria.ReadStreamOptions,
	) (estoria.EventStreamIterator, error)
	InsertStreamEvents(
		tx *sql.Tx,
		streamID typeid.AnyID,
		events []estoria.Event,
		opts estoria.AppendStreamOptions,
	) (sql.Result, error)
}

// NewEventStore creates a new event store using the given database connection.
func NewEventStore(db *sql.DB, opts ...EventStoreOption) (*EventStore, error) {
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

// ReadStream returns an iterator for reading events from the specified stream.
func (s *EventStore) ReadStream(ctx context.Context, streamID typeid.AnyID, opts estoria.ReadStreamOptions) (estoria.EventStreamIterator, error) {
	s.log.Debug("reading events from Postgres stream", "stream_id", streamID.String())

	iter, err := s.strategy.GetStreamIterator(ctx, streamID, opts)
	if err != nil {
		return nil, fmt.Errorf("getting stream cursor: %w", err)
	}

	return iter, nil
}

// AppendStream appends events to the specified stream.
func (s *EventStore) AppendStream(ctx context.Context, streamID typeid.AnyID, opts estoria.AppendStreamOptions, events ...estoria.Event) error {
	s.log.Debug("appending events to Postgres stream", "stream_id", streamID.String(), "events", len(events))

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}

	s.log.Debug("inserting events", "events", len(events))
	if _, err = s.strategy.InsertStreamEvents(tx, streamID, events, opts); err != nil {
		return fmt.Errorf("inserting events: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	return nil
}
