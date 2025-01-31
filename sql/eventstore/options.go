package eventstore

import (
	"errors"

	"github.com/go-estoria/estoria"
	"github.com/go-estoria/estoria-contrib/sql/outbox"
)

type EventStoreOption func(*EventStore) error

// WithLogger sets the logger to use for the event store.
//
// The default logger is estoria.DefaultLogger().
func WithLogger(logger estoria.Logger) EventStoreOption {
	return func(s *EventStore) error {
		if logger == nil {
			return errors.New("logger cannot be nil")
		}

		s.log = logger
		return nil
	}
}

// WithOutbox sets the outbox to use for the event store.
func WithOutbox(outbox *outbox.Outbox) EventStoreOption {
	return func(s *EventStore) error {
		s.AddTransactionalHook(outbox.HandleEvents)
		return nil
	}
}

// WithStrategy sets the strategy to use for the event store.
//
// The default strategy is one collection per stream.
func WithStrategy(strategy Strategy) EventStoreOption {
	return func(s *EventStore) error {
		if strategy == nil {
			return errors.New("strategy cannot be nil")
		}

		s.strategy = strategy
		return nil
	}
}
