package eventstore

import (
	"errors"
	"log/slog"
)

type EventStoreOption func(*EventStore) error

// WithLogger sets the logger to use for the event store.
//
// The default logger is slog.Default().
func WithLogger(logger *slog.Logger) EventStoreOption {
	return func(s *EventStore) error {
		if logger == nil {
			return errors.New("logger cannot be nil")
		}

		s.log = logger
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
