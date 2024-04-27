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

// WithTransactionHook adds a hook to be run within the same transaction when
// a batch of events is appended to the store. The hooks are run in the order
// they are added, and are run after the events are appended to the store.
func WithTransactionHook(hook TransactionHook) EventStoreOption {
	return func(s *EventStore) error {
		if hook == nil {
			return errors.New("hook cannot be nil")
		}

		s.appendTxHooks = append(s.appendTxHooks, hook)
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
