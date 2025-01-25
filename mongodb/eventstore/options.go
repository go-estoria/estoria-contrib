package eventstore

import (
	"errors"

	"github.com/go-estoria/estoria"
	"go.mongodb.org/mongo-driver/mongo/options"
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

// WithSessionOptions overrides the default session options used when starting
// a new MongoDB session.
func WithSessionOptions(opts *options.SessionOptions) EventStoreOption {
	return func(s *EventStore) error {
		if opts == nil {
			return errors.New("session options cannot be nil")
		}

		s.sessionOptions = opts
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

// WithTransactionHook adds a hook to be run within the same transaction when
// a batch of events is appended to the store. The hooks are run in the order
// they are added, and are run after the events are appended to the store.
func WithTransactionHook(hook TransactionHook) EventStoreOption {
	return func(s *EventStore) error {
		if hook == nil {
			return errors.New("hook cannot be nil")
		}

		s.txHooks = append(s.txHooks, hook)
		return nil
	}
}

// WithTransactionOptions overrides the default transaction options used when
// starting a new MongoDB transaction on a session.
func WithTransactionOptions(opts *options.TransactionOptions) EventStoreOption {
	return func(s *EventStore) error {
		if opts == nil {
			return errors.New("transaction options cannot be nil")
		}

		s.txOptions = opts
		return nil
	}
}
