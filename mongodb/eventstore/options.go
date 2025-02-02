package eventstore

import (
	"errors"

	"github.com/go-estoria/estoria"
	"github.com/go-estoria/estoria-contrib/mongodb/eventstore/strategy"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
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
//
// By default, DefaultSessionOptions() is used.
func WithSessionOptions(opts *options.SessionOptionsBuilder) EventStoreOption {
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
// The default strategy is to use a database named "estoria"
// with a single collection named "events" for all events.
func WithStrategy(strategy Strategy) EventStoreOption {
	return func(s *EventStore) error {
		if strategy == nil {
			return errors.New("strategy cannot be nil")
		}

		s.strategy = strategy
		return nil
	}
}

func WithDocumentMarshaler(marshaler strategy.DocumentMarshaler) EventStoreOption {
	return func(s *EventStore) error {
		if marshaler == nil {
			return errors.New("marshaler cannot be nil")
		}

		s.marshaler = marshaler
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
//
// By default, DefaultTransactionOptions() is used.
func WithTransactionOptions(opts *options.TransactionOptionsBuilder) EventStoreOption {
	return func(s *EventStore) error {
		if opts == nil {
			return errors.New("transaction options cannot be nil")
		}

		s.txOptions = opts
		return nil
	}
}
