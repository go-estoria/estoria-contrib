package eventstore

import (
	"database/sql"
	"errors"

	"github.com/go-estoria/estoria"
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

// WithStrategy sets the strategy to use for the event store.
func WithStrategy(strategy Strategy) EventStoreOption {
	return func(s *EventStore) error {
		s.strategy = strategy
		return nil
	}
}

// WithTransactionalHook adds a transactional hook to the event store.
func WithTransactionalHooks(hooks ...TransactionHookFunc) EventStoreOption {
	return func(s *EventStore) error {
		s.AddTransactionalHooks(hooks...)
		return nil
	}
}

// WithTxOptions sets the transaction options to use for write transactions.
func WithTxOptions(opts sql.TxOptions) EventStoreOption {
	return func(s *EventStore) error {
		s.txOpts = &opts
		return nil
	}
}
