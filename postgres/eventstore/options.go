package eventstore

import (
	"database/sql"
	"errors"

	"github.com/go-estoria/estoria"
)

// EventStoreOption is a functional option for configuring an EventStore.
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
//
// Strategies define the specific SQL schema and behavior to use when storing and retrieving events
// from the database. Different strategies may be used to support different database schemas or
// optimizations, such as per-stream event tables or high-throughput append strategies.
//
// The default strategy is a DefaultStrategy.
func WithStrategy(strategy Strategy) EventStoreOption {
	return func(s *EventStore) error {
		s.strategy = strategy
		return nil
	}
}

// WithTransactionalHooks adds one or more hooks to be executed during the database transaction when appending events.
//
// Hooks are executed in the order they were added, after the events have been inserted. Each hook receives the
// full set of events that have been inserted for the transaction, including their stream offsets.
//
// If an error is returned from any hook, the transaction will be aborted. Thus, it is not safe to perform
// operations that cannot be rolled back within a hook, such as publishing messages to an external message bus
// or making irreversible changes to external systems.
func WithAppendTransactionHooks(hooks ...TransactionHook) EventStoreOption {
	return func(s *EventStore) error {
		s.appendTxHooks = append(s.appendTxHooks, hooks...)
		return nil
	}
}

// WithTxOptions sets optional transaction options to use for write transactions.
func WithTxOptions(opts *sql.TxOptions) EventStoreOption {
	return func(s *EventStore) error {
		s.txOpts = opts
		return nil
	}
}
