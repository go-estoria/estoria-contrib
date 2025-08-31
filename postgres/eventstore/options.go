package eventstore

import (
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

// // WithOutbox sets the outbox to use for the event store.
// func WithOutbox(outbox *outbox.Outbox) EventStoreOption {
// 	return func(s *EventStore) error {
// 		s.AddTransactionalHooks(outbox.HandleEvents)
// 		return nil
// 	}
// }

// WithTableName sets a custom table name for the event store.
// The table name must be a valid SQL identifier.
func WithTableName(tableName string) EventStoreOption {
	return func(s *EventStore) error {
		if tableName == "" {
			return errors.New("table name cannot be empty")
		}
		s.tableName = tableName
		return nil
	}
}

// WithTransactionalHook adds a transactional hook to the event store.
func WithTransactionalHooks(hooks ...TransactionHook) EventStoreOption {
	return func(s *EventStore) error {
		s.AddTransactionalHooks(hooks...)
		return nil
	}
}
