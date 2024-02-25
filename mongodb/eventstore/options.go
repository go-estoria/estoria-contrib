package eventstore

import (
	"errors"
	"log/slog"
)

type EventStoreOption func(*EventStore) error

// WithDatabaseName sets the database name to use for the event store.
//
// The default database name is "eventstore".
func WithDatabaseName(name string) EventStoreOption {
	return func(s *EventStore) error {
		if name == "" {
			return errors.New("database name cannot be empty")
		}

		s.databaseName = name
		return nil
	}
}

// WithEventsCollectionName sets the collection name to use for the events.
//
// The default collection name is "events".
func WithEventsCollectionName(name string) EventStoreOption {
	return func(s *EventStore) error {
		if name == "" {
			return errors.New("events collection name cannot be empty")
		}

		s.eventsCollectionName = name
		return nil
	}
}

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
