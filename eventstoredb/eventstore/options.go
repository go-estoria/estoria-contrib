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
