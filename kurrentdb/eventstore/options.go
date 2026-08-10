package eventstore

import (
	"errors"
	"strings"

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

// WithStreamPrefix namespaces the store's underlying KurrentDB stream names: a stream ID
// is stored as "<prefix>.<id>", and global reads yield only streams carrying the prefix.
// KurrentDB has no databases, so a prefix is the isolation available to stores sharing a
// node. The isolation is one-way: prefixed stores never see each other's streams, but an
// unprefixed store's global reads yield every stream whose name parses as a stream ID,
// prefixed ones included. An existing store's prefix cannot change without orphaning its
// streams.
//
// By default, stream names carry no prefix.
func WithStreamPrefix(prefix string) EventStoreOption {
	return func(s *EventStore) error {
		if prefix == "" {
			return errors.New("stream prefix cannot be empty")
		} else if strings.HasPrefix(prefix, "$") {
			return errors.New("stream prefix cannot begin with '$'")
		}

		s.streamPrefix = prefix
		return nil
	}
}

// WithReadAllWindowSize overrides how many raw $all records ReadAll fetches per server
// read. Smaller windows fetch less per read; larger windows make fewer reads.
//
// The default is 1024.
func WithReadAllWindowSize(size int64) EventStoreOption {
	return func(s *EventStore) error {
		if size < 1 {
			return errors.New("window size must be positive")
		}

		s.readAllWindowSize = size
		return nil
	}
}
