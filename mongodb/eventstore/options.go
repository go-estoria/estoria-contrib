package eventstore

import (
	"errors"

	"github.com/go-estoria/estoria"
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

// WithDocumentMarshaler sets the marshaler to use for the event store.
//
// The default marshaler is DefaultMarshaler.
func WithDocumentMarshaler(marshaler DocumentMarshaler) EventStoreOption {
	return func(s *EventStore) error {
		if marshaler == nil {
			return errors.New("marshaler cannot be nil")
		}

		s.marshaler = marshaler
		return nil
	}
}

// WithTransactionHook adds a hook to be run within the same transaction when a batch of events is
// appended to the store. Hooks are run in the order they are added, after the events are written
// but before the transaction commits. If a hook returns an error, the append is aborted.
func WithTransactionHook(hook TransactionHook) EventStoreOption {
	return func(s *EventStore) error {
		if hook == nil {
			return errors.New("hook cannot be nil")
		}

		s.txHooks = append(s.txHooks, hook)
		return nil
	}
}

// WithDatabaseName overrides the MongoDB database name (default "estoria").
func WithDatabaseName(name string) EventStoreOption {
	return func(s *EventStore) error {
		if name == "" {
			return errors.New("database name cannot be empty")
		}

		s.dbName = name
		return nil
	}
}

// WithEventsCollectionName overrides the name of the events collection (default "events").
func WithEventsCollectionName(name string) EventStoreOption {
	return func(s *EventStore) error {
		if name == "" {
			return errors.New("events collection name cannot be empty")
		}

		s.eventsCollName = name
		return nil
	}
}

// WithStreamsCollectionName overrides the name of the streams metadata collection (default "streams").
func WithStreamsCollectionName(name string) EventStoreOption {
	return func(s *EventStore) error {
		if name == "" {
			return errors.New("streams collection name cannot be empty")
		}

		s.streamsCollName = name
		return nil
	}
}

// WithCountersCollectionName overrides the name of the counters collection (default "counters").
func WithCountersCollectionName(name string) EventStoreOption {
	return func(s *EventStore) error {
		if name == "" {
			return errors.New("counters collection name cannot be empty")
		}

		s.countersCollName = name
		return nil
	}
}

// WithSessionOptions overrides the MongoDB session options used when starting append sessions.
func WithSessionOptions(opts options.Lister[options.SessionOptions]) EventStoreOption {
	return func(s *EventStore) error {
		if opts == nil {
			return errors.New("session options cannot be nil")
		}

		s.sessOpts = opts
		return nil
	}
}

// WithTransactionOptions overrides the MongoDB transaction options used for appends.
//
// The default sets snapshot read concern, majority write concern, and primary read preference.
func WithTransactionOptions(opts options.Lister[options.TransactionOptions]) EventStoreOption {
	return func(s *EventStore) error {
		if opts == nil {
			return errors.New("transaction options cannot be nil")
		}

		s.txOpts = opts
		return nil
	}
}

// WithAutoEnsureIndexes causes New to call EnsureIndexes automatically. It is off by default, for
// parity with the explicit Postgres Schema() flow.
func WithAutoEnsureIndexes() EventStoreOption {
	return func(s *EventStore) error {
		s.autoEnsureIdx = true
		return nil
	}
}
