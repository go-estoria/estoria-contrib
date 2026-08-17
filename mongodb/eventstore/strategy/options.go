package strategy

import (
	"errors"

	"github.com/go-estoria/estoria"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type strategyConfig struct {
	log                   estoria.Logger
	sessOpts              *options.SessionOptionsBuilder
	txOpts                *options.TransactionOptionsBuilder
	eventsCollectionName  string
	streamsCollectionName string
	autoEnsureIndexes     bool
}

func newStrategyConfig() *strategyConfig {
	return &strategyConfig{
		log:                   estoria.DefaultLogger(),
		sessOpts:              DefaultSessionOptions(),
		txOpts:                DefaultTransactionOptions(),
		eventsCollectionName:  DefaultEventsCollectionName,
		streamsCollectionName: DefaultStreamsCollectionName,
	}
}

func (c *strategyConfig) apply(opts ...StrategyOption) error {
	for _, opt := range opts {
		if err := opt(c); err != nil {
			return err
		}
	}

	return nil
}

type StrategyOption func(*strategyConfig) error

// WithLogger sets the logger to use for the event store.
//
// The default logger is estoria.DefaultLogger().
func WithLogger(logger estoria.Logger) StrategyOption {
	return func(s *strategyConfig) error {
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
func WithSessionOptions(opts *options.SessionOptionsBuilder) StrategyOption {
	return func(s *strategyConfig) error {
		if opts == nil {
			return errors.New("session options cannot be nil")
		}

		s.sessOpts = opts
		return nil
	}
}

// WithTransactionOptions overrides the default transaction options used when
// starting a new MongoDB transaction on a session.
//
// By default, DefaultTransactionOptions() is used.
func WithTransactionOptions(opts *options.TransactionOptionsBuilder) StrategyOption {
	return func(s *strategyConfig) error {
		if opts == nil {
			return errors.New("transaction options cannot be nil")
		}

		s.txOpts = opts
		return nil
	}
}

// WithAutoEnsureIndexes causes the strategy to ensure an event collection's indexes
// before that collection's first append (once per collection per process), instead of
// relying on an explicit EnsureIndexes call at deployment time. It is the only workable
// arrangement when a collection selector creates collections on the fly, since
// collections that do not exist yet cannot be indexed up front.
//
// By default, indexes are only created by explicit EnsureIndexes calls.
func WithAutoEnsureIndexes() StrategyOption {
	return func(s *strategyConfig) error {
		s.autoEnsureIndexes = true
		return nil
	}
}

// WithEventsCollectionName overrides the name of the collection a
// SingleCollectionStrategy stores all events in; a MultiCollectionStrategy derives its
// event collection names from its collection selector and ignores this option.
//
// By default, DefaultEventsCollectionName is used.
func WithEventsCollectionName(name string) StrategyOption {
	return func(s *strategyConfig) error {
		if name == "" {
			return errors.New("events collection name cannot be empty")
		}

		s.eventsCollectionName = name
		return nil
	}
}

// WithStreamsCollectionName overrides the name of the collection holding stream and
// offset counter documents. With a MultiCollectionStrategy, the name must not collide
// with any name the collection selector can produce.
//
// By default, DefaultStreamsCollectionName is used.
func WithStreamsCollectionName(name string) StrategyOption {
	return func(s *strategyConfig) error {
		if name == "" {
			return errors.New("streams collection name cannot be empty")
		}

		s.streamsCollectionName = name
		return nil
	}
}
