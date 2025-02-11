package strategy

import (
	"errors"

	"github.com/go-estoria/estoria"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type strategyConfig struct {
	log      estoria.Logger
	sessOpts *options.SessionOptionsBuilder
	txOpts   *options.TransactionOptionsBuilder
}

func newStrategyConfig() *strategyConfig {
	return &strategyConfig{
		log:      estoria.DefaultLogger(),
		sessOpts: DefaultSessionOptions(),
		txOpts:   DefaultTransactionOptions(),
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
