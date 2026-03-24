package outbox

import (
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/go-estoria/estoria"
)

var tableNameRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}$`)

// Option is a functional option for configuring an Outbox.
type Option func(*Outbox) error

// WithTableName sets the database table name used by the outbox.
//
// The name must be a valid SQL identifier: it must start with a letter or
// underscore and contain only letters, digits, or underscores, with a maximum
// length of 63 characters.
func WithTableName(name string) Option {
	return func(o *Outbox) error {
		if err := validateTableName(name); err != nil {
			return fmt.Errorf("invalid table name: %w", err)
		}

		o.tableName = name
		return nil
	}
}

// WithPollInterval sets the interval at which the outbox polls for unprocessed items.
//
// The default poll interval is 1 second. The duration must be positive.
func WithPollInterval(d time.Duration) Option {
	return func(o *Outbox) error {
		if d <= 0 {
			return errors.New("poll interval must be positive")
		}

		o.pollInterval = d
		return nil
	}
}

// WithLogger sets the logger used by the outbox.
//
// The default logger is estoria.DefaultLogger().
func WithLogger(logger estoria.Logger) Option {
	return func(o *Outbox) error {
		if logger == nil {
			return errors.New("logger cannot be nil")
		}

		o.log = logger
		return nil
	}
}

// WithMaxRetries sets the maximum number of times a failing outbox item will be retried
// before being marked as permanently failed.
//
// The default is 10. Set to 0 to disable the retry limit (items will retry indefinitely).
func WithMaxRetries(n int) Option {
	return func(o *Outbox) error {
		if n < 0 {
			return errors.New("max retries must be non-negative")
		}
		o.maxRetries = n
		return nil
	}
}

// validateTableName validates that the given table name is a valid SQL identifier.
func validateTableName(name string) error {
	if !tableNameRE.MatchString(name) {
		return errors.New("table name must be a valid SQL identifier")
	}

	return nil
}
