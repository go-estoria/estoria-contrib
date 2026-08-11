package outbox

import (
	"errors"
	"time"

	"github.com/go-estoria/estoria"
)

// Option is a functional option for configuring an Outbox.
type Option func(*Outbox) error

// WithPollInterval sets the interval at which Run polls for eligible items.
//
// The default is 1 second. The duration must be positive.
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

// WithMaxRetries sets the maximum number of times a failing item is retried before being marked
// permanently failed (which halts its stream).
//
// The default is 10. Set to 0 to retry indefinitely.
func WithMaxRetries(n int) Option {
	return func(o *Outbox) error {
		if n < 0 {
			return errors.New("max retries must be non-negative")
		}
		o.maxRetries = n
		return nil
	}
}

// WithLeaseDuration sets how long a per-stream lease is held while an item is processed. A handler
// that runs longer than the lease risks duplicate delivery (delivery is at-least-once).
//
// The default is 30 seconds. The duration must be positive.
func WithLeaseDuration(d time.Duration) Option {
	return func(o *Outbox) error {
		if d <= 0 {
			return errors.New("lease duration must be positive")
		}
		o.leaseDur = d
		return nil
	}
}
