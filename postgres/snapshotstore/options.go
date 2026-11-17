package snapshotstore

import (
	"errors"
	"fmt"
	"regexp"

	"github.com/go-estoria/estoria"
)

var tableNameRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}$`)

// Option is a functional option for configuring a SnapshotStore.
type Option func(*SnapshotStore) error

// WithTableName sets the database table name used by the snapshot store. The
// name must be a valid SQL identifier: it must start with a letter or underscore
// and contain only letters, digits, or underscores, with a maximum length of 63
// characters. The default is "aggregate_snapshot".
func WithTableName(name string) Option {
	return func(s *SnapshotStore) error {
		if err := validateTableName(name); err != nil {
			return fmt.Errorf("invalid table name: %w", err)
		}

		s.tableName = name
		return nil
	}
}

// WithMaxSnapshots retains only the newest n snapshots per aggregate: each write
// prunes the aggregate's older snapshots. Pruning is best-effort housekeeping; a
// pruning failure is logged, never surfaced as a write failure. n must be at
// least 1. The default retains every snapshot.
func WithMaxSnapshots(n int64) Option {
	return func(s *SnapshotStore) error {
		if n < 1 {
			return errors.New("max snapshots must be at least 1")
		}

		s.maxSnapshots = n
		return nil
	}
}

// WithLogger sets the logger the snapshot store reports pruning failures to. The
// default is estoria.GetLogger(), grouped under "snapshotstore".
func WithLogger(logger estoria.Logger) Option {
	return func(s *SnapshotStore) error {
		if logger == nil {
			return errors.New("logger cannot be nil")
		}

		s.log = logger
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
