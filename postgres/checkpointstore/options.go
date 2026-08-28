package checkpointstore

import (
	"errors"
	"fmt"
	"regexp"
)

var tableNameRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}$`)

// Option is a functional option for configuring a CheckpointStore.
type Option func(*CheckpointStore) error

// WithTableName sets the database table name used by the checkpoint store.
//
// The name must be a valid SQL identifier: it must start with a letter or
// underscore and contain only letters, digits, or underscores, with a maximum
// length of 63 characters. The default is "projection_checkpoint".
func WithTableName(name string) Option {
	return func(s *CheckpointStore) error {
		if err := validateTableName(name); err != nil {
			return fmt.Errorf("invalid table name: %w", err)
		}

		s.tableName = name
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
