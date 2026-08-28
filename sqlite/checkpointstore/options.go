package checkpointstore

import (
	"errors"
	"fmt"
	"regexp"
)

// tableNameRE is the pre-compiled regex used to validate SQL table identifiers.
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

// quoteIdentifier wraps a SQL identifier in double quotes, escaping any embedded
// double quotes by doubling them. validateTableName already restricts identifiers
// to a safe character set, but quoting matches the style used elsewhere and
// protects against future relaxations.
func quoteIdentifier(name string) string {
	out := make([]byte, 0, len(name)+2)
	out = append(out, '"')
	for i := range len(name) {
		c := name[i]
		if c == '"' {
			out = append(out, '"', '"')
		} else {
			out = append(out, c)
		}
	}
	out = append(out, '"')
	return string(out)
}
