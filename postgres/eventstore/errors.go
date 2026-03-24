package eventstore

import (
	"fmt"

	"github.com/go-estoria/estoria/typeid"
)

// ErrEventExists is returned when attempting to write an event that already exists.
type ErrEventExists struct {
	EventID typeid.ID
	Err     error
}

// Error returns the error message.
func (e ErrEventExists) Error() string {
	return fmt.Sprintf("event exists: %s", e.EventID)
}

// Unwrap returns the underlying error.
func (e ErrEventExists) Unwrap() error {
	return e.Err
}
