package eventstore

import (
	"fmt"

	"go.jetpack.io/typeid"
)

// ErrEventExists is returned when attempting to write an event that already exists.
type ErrEventExists struct {
	EventID typeid.AnyID
}

// Error returns the error message.
func (e ErrEventExists) Error() string {
	return fmt.Sprintf("event exists: %s", e.EventID)
}
