package eventstore

import (
	"fmt"

	"github.com/go-estoria/estoria"
)

// ErrEventExists is returned when attempting to write an event that already exists.
type ErrEventExists struct {
	EventID estoria.TypedID
}

// Error returns the error message.
func (e ErrEventExists) Error() string {
	return fmt.Sprintf("event exists: %s", e.EventID)
}
