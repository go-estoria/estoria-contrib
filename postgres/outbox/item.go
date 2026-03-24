package outbox

import (
	"time"

	"github.com/go-estoria/estoria/typeid"
)

// Item represents a single outbox item.
type Item struct {
	// ID is the auto-generated outbox row ID (insertion order).
	ID int64

	// EventID is the ID of the event.
	EventID typeid.ID

	// StreamID is the ID of the stream the event belongs to.
	StreamID typeid.ID

	// StreamVersion is the version of the event within its stream.
	StreamVersion int64

	// Timestamp is the original event timestamp.
	Timestamp time.Time

	// Data is the serialized event data.
	Data []byte

	// CreatedAt is when the outbox item was created.
	CreatedAt time.Time
}
