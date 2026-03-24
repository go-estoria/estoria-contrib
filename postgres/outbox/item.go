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

	// Metadata is optional key-value metadata associated with the event.
	Metadata map[string]string

	// CreatedAt is when the outbox item was created.
	CreatedAt time.Time

	// RetryCount is the number of times processing this item has been attempted and failed.
	RetryCount int

	// LastError is the error message from the most recent failed processing attempt.
	LastError *string

	// FailedAt is set when the item has been permanently failed after exceeding the retry limit.
	FailedAt *time.Time
}
