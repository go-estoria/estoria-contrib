package outbox

import (
	"time"

	"github.com/go-estoria/estoria/typeid"
	"github.com/gofrs/uuid/v5"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Item represents a single outbox item delivered to an ItemHandler.
type Item struct {
	// GlobalOffset is the event's global offset; it is the insertion-order key for the outbox.
	GlobalOffset int64

	// EventID is the ID of the event.
	EventID typeid.ID

	// StreamID is the ID of the stream the event belongs to.
	StreamID typeid.ID

	// StreamVersion is the version of the event within its stream (the per-stream FIFO key).
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

// itemDocument is the BSON shape of an outbox item document.
type itemDocument struct {
	ID            bson.ObjectID     `bson:"_id,omitempty"`
	GlobalOffset  int64             `bson:"global_offset"`
	StreamType    string            `bson:"stream_type"`
	StreamID      string            `bson:"stream_id"`
	StreamVersion int64             `bson:"stream_version"`
	EventID       string            `bson:"event_id"`
	EventType     string            `bson:"event_type"`
	Timestamp     time.Time         `bson:"timestamp"`
	Data          []byte            `bson:"data"`
	Metadata      map[string]string `bson:"metadata,omitempty"`
	Status        string            `bson:"status"`
	RetryCount    int               `bson:"retry_count"`
	LastError     *string           `bson:"last_error"`
	CreatedAt     time.Time         `bson:"created_at"`
	FailedAt      *time.Time        `bson:"failed_at,omitempty"`
}

// toItem converts a decoded outbox document into a public Item.
func (d itemDocument) toItem() (*Item, error) {
	eventUUID, err := uuid.FromString(d.EventID)
	if err != nil {
		return nil, err
	}
	streamUUID, err := uuid.FromString(d.StreamID)
	if err != nil {
		return nil, err
	}

	return &Item{
		GlobalOffset:  d.GlobalOffset,
		EventID:       typeid.New(d.EventType, eventUUID),
		StreamID:      typeid.New(d.StreamType, streamUUID),
		StreamVersion: d.StreamVersion,
		Timestamp:     d.Timestamp,
		Data:          d.Data,
		Metadata:      d.Metadata,
		CreatedAt:     d.CreatedAt,
		RetryCount:    d.RetryCount,
		LastError:     d.LastError,
		FailedAt:      d.FailedAt,
	}, nil
}

const (
	statusPending = "pending"
	statusFailed  = "failed"
)
