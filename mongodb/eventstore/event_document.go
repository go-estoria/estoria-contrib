package eventstore

import (
	"fmt"
	"time"

	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
	"github.com/gofrs/uuid/v5"
)

type (
	// DecodeDocumentFunc is a function that decodes a MongoDB document into a destination type.
	DecodeDocumentFunc func(dest any) error

	// A DocumentMarshaler is responsible for marshaling and unmarshaling MongoDB documents to and from event store events.
	DocumentMarshaler interface {
		MarshalDocument(event *Event) (any, error)
		UnmarshalDocument(decode DecodeDocumentFunc) (*Event, error)
	}
)

type Event struct {
	eventstore.Event
	GlobalOffset int64
}

type EventDocument struct {
	StreamType   string    `bson:"stream_type"`
	StreamID     string    `bson:"stream_id"`
	EventType    string    `bson:"event_type"`
	EventID      string    `bson:"event_id"`
	Offset       int64     `bson:"offset"`
	GlobalOffset int64     `bson:"global_offset"`
	Timestamp    time.Time `bson:"timestamp"`
	EventData    []byte    `bson:"event_data"`
}

type DefaultMarshaler struct{}

// MarshalDocument encodes an event into a document.
func (DefaultMarshaler) MarshalDocument(event *Event) (any, error) {
	return EventDocument{
		StreamType:   event.StreamID.Type,
		StreamID:     event.StreamID.UUID.String(),
		EventType:    event.ID.Type,
		EventID:      event.ID.UUID.String(),
		Offset:       event.StreamVersion,
		GlobalOffset: event.GlobalOffset,
		Timestamp:    event.Timestamp,
		EventData:    event.Data,
	}, nil
}

// UnmarshalDocument decodes a document into an event.
func (DefaultMarshaler) UnmarshalDocument(decode DecodeDocumentFunc) (*Event, error) {
	doc := EventDocument{}
	if err := decode(&doc); err != nil {
		return nil, fmt.Errorf("decoding event document: %w", err)
	}

	streamID, err := uuid.FromString(doc.StreamID)
	if err != nil {
		return nil, fmt.Errorf("parsing stream ID: %w", err)
	}

	eventID, err := uuid.FromString(doc.EventID)
	if err != nil {
		return nil, fmt.Errorf("parsing event ID: %w", err)
	}

	return &Event{
		Event: eventstore.Event{
			ID:            typeid.New(doc.EventType, eventID),
			StreamID:      typeid.New(doc.StreamType, streamID),
			StreamVersion: doc.Offset,
			Timestamp:     doc.Timestamp,
			Data:          doc.EventData,
		},
		GlobalOffset: doc.GlobalOffset,
	}, nil
}
