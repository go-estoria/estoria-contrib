package eventstore

import (
	"time"

	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
	"github.com/gofrs/uuid"
	uuidv5 "github.com/gofrs/uuid/v5"
)

type eventDocument struct {
	StreamID  string    `json:"stream_id"`
	EventType string    `json:"event_type"`
	EventID   string    `json:"event_id"`
	Timestamp time.Time `json:"timestamp"`
	Data      []byte    `json:"data"`
}

func documentFromEvent(e eventstore.EventStoreEvent) *eventDocument {
	eventID := e.ID
	streamID := e.StreamID
	return &eventDocument{
		StreamID:  streamID.String(),
		EventID:   eventID.Value(),
		EventType: eventID.TypeName(),
		Timestamp: e.Timestamp,
		Data:      e.Data,
	}
}

func eventFromDocument(d *eventDocument) (*eventstore.EventStoreEvent, error) {
	uid, err := uuid.FromString(d.EventID)
	if err != nil {
		return nil, err
	}

	uidV5, err := uuidv5.FromBytes(uid.Bytes())
	if err != nil {
		return nil, err
	}

	streamID, err := typeid.ParseUUID(d.StreamID)
	if err != nil {
		return nil, err
	}

	return &eventstore.EventStoreEvent{
		ID:        typeid.FromUUID(d.EventType, uidV5),
		StreamID:  streamID,
		Timestamp: d.Timestamp,
		Data:      d.Data,
	}, nil
}
