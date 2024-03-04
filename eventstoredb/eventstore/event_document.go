package eventstore

import (
	"time"

	"github.com/go-estoria/estoria"
	"go.jetpack.io/typeid"
)

type eventDocument struct {
	StreamID  string    `json:"stream_id"`
	EventType string    `json:"event_type"`
	EventID   string    `json:"event_id"`
	Timestamp time.Time `json:"timestamp"`
	Data      []byte    `json:"data"`
}

type event struct {
	id        typeid.AnyID
	streamID  typeid.AnyID
	timestamp time.Time
	data      []byte
}

var _ estoria.Event = (*event)(nil)

func documentFromEvent(e estoria.Event) *eventDocument {
	eventID := e.ID()
	streamID := e.StreamID()
	return &eventDocument{
		StreamID:  streamID.String(),
		EventID:   eventID.Suffix(),
		EventType: eventID.Prefix(),
		Timestamp: e.Timestamp(),
		Data:      e.Data(),
	}
}

func eventFromDocument(d *eventDocument) (*event, error) {
	eventID, err := typeid.From(d.EventType, d.EventID)
	if err != nil {
		return nil, err
	}

	streamID, err := typeid.FromString(d.StreamID)
	if err != nil {
		return nil, err
	}

	return &event{
		id:        eventID,
		streamID:  streamID,
		timestamp: d.Timestamp,
		data:      d.Data,
	}, nil
}

func (e *event) ID() typeid.AnyID {
	return e.id
}

func (e *event) StreamID() typeid.AnyID {
	return e.streamID
}

func (e *event) Timestamp() time.Time {
	return e.timestamp
}

func (e *event) Data() []byte {
	return e.data
}
