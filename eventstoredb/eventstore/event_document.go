package eventstore

import (
	"time"

	"github.com/go-estoria/estoria"
	"github.com/go-estoria/estoria/typeid"
)

type eventDocument struct {
	StreamID  string    `json:"stream_id"`
	EventType string    `json:"event_type"`
	EventID   string    `json:"event_id"`
	Timestamp time.Time `json:"timestamp"`
	Data      []byte    `json:"data"`
}

type event struct {
	id            typeid.TypeID
	streamID      typeid.TypeID
	streamVersion int64
	timestamp     time.Time
	data          []byte
}

var _ estoria.EventStoreEvent = (*event)(nil)

func documentFromEvent(e estoria.EventStoreEvent) *eventDocument {
	eventID := e.ID()
	streamID := e.StreamID()
	return &eventDocument{
		StreamID:  streamID.String(),
		EventID:   eventID.Value(),
		EventType: eventID.TypeName(),
		Timestamp: e.Timestamp(),
		Data:      e.Data(),
	}
}

func eventFromDocument(d *eventDocument) (*event, error) {
	eventID, err := typeid.From(d.EventType, d.EventID)
	if err != nil {
		return nil, err
	}

	streamID, err := typeid.ParseString(d.StreamID)
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

func (e *event) ID() typeid.TypeID {
	return e.id
}

func (e *event) StreamID() typeid.TypeID {
	return e.streamID
}

func (e *event) StreamVersion() int64 {
	return e.streamVersion
}

func (e *event) Timestamp() time.Time {
	return e.timestamp
}

func (e *event) Data() []byte {
	return e.data
}
