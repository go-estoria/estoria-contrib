package strategy

import (
	"fmt"
	"time"

	"github.com/go-estoria/estoria"
	"go.jetpack.io/typeid"
)

type eventDocument struct {
	StreamType string    `bson:"stream_type"`
	StreamID   string    `bson:"stream_id"`
	EventType  string    `bson:"event_type"`
	EventID    string    `bson:"event_id"`
	Timestamp  time.Time `bson:"timestamp"`
	Data       []byte    `bson:"data"`
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
		StreamID:   streamID.Suffix(),
		StreamType: streamID.Prefix(),
		EventID:    eventID.Suffix(),
		EventType:  eventID.Prefix(),
		Timestamp:  e.Timestamp(),
		Data:       e.Data(),
	}
}

func eventFromDocument(d *eventDocument) (*event, error) {
	eventID, err := typeid.From(d.EventType, d.EventID)
	if err != nil {
		return nil, fmt.Errorf("parsing event ID: %w", err)
	}

	streamID, err := typeid.From(d.StreamType, d.StreamID)
	if err != nil {
		return nil, fmt.Errorf("parsing stream ID: %w", err)
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
