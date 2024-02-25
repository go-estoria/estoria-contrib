package eventstore

import (
	"time"

	"github.com/go-estoria/estoria"
)

type eventDocument struct {
	EventID            string    `bson:"event_id"`
	EventType          string    `bson:"event_type"`
	EventAggregateID   string    `bson:"aggregate_id"`
	EventAggregateType string    `bson:"aggregate_type"`
	EventTimestamp     time.Time `bson:"timestamp"`
	EventData          []byte    `bson:"data"`

	data estoria.EventData `bson:"-"`
}

var _ estoria.Event = (*eventDocument)(nil)

func documentFromEvent(e estoria.Event) *eventDocument {
	eventID := e.ID()
	aggregateID := e.AggregateID()
	return &eventDocument{
		EventID:            eventID.ID.String(),
		EventType:          eventID.Type,
		EventAggregateID:   aggregateID.ID.String(),
		EventAggregateType: aggregateID.Type,
		EventTimestamp:     e.Timestamp(),
		EventData:          e.RawData(),
	}
}

func (e *eventDocument) ID() estoria.TypedID {
	return estoria.TypedID{
		ID:   estoria.StringID(e.EventID),
		Type: e.EventType,
	}
}

func (e *eventDocument) AggregateID() estoria.TypedID {
	return estoria.TypedID{
		ID:   estoria.StringID(e.EventAggregateID),
		Type: e.EventAggregateType,
	}
}

func (e *eventDocument) Timestamp() time.Time {
	return e.EventTimestamp
}

func (e *eventDocument) Data() estoria.EventData {
	return nil
}

func (e *eventDocument) SetData(data estoria.EventData) {
	e.data = data
}

func (e *eventDocument) RawData() []byte {
	return e.EventData
}

func (e *eventDocument) SetRawData(data []byte) {
	e.EventData = data
}
