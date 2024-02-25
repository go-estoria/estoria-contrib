package eventstore

import (
	"time"

	"github.com/go-estoria/estoria"
)

type eventDocument struct {
	EventAggregateType string    `json:"aggregate_type"`
	EventAggregateID   string    `json:"aggregate_id"`
	EventType          string    `json:"event_type"`
	EventID            string    `json:"event_id"`
	EventTimestamp     time.Time `json:"timestamp"`
	EventData          []byte    `json:"data"`
}

var _ estoria.Event = (*eventDocument)(nil)

func documentFromEvent(e estoria.Event) *eventDocument {
	eventID := e.ID()
	aggregateID := e.AggregateID()
	return &eventDocument{
		EventAggregateID:   aggregateID.ID.String(),
		EventAggregateType: aggregateID.Type,
		EventID:            eventID.ID.String(),
		EventType:          eventID.Type,
		EventTimestamp:     e.Timestamp(),
		EventData:          e.Data(),
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

func (e *eventDocument) Data() []byte {
	return e.EventData
}
