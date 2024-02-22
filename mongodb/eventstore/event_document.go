package eventstore

import (
	"time"

	"github.com/go-estoria/estoria"
)

type eventDocument struct {
	EventID            string            `bson:"event_id"`
	EventType          string            `bson:"type"`
	EventAggregateID   string            `bson:"aggregate_id"`
	EventAggregateType string            `bson:"aggregate_type"`
	EventTimestamp     time.Time         `bson:"timestamp"`
	EventData          estoria.EventData `bson:"data"`
}

var _ estoria.Event = (*eventDocument)(nil)

func documentFromEvent(e estoria.Event) eventDocument {
	eventID := e.ID()
	aggregateID := e.AggregateID()
	return eventDocument{
		EventID:            eventID.ID.String(),
		EventType:          eventID.EventType,
		EventAggregateID:   aggregateID.ID.String(),
		EventAggregateType: aggregateID.Type,
		EventTimestamp:     e.Timestamp(),
		EventData:          e.Data(),
	}
}

func (e eventDocument) ID() estoria.EventID {
	return estoria.EventID{
		ID:          estoria.StringID(e.EventID),
		EventType:   e.EventType,
		AggregateID: e.AggregateID(),
	}
}

func (e eventDocument) AggregateID() estoria.AggregateID {
	return estoria.AggregateID{
		ID:   estoria.StringID(e.EventAggregateID),
		Type: e.EventAggregateType,
	}
}

func (e eventDocument) Timestamp() time.Time {
	return e.EventTimestamp
}

func (e eventDocument) Data() estoria.EventData {
	return e.EventData
}
