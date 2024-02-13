package eventstore

import (
	"time"

	"github.com/jefflinse/continuum"
)

type eventDocument struct {
	EventID            string              `bson:"event_id"`
	EventType          string              `bson:"type"`
	EventAggregateID   string              `bson:"aggregate_id"`
	EventAggregateType string              `bson:"aggregate_type"`
	EventTimestamp     time.Time           `bson:"timestamp"`
	EventData          continuum.EventData `bson:"data"`
}

var _ continuum.Event = (*eventDocument)(nil)

func documentFromEvent(e continuum.Event) eventDocument {
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

func (e eventDocument) ID() continuum.EventID {
	return continuum.EventID{
		ID:          continuum.StringID(e.EventID),
		EventType:   e.EventType,
		AggregateID: e.AggregateID(),
	}
}

func (e eventDocument) AggregateID() continuum.AggregateID {
	return continuum.AggregateID{
		ID:   continuum.StringID(e.EventAggregateID),
		Type: e.EventAggregateType,
	}
}

func (e eventDocument) Timestamp() time.Time {
	return e.EventTimestamp
}

func (e eventDocument) Data() continuum.EventData {
	return e.EventData
}
