package eventstore

import (
	"fmt"
	"time"

	"github.com/go-estoria/estoria"
	"github.com/go-estoria/estoria/typeid"
	"github.com/redis/go-redis/v9"
)

type event struct {
	id            typeid.TypeID
	streamID      typeid.TypeID
	streamVersion int64
	timestamp     time.Time
	data          []byte
}

var _ estoria.EventStoreEvent = (*event)(nil)

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

func eventFromRedisMessage(streamID typeid.TypeID, message redis.XMessage) (*event, error) {
	eventData := message.Values

	eventIDStr, ok := eventData["event_id"].(string)
	if !ok {
		return nil, fmt.Errorf("event ID is not string")
	}

	eventID, err := typeid.ParseString(eventIDStr)
	if err != nil {
		return nil, fmt.Errorf("parsing event ID: %w", err)
	}

	timestampStr, ok := eventData["timestamp"].(string)
	if !ok {
		return nil, fmt.Errorf("timestamp is not string")
	}

	timestamp, err := time.Parse(time.RFC3339, timestampStr)
	if err != nil {
		return nil, fmt.Errorf("parsing timestamp: %w", err)
	}

	data, ok := eventData["data"].(string)
	if !ok {
		return nil, fmt.Errorf("event data (%T) is not string", eventData["data"])
	}

	return &event{
		streamID:  streamID,
		id:        eventID,
		timestamp: timestamp,
		data:      []byte(data),
	}, nil
}
