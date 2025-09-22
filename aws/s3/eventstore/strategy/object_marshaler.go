package strategy

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
	"github.com/gofrs/uuid/v5"
)

type ObjectMarshaler interface {
	MarshalObject(event *eventstore.Event) ([]byte, error)
	UnmarshalObject(src io.ReadCloser) (*eventstore.Event, error)
}

type JSONObject struct {
	StreamID   uuid.UUID `json:"stream_id"`
	StreamType string    `json:"stream_type"`
	EventID    uuid.UUID `json:"event_id"`
	EventType  string    `json:"event_type"`
	Version    int64     `json:"version"`
	Timestamp  time.Time `json:"timestamp"`
	Data       string    `json:"data"`
}

type JSONObjectMarshaler struct{}

func (m JSONObjectMarshaler) MarshalObject(event *eventstore.Event) ([]byte, error) {
	return json.Marshal(JSONObject{
		StreamID:   event.StreamID.UUID,
		StreamType: event.StreamID.Type,
		EventID:    event.ID.UUID,
		EventType:  event.ID.Type,
		Version:    event.StreamVersion,
		Timestamp:  event.Timestamp,
		Data:       base64.StdEncoding.EncodeToString(event.Data),
	})
}

func (m JSONObjectMarshaler) UnmarshalObject(src io.ReadCloser) (*eventstore.Event, error) {
	var obj JSONObject
	err := json.NewDecoder(src).Decode(&obj)
	if err != nil {
		return nil, err
	}

	data, err := base64.StdEncoding.DecodeString(obj.Data)
	if err != nil {
		return nil, fmt.Errorf("decoding base64 event data: %w", err)
	}

	return &eventstore.Event{
		StreamID:      typeid.New(obj.EventType, obj.EventID),
		ID:            typeid.New(obj.EventType, obj.EventID),
		StreamVersion: obj.Version,
		Timestamp:     obj.Timestamp,
		Data:          data,
	}, nil
}
