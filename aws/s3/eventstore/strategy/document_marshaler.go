package strategy

import (
	"bytes"
	"encoding/json"
	"io"

	"github.com/go-estoria/estoria/eventstore"
)

type DecodeObjectFunc func(dest any) error

type ObjectMarshaler interface {
	MarshalObject(event *eventstore.Event) (io.ReadCloser, error)
	UnmarshalObject(src io.ReadCloser) (*eventstore.Event, error)
}

type JSONObjectMarshaler struct{}

func (m JSONObjectMarshaler) MarshalObject(event *eventstore.Event) (io.ReadCloser, error) {
	data, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}

	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m JSONObjectMarshaler) UnmarshalObject(src io.ReadCloser) (*eventstore.Event, error) {
	var event eventstore.Event
	err := json.NewDecoder(src).Decode(&event)
	return &event, err
}
