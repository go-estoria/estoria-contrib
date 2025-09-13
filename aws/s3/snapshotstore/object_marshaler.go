package snapshotstore

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/go-estoria/estoria/snapshotstore"
	"github.com/go-estoria/estoria/typeid"
	"github.com/gofrs/uuid/v5"
)

type ObjectMarshaler interface {
	MarshalObject(snapshot *snapshotstore.AggregateSnapshot) ([]byte, error)
	UnmarshalObject(src io.ReadCloser) (*snapshotstore.AggregateSnapshot, error)
}

type JSONObject struct {
	AggregateID   uuid.UUID `json:"aggregate_id"`
	AggregateType string    `json:"aggregate_type"`
	Version       int64     `json:"version"`
	Timestamp     time.Time `json:"timestamp"`
	Data          string    `json:"data"`
}

type JSONObjectMarshaler struct{}

func (m JSONObjectMarshaler) MarshalObject(snapshot *snapshotstore.AggregateSnapshot) ([]byte, error) {
	return json.Marshal(JSONObject{
		AggregateID:   snapshot.AggregateID.UUID,
		AggregateType: snapshot.AggregateID.Type,
		Version:       snapshot.AggregateVersion,
		Timestamp:     snapshot.Timestamp,
		Data:          base64.StdEncoding.EncodeToString(snapshot.Data),
	})
}

func (m JSONObjectMarshaler) UnmarshalObject(src io.ReadCloser) (*snapshotstore.AggregateSnapshot, error) {
	var obj JSONObject
	err := json.NewDecoder(src).Decode(&obj)
	if err != nil {
		return nil, err
	}

	data, err := base64.StdEncoding.DecodeString(obj.Data)
	if err != nil {
		return nil, fmt.Errorf("decoding base64 event data: %w", err)
	}

	return &snapshotstore.AggregateSnapshot{
		AggregateID:      typeid.New(obj.AggregateType, obj.AggregateID),
		AggregateVersion: obj.Version,
		Timestamp:        obj.Timestamp,
		Data:             data,
	}, nil
}
