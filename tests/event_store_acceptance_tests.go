package tests

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
)

func EventStoreAcceptanceTest(t *testing.T, eventStore eventstore.Store) error {
	t.Helper()

	streamID := typeid.NewV4("streamtype")

	appendedEvents := []*eventstore.WritableEvent{}
	for i := range 10 {
		appendedEvents = append(appendedEvents, &eventstore.WritableEvent{
			Type: "eventtype",
			Data: fmt.Appendf(nil, `{"index":%d}`, i+1),
		})
	}

	if err := eventStore.AppendStream(t.Context(), streamID, appendedEvents, eventstore.AppendStreamOptions{}); err != nil {
		return fmt.Errorf("error appending events to stream: %v", err)
	}

	iter, err := eventStore.ReadStream(t.Context(), streamID, eventstore.ReadStreamOptions{})
	if err != nil {
		return fmt.Errorf("error reading stream: %v", err)
	}

	readEvents, err := eventstore.ReadAll(t.Context(), iter)
	if err != nil {
		return fmt.Errorf("error reading events: %v", err)
	}

	if len(readEvents) != len(appendedEvents) {
		return fmt.Errorf("expected %d events, got %d", len(appendedEvents), len(readEvents))
	}

	for i, readEvent := range readEvents {
		t.Logf("read event: ID=%s, StreamID=%s, StreamVersion=%d, Data=%s", readEvent.ID.String(), readEvent.StreamID, readEvent.StreamVersion, string(readEvent.Data))

		if readEvent.ID.UUID.IsNil() {
			return fmt.Errorf("event ID is empty")
		}

		if readEvent.StreamID != streamID {
			return fmt.Errorf("expected stream ID %s, got %s", streamID, readEvent.StreamID)
		}

		if readEvent.StreamVersion != int64(i+1) {
			return fmt.Errorf("expected stream version %d, got %d", i+1, readEvent.StreamVersion)
		}

		// Use semantic JSON comparison so that storage backends that normalize JSON
		// (e.g. Postgres jsonb) don't cause spurious failures.
		if eq, err := jsonEqual(readEvent.Data, appendedEvents[i].Data); err != nil {
			return fmt.Errorf("comparing event data: %w", err)
		} else if !eq {
			return fmt.Errorf("expected event data %s, got %s", string(appendedEvents[i].Data), string(readEvent.Data))
		}
	}

	return nil
}

// jsonEqual reports whether a and b are semantically equivalent JSON documents.
func jsonEqual(a, b []byte) (bool, error) {
	var va, vb any
	if err := json.Unmarshal(a, &va); err != nil {
		return false, fmt.Errorf("unmarshaling first value: %w", err)
	}
	if err := json.Unmarshal(b, &vb); err != nil {
		return false, fmt.Errorf("unmarshaling second value: %w", err)
	}
	return reflect.DeepEqual(va, vb), nil
}
