package tests

import (
	"fmt"
	"testing"

	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
)

func EventStoreAcceptanceTest(t *testing.T, eventStore eventstore.Store) error {
	t.Helper()

	streamID, err := typeid.NewUUID("streamtype")
	if err != nil {
		return fmt.Errorf("error creating streamID: %v", err)
	}

	appendedEvents := []*eventstore.WritableEvent{}
	for i := range 10 {
		appendedEvents = append(appendedEvents, &eventstore.WritableEvent{
			Type: "eventtype",
			Data: fmt.Appendf(nil, "event data %d", i),
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
		if readEvent.ID.IsEmpty() {
			return fmt.Errorf("event ID is empty")
		}

		if readEvent.StreamID != streamID {
			return fmt.Errorf("expected stream ID %s, got %s", streamID, readEvent.StreamID)
		}

		if readEvent.StreamVersion != int64(i+1) {
			return fmt.Errorf("expected stream version %d, got %d", i+1, readEvent.StreamVersion)
		}

		if string(readEvent.Data) != string(appendedEvents[i].Data) {
			return fmt.Errorf("expected event data %s, got %s", string(appendedEvents[i].Data), string(readEvent.Data))
		}
	}

	return nil
}
