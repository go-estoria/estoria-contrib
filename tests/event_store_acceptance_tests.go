package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

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
	for i := 0; i < 10; i++ {
		id, err := typeid.NewUUID("eventtype")
		if err != nil {
			return fmt.Errorf("error creating event ID: %v", err)
		}

		appendedEvents = append(appendedEvents, &eventstore.WritableEvent{
			ID:        id,
			Data:      []byte(fmt.Sprintf("event data %d", i)),
			Timestamp: time.Now(),
		})
	}

	if err := eventStore.AppendStream(context.Background(), streamID, appendedEvents, eventstore.AppendStreamOptions{}); err != nil {
		return fmt.Errorf("error appending events to stream: %v", err)
	}

	iter, err := eventStore.ReadStream(context.Background(), streamID, eventstore.ReadStreamOptions{})
	if err != nil {
		return fmt.Errorf("error reading stream: %v", err)
	}

	readEvents := []*eventstore.Event{}
	for {
		event, err := iter.Next(context.Background())
		if err == eventstore.ErrEndOfEventStream {
			break
		}

		if err != nil {
			return fmt.Errorf("error reading event: %v", err)
		}

		readEvents = append(readEvents, event)
	}

	if len(readEvents) != len(appendedEvents) {
		return fmt.Errorf("expected %d events, got %d", len(appendedEvents), len(readEvents))
	}

	for i, readEvent := range readEvents {
		if readEvent.ID != appendedEvents[i].ID {
			return fmt.Errorf("expected event ID %s, got %s", appendedEvents[i].ID, readEvent.ID)
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

		if readEvent.Timestamp.Equal(appendedEvents[i].Timestamp) {
			return fmt.Errorf("expected event timestamp %s, got %s", appendedEvents[i].Timestamp, readEvent.Timestamp)
		}
	}

	return nil
}
