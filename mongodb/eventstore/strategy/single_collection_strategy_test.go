package strategy_test

import (
	"context"
	"strings"
	"testing"

	"github.com/go-estoria/estoria-contrib/mongodb/eventstore/strategy"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// TestNewSingleCollectionStrategy_RejectsSharedCollectionName pins the constructor-time
// refusal of one collection serving as both events and streams: stream and counter
// documents would contaminate event reads, and the event indexes would be built over
// counter documents. Connecting is lazy, so no server is needed.
func TestNewSingleCollectionStrategy_RejectsSharedCollectionName(t *testing.T) {
	t.Parallel()

	client, err := mongo.Connect(options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		t.Fatalf("creating lazy client: %v", err)
	}
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })

	_, err = strategy.NewSingleCollectionStrategy(client.Database("estoria"),
		strategy.WithEventsCollectionName("shared"),
		strategy.WithStreamsCollectionName("shared"))
	if err == nil || !strings.Contains(err.Error(), "must be distinct") {
		t.Fatalf("want a distinct-collections refusal, got %v", err)
	}
}
