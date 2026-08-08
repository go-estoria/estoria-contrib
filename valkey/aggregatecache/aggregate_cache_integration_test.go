package aggregatecache_test

import (
	"context"
	"testing"

	"github.com/go-estoria/estoria-contrib/valkey/aggregatecache"
	"github.com/go-estoria/estoria/aggregatestore"
	"github.com/go-estoria/estoria/typeid"
	"github.com/gofrs/uuid/v5"
	"github.com/valkey-io/valkey-go"
)

type mockState struct {
	Name string `json:"name"`
}

func newValkeyClient(t *testing.T) valkey.Client {
	t.Helper()

	client, err := valkey.NewClient(valkey.ClientOption{
		InitAddress: []string{"localhost:6380"},
		Password:    "valkey",
	})
	if err != nil {
		t.Fatalf("creating valkey client: %v", err)
	}

	return client
}

func TestCache_GetAggregate_Miss(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("skipping integration test")
	}

	cache := aggregatecache.New[mockState](newValkeyClient(t))

	entry, err := cache.GetAggregate(context.Background(), typeid.New("mockstate", uuid.Must(uuid.NewV4())))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if entry != nil {
		t.Errorf("want nil entry for a cache miss, got %+v", entry)
	}
}

func TestCache_RoundTrip(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("skipping integration test")
	}

	cache := aggregatecache.New[mockState](newValkeyClient(t))
	aggregateID := typeid.New("mockstate", uuid.Must(uuid.NewV4()))

	if err := cache.PutAggregate(context.Background(), aggregateID, aggregatestore.CachedAggregate[mockState]{
		State:   mockState{Name: "test"},
		Version: 42,
	}); err != nil {
		t.Fatalf("putting aggregate: %v", err)
	}

	entry, err := cache.GetAggregate(context.Background(), aggregateID)
	if err != nil {
		t.Fatalf("getting aggregate: %v", err)
	}

	if entry == nil {
		t.Fatal("want a cached entry, got nil")
	}

	if entry.State.Name != "test" {
		t.Errorf("want state name %q, got %q", "test", entry.State.Name)
	}

	if entry.Version != 42 {
		t.Errorf("want version 42, got %d", entry.Version)
	}
}
