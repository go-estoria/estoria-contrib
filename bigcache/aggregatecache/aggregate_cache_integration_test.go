package aggregatecache_test

import (
	"context"
	"testing"
	"time"

	"github.com/allegro/bigcache/v3"
	"github.com/go-estoria/estoria-contrib/bigcache/aggregatecache"
	"github.com/go-estoria/estoria/aggregatestore"
	"github.com/go-estoria/estoria/typeid"
	"github.com/gofrs/uuid/v5"
)

type mockState struct {
	Name string `json:"name"`
}

func newBigCache(t *testing.T) *bigcache.BigCache {
	t.Helper()

	cache, err := bigcache.New(context.Background(), bigcache.DefaultConfig(time.Minute))
	if err != nil {
		t.Fatalf("creating bigcache: %v", err)
	}

	return cache
}

func TestCache_GetAggregate_Miss(t *testing.T) {
	t.Parallel()

	cache := aggregatecache.New[mockState](newBigCache(t))

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

	cache := aggregatecache.New[mockState](newBigCache(t))
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
