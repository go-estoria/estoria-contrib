package aggregatecache

import (
	"context"
	"errors"
	"fmt"

	"github.com/allegro/bigcache/v3"
	"github.com/go-estoria/estoria"
	"github.com/go-estoria/estoria/aggregatestore"
	"github.com/go-estoria/estoria/typeid"
)

type Snapshot[E estoria.Entity] struct {
	Entity  E     `json:"e"`
	Version int64 `json:"v"`
}

type Cache[E estoria.Entity] struct {
	cache     *bigcache.BigCache
	marshaler estoria.Marshaler[Snapshot[E], *Snapshot[E]]
}

func New[E estoria.Entity](cache *bigcache.BigCache, opts ...CacheOption[E]) *Cache[E] {
	aggregateCache := &Cache[E]{
		cache:     cache,
		marshaler: estoria.JSONMarshaler[Snapshot[E]]{},
	}

	for _, opt := range opts {
		opt(aggregateCache)
	}

	return aggregateCache
}

func (c *Cache[E]) GetAggregate(ctx context.Context, aggregateID typeid.UUID) (*aggregatestore.Aggregate[E], error) {
	data, err := c.cache.Get(aggregateID.String())
	if errors.Is(err, bigcache.ErrEntryNotFound) {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("getting data from cache: %w", err)
	}

	snapshot := Snapshot[E]{}
	if err := c.marshaler.Unmarshal(data, &snapshot); err != nil {
		return nil, fmt.Errorf("unmarshaling data: %w", err)
	}

	aggregate := &aggregatestore.Aggregate[E]{}
	aggregate.State().SetEntityAtVersion(snapshot.Entity, snapshot.Version)

	return aggregate, nil
}

func (c *Cache[E]) PutAggregate(ctx context.Context, aggregate *aggregatestore.Aggregate[E]) error {
	snapshot := &Snapshot[E]{
		Entity:  aggregate.Entity(),
		Version: aggregate.Version(),
	}

	data, err := c.marshaler.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("marshaling snapshot: %w", err)
	}

	if err := c.cache.Set(aggregate.ID().String(), data); err != nil {
		return fmt.Errorf("setting data in cache: %w", err)
	}

	return nil
}

type CacheOption[E estoria.Entity] func(*Cache[E])

func WithMarshaler[E estoria.Entity](marshaler estoria.Marshaler[Snapshot[E], *Snapshot[E]]) CacheOption[E] {
	return func(c *Cache[E]) {
		c.marshaler = marshaler
	}
}
