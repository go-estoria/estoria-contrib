package aggregatecache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/coocood/freecache"
	"github.com/go-estoria/estoria"
	"github.com/go-estoria/estoria/aggregatestore"
	"github.com/go-estoria/estoria/typeid"
)

// FreeCache is the subset of *freecache.Cache the aggregate cache uses.
type FreeCache interface {
	Get(key []byte) (value []byte, err error)
	Set(key, value []byte, expire int) (err error)
}

// A Cache is an aggregate cache backed by FreeCache, storing aggregate state
// and version keyed by aggregate ID.
type Cache[S any] struct {
	cache      FreeCache
	stateCodec estoria.StateCodec[S]
	ttlSeconds int
}

var _ aggregatestore.AggregateCache[struct{}] = (*Cache[struct{}])(nil)

// New creates a new Cache using the given FreeCache instance.
func New[S any](cache FreeCache, opts ...CacheOption[S]) *Cache[S] {
	aggregateCache := &Cache[S]{
		cache:      cache,
		stateCodec: estoria.JSONStateCodec[S]{},
		ttlSeconds: 300,
	}

	for _, opt := range opts {
		opt(aggregateCache)
	}

	return aggregateCache
}

// cacheEntry is the stored envelope: state bytes as produced by the state codec,
// plus the aggregate version. The envelope itself is always JSON; the state bytes
// pass through opaquely, so a non-JSON state codec is safe.
type cacheEntry struct {
	State   []byte `json:"s"`
	Version int64  `json:"v"`
}

// GetAggregate returns the cached state and version for an aggregate, or nil
// with a nil error if the aggregate is not in the cache.
func (c *Cache[S]) GetAggregate(_ context.Context, aggregateID typeid.ID) (*aggregatestore.CachedAggregate[S], error) {
	data, err := c.cache.Get([]byte(aggregateID.String()))
	if errors.Is(err, freecache.ErrNotFound) {
		return nil, nil //nolint:nilnil // a nil entry with a nil error is the cache-miss contract
	} else if err != nil {
		return nil, fmt.Errorf("getting data from cache: %w", err)
	}

	entry := cacheEntry{}
	if err := json.Unmarshal(data, &entry); err != nil {
		return nil, fmt.Errorf("unmarshaling cache entry: %w", err)
	}

	var state S
	if err := c.stateCodec.UnmarshalState(entry.State, &state); err != nil {
		return nil, fmt.Errorf("unmarshaling state: %w", err)
	}

	return &aggregatestore.CachedAggregate[S]{State: state, Version: entry.Version}, nil
}

// PutAggregate stores an aggregate's state and version in the cache.
func (c *Cache[S]) PutAggregate(_ context.Context, aggregateID typeid.ID, entry aggregatestore.CachedAggregate[S]) error {
	stateData, err := c.stateCodec.MarshalState(entry.State)
	if err != nil {
		return fmt.Errorf("marshaling state: %w", err)
	}

	data, err := json.Marshal(cacheEntry{State: stateData, Version: entry.Version})
	if err != nil {
		return fmt.Errorf("marshaling cache entry: %w", err)
	}

	if err := c.cache.Set([]byte(aggregateID.String()), data, c.ttlSeconds); err != nil {
		return fmt.Errorf("setting data in cache: %w", err)
	}

	return nil
}

// A CacheOption configures a Cache.
type CacheOption[S any] func(*Cache[S])

// WithStateCodec sets the codec used to encode and decode aggregate state.
func WithStateCodec[S any](codec estoria.StateCodec[S]) CacheOption[S] {
	return func(c *Cache[S]) {
		c.stateCodec = codec
	}
}

// WithTTL sets the time-to-live for cached aggregates.
func WithTTL[S any](ttl time.Duration) CacheOption[S] {
	return func(c *Cache[S]) {
		c.ttlSeconds = int(ttl.Seconds())
	}
}
