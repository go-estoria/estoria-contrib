package strategy

import "github.com/go-estoria/estoria/typeid"

// StreamMetadata contains metadata about a stream.
type StreamMetadata struct {
	StreamID   typeid.UUID
	LastOffset int64
}
