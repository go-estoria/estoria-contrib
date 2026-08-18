package eventstore

import (
	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
)

// AllIteratorWithFrontier builds the store's $all iterator with an explicit frontier, so
// integration tests can simulate a node whose log ends below the frontier captured at
// ReadAll — a lagging follower — without a multi-node cluster. The iterator owns no
// streams: the tests using it exercise termination behavior, never yields.
func AllIteratorWithFrontier(client KurrentClient, frontier int64) eventstore.StreamIterator {
	return &allStreamIterator{
		client:        client,
		owns:          func(string) (typeid.ID, bool) { return typeid.ID{}, false },
		windowSize:    64,
		bound:         -1,
		frontier:      frontier,
		cursor:        -1,
		cursorPrepare: -1,
		verified:      true,
		remaining:     -1,
	}
}
