package eventstore_test

import (
	"testing"

	"github.com/go-estoria/estoria-contrib/kurrentdb/eventstore"
	coreeventstore "github.com/go-estoria/estoria/eventstore"
)

// The store must not satisfy eventstore.StreamDeleter: KurrentDB cannot honor
// full-delete's reuse-from-version-1 semantic (see the EventStore doc comment), and
// capability discovery is by type assertion, so adding a DeleteStream method with any
// other semantic would silently claim the interface and lie to every caller.
func TestEventStore_DoesNotClaimStreamDeleter(t *testing.T) {
	t.Parallel()

	var store any = (*eventstore.EventStore)(nil)
	if _, ok := store.(coreeventstore.StreamDeleter); ok {
		t.Fatal("EventStore satisfies eventstore.StreamDeleter, whose full-delete semantics KurrentDB cannot implement")
	}
}
