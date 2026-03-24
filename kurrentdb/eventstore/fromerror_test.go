package eventstore

import (
	"errors"
	"testing"

	"github.com/kurrent-io/KurrentDB-Client-Go/kurrentdb"
)

// TestFromErrorBehavior documents the actual behavior of kurrentdb.FromError.
// This test exists to verify our understanding of the (confusing) API.
func TestFromErrorBehavior(t *testing.T) {
	t.Run("nil error returns nil, true", func(t *testing.T) {
		result, ok := kurrentdb.FromError(nil)
		if result != nil {
			t.Errorf("expected nil result for nil error, got %v", result)
		}
		if !ok {
			t.Error("expected ok=true for nil error")
		}
	})

	t.Run("KurrentDB error returns error, false", func(t *testing.T) {
		kdbErr := &kurrentdb.Error{}
		result, ok := kurrentdb.FromError(kdbErr)
		if result == nil {
			t.Error("expected non-nil result for KurrentDB error")
		}
		if ok {
			t.Error("expected ok=false for KurrentDB error")
		}
	})

	t.Run("generic error returns wrapped error, false", func(t *testing.T) {
		genericErr := errors.New("generic")
		result, ok := kurrentdb.FromError(genericErr)
		if result == nil {
			t.Error("expected non-nil result for generic error")
		}
		if ok {
			t.Error("expected ok=false for generic error")
		}
		// Wrapped error should have ErrorCodeUnknown
		if result.Code() != kurrentdb.ErrorCodeUnknown {
			t.Errorf("expected ErrorCodeUnknown for generic error, got %v", result.Code())
		}
	})
}

// TestStreamIteratorErrorHandling verifies the error mapping logic.
// This test confirms that line 59 correctly handles KurrentDB errors with `!ok` check.
func TestStreamIteratorErrorHandling(t *testing.T) {
	t.Run("pattern with !ok correctly enters block for all errors", func(t *testing.T) {
		// Simulate the pattern at line 59 (ORIGINAL CODE with !ok)
		testErr := &kurrentdb.Error{}

		kdbErr, ok := kurrentdb.FromError(testErr)

		// With !ok, we should enter the block
		if !ok {
			// We're in the block - kdbErr should NOT be nil
			if kdbErr == nil {
				t.Fatal("kdbErr is nil when using !ok pattern - THIS IS THE BUG")
			}
			t.Logf("Successfully entered block with !ok, kdbErr.Code() = %v", kdbErr.Code())
		} else {
			t.Error("!ok pattern should have entered block for KurrentDB error")
		}
	})

	t.Run("pattern with ok does NOT enter block for errors", func(t *testing.T) {
		// Simulate changing to `ok` pattern (PROPOSED CHANGE)
		testErr := &kurrentdb.Error{}

		kdbErr, ok := kurrentdb.FromError(testErr)

		// With ok, we should NOT enter for errors (ok is false)
		if ok {
			// We're in the block - this happens only for nil error
			t.Error("ok pattern entered block for non-nil error")
			if kdbErr == nil {
				t.Log("kdbErr is nil when using ok pattern - confirms nil dereference risk")
			}
		} else {
			t.Log("ok pattern correctly does NOT enter block for KurrentDB error")
		}
	})
}
