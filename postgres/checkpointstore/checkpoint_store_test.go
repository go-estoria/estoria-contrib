package checkpointstore_test

import (
	"testing"

	pgcheckpointstore "github.com/go-estoria/estoria-contrib/postgres/checkpointstore"
)

func TestNew_RequiresPool(t *testing.T) {
	t.Parallel()

	if _, err := pgcheckpointstore.New(nil); err == nil {
		t.Error("want an error creating a CheckpointStore without a pool, got nil")
	}
}

// The option is applied directly rather than through New, whose nil-pool check
// would otherwise mask which validation produced the error.
func TestWithTableName_ValidatesIdentifiers(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		wantErr bool
	}{
		{name: "custom_checkpoint", wantErr: false},
		{name: "_checkpoint1", wantErr: false},
		{name: "", wantErr: true},
		{name: "1checkpoint", wantErr: true},
		{name: "checkpoint;drop table event", wantErr: true},
		{name: "checkpoint name", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := pgcheckpointstore.WithTableName(tc.name)(&pgcheckpointstore.CheckpointStore{})
			if gotErr := err != nil; gotErr != tc.wantErr {
				t.Errorf("want error %t for table name %q, got %v", tc.wantErr, tc.name, err)
			}
		})
	}
}
