package checkpointstore_test

import (
	"testing"

	sqlitecheckpointstore "github.com/go-estoria/estoria-contrib/sqlite/checkpointstore"
)

func TestNew_RequiresDB(t *testing.T) {
	t.Parallel()

	if _, err := sqlitecheckpointstore.New(nil); err == nil {
		t.Error("want an error creating a CheckpointStore without a database, got nil")
	}
}

// The option is applied directly rather than through New, whose nil-db check
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

			err := sqlitecheckpointstore.WithTableName(tc.name)(&sqlitecheckpointstore.CheckpointStore{})
			if gotErr := err != nil; gotErr != tc.wantErr {
				t.Errorf("want error %t for table name %q, got %v", tc.wantErr, tc.name, err)
			}
		})
	}
}
