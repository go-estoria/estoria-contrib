package checkpointstore_test

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

// newSQLiteDB creates a new file-backed SQLite database for tests.
//
// A file-backed database is used (rather than ":memory:") so that the same DB
// is visible across multiple connections opened by the database/sql pool.
// The file is placed inside t.TempDir() and is cleaned up when the test ends.
func newSQLiteDB(t *testing.T) *sql.DB {
	t.Helper()

	// journal_mode=WAL improves concurrency by letting readers run while a writer holds
	// the write lock; busy_timeout makes contention failures retry transparently
	// rather than surface as SQLITE_BUSY.
	dbPath := filepath.Join(t.TempDir(), "estoria_test.db")
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(on)", dbPath)

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("failed to open SQLite database: %v", err)
	}

	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Logf("failed to close SQLite database: %v", err)
		}
	})

	if err := db.PingContext(context.Background()); err != nil {
		t.Fatalf("failed to ping SQLite database: %v", err)
	}

	return db
}
