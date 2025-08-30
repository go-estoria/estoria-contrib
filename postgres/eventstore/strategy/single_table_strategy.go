package strategy

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/go-estoria/estoria"
	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
)

type (
	// Database is an interface for starting transactions and performing queries.
	Database interface {
		BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
		ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
		QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	}

	// Transaction is an interface for executing queries and rolling back transactions.
	Transaction interface {
		Prepare(query string) (*sql.Stmt, error)
		QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	}
)

// A SingleTableStrategy stores all events for all streams in a single table.
type SingleTableStrategy struct {
	db        Database
	tableName string
	log       estoria.Logger
}

// NewSingleTableStrategy creates a new SingleTableStrategy using the given database and table name.
func NewSingleTableStrategy(db Database, tableName string) (*SingleTableStrategy, error) {
	if db == nil {
		return nil, fmt.Errorf("database is required")
	} else if err := validateTableName(tableName); err != nil {
		return nil, fmt.Errorf("invalid table name: %w", err)
	}

	strat := &SingleTableStrategy{
		db:        db,
		tableName: tableName,
		log:       estoria.GetLogger().WithGroup("eventstore"),
	}

	if err := strat.ensureEventsTable(context.Background()); err != nil {
		return nil, fmt.Errorf("creating %s table: %w", strat.tableName, err)
	}

	return strat, nil
}

// ListStreams returns a list of cursors for iterating over stream metadata.
func (s *SingleTableStrategy) ListStreams(ctx context.Context) ([]*sql.Rows, error) {
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT stream_id, stream_type, MAX(stream_offset), MAX(global_offset)
		FROM %s
		GROUP BY stream_id, stream_type
	`, s.tableName))
	if err != nil {
		return nil, fmt.Errorf("querying streams: %w", err)
	}

	return []*sql.Rows{rows}, nil
}

// GetAllRows returns an iterator over all events in the event store, ordered by global offset.
func (s *SingleTableStrategy) GetAllRows(
	ctx context.Context,
	opts eventstore.ReadStreamOptions,
) ([]*sql.Rows, error) {
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT stream_id, stream_type, event_id, event_type, timestamp, stream_offset, global_offset, data
		FROM %s
		ORDER BY global_offset ASC
		OFFSET $1
	`, s.tableName), opts.Offset)
	if err != nil {
		return nil, fmt.Errorf("querying events: %w", err)
	}

	return []*sql.Rows{rows}, nil
}

// GetStreamCursor returns a SQL rows object for events in the specified stream, ordered by stream offset.
func (s *SingleTableStrategy) GetStreamRows(
	ctx context.Context,
	streamID typeid.UUID,
	opts eventstore.ReadStreamOptions,
) (*sql.Rows, error) {
	s.log.Debug("querying events", "stream_id", streamID)

	limitClause := ""
	if opts.Count > 0 {
		limitClause = fmt.Sprintf("LIMIT %d", opts.Count)
	}

	return s.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT stream_id, stream_type, event_id, event_type, timestamp, stream_offset, global_offset, data
		FROM %s
		WHERE stream_type = $1 AND stream_id = $2
		ORDER BY stream_offset ASC
		%s
		OFFSET $3
	`, s.tableName, limitClause), streamID.TypeName(), streamID.Value(), opts.Offset)
}

// ExecuteInsertTransaction executes the given function within a new SQL transaction suitable for inserting events.
// The function is executed within a transaction and is invoked with the transaction handle, a table name,
// the current offset of the stream, and the global offset.
func (s *SingleTableStrategy) ExecuteInsertTransaction(
	ctx context.Context,
	streamID typeid.UUID,
	inTxnFn func(tx Transaction, table string, offset int64, globalOffset int64) ([]sql.Result, error),
) (e error) {
	tx, err := s.db.BeginTx(ctx, nil) // TODO: add transaction options
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}

	defer func() {
		if e != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				s.log.Error("error rolling back transaction", "error", rollbackErr, "cause", e)
			}
		} else if err := tx.Commit(); err != nil {
			e = fmt.Errorf("committing transaction: %w", err)
		}
	}()

	offset, err := s.getHighestOffset(ctx, tx, streamID)
	if err != nil {
		return fmt.Errorf("getting highest offset: %w", err)
	}

	globalOffset, err := s.getHighestGlobalOffset(ctx, tx)
	if err != nil {
		return fmt.Errorf("getting highest global offset: %w", err)
	}

	_, err = inTxnFn(tx, s.tableName, offset, globalOffset)
	return err
}

// Creates the events table if it does not already exist.
func (s *SingleTableStrategy) ensureEventsTable(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id BIGSERIAL PRIMARY KEY,
			stream_id uuid NOT NULL,
			stream_type varchar(255) NOT NULL,
			event_id uuid NOT NULL,
			event_type varchar(255) NOT NULL,
			timestamp timestamptz NOT NULL,
			stream_offset bigint NOT NULL,
			global_offset bigint NOT NULL,
			data bytea,

			UNIQUE (stream_id, stream_type, stream_offset),
			UNIQUE (global_offset),

			CHECK (stream_offset > 0),
			CHECK (global_offset > 0)
		)
	`, s.tableName))
	return err
}

// Finds the highest offset for the given stream.
func (s *SingleTableStrategy) getHighestOffset(ctx context.Context, tx Transaction, streamID typeid.UUID) (int64, error) {
	s.log.Debug("finding highest offset for stream", "stream_id", streamID)
	var offset int64
	if err := tx.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT stream_offset
		FROM "%s"
		WHERE stream_type = $1 AND stream_id = $2
		ORDER BY stream_offset DESC
		LIMIT 1
	`, s.tableName), streamID.TypeName(), streamID.Value()).Scan(&offset); errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	} else if err != nil {
		return 0, fmt.Errorf("querying highest offset: %w", err)
	}

	s.log.Debug("got highest offset for stream", "stream_id", streamID, "offset", offset)
	return offset, nil
}

// Finds the highest global offset among all events in the event store.
func (s *SingleTableStrategy) getHighestGlobalOffset(ctx context.Context, tx Transaction) (int64, error) {
	s.log.Debug("finding highest global offset in event store")
	var offset int64
	if err := tx.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT global_offset
		FROM "%s"
		ORDER BY global_offset DESC
		LIMIT 1
	`, s.tableName)).Scan(&offset); errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	} else if err != nil {
		return 0, fmt.Errorf("querying highest global offset: %w", err)
	}

	s.log.Debug("got highest global offset in event store", "offset", offset)
	return offset, nil
}
