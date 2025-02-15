package strategy

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/go-estoria/estoria"
	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
)

type Database interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

type Transaction interface {
	QueryRow(query string, args ...any) *sql.Row
	Rollback() error
	Prepare(query string) (*sql.Stmt, error)
}

type SingleTableStrategy struct {
	db        Database
	tableName string
	log       estoria.Logger
}

type InsertStreamEventsResult struct {
	StatementResults []sql.Result
}

func NewSingleTableStrategy(db Database, tableName string) (*SingleTableStrategy, error) {
	if db == nil {
		return nil, fmt.Errorf("database is required")
	} else if tableName == "" {
		return nil, fmt.Errorf("table is required")
	}

	return &SingleTableStrategy{
		db:        db,
		tableName: tableName,
		log:       estoria.GetLogger().WithGroup("eventstore"),
	}, nil
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

	offset, err := s.getHighestOffset(tx, streamID)
	if err != nil {
		return fmt.Errorf("getting highest offset: %w", err)
	}

	globalOffset, err := s.getHighestGlobalOffset(tx)
	if err != nil {
		return fmt.Errorf("getting highest global offset: %w", err)
	}

	_, err = inTxnFn(tx, s.tableName, offset, globalOffset)
	return err
}

func (s *SingleTableStrategy) getHighestOffset(tx Transaction, streamID typeid.UUID) (int64, error) {
	s.log.Debug("finding highest offset for stream", "stream_id", streamID)
	var offset int64
	if err := tx.QueryRow(fmt.Sprintf(`
		SELECT COALESCE(MAX(stream_offset), 0)
		FROM "%s"
		WHERE stream_type = $1 AND stream_id = $2
	`, s.tableName), streamID.TypeName(), streamID.Value()).Scan(&offset); err != nil {
		return 0, fmt.Errorf("querying highest offset: %w", err)
	}

	s.log.Debug("got highest offset for stream", "stream_id", streamID, "offset", offset)
	return offset, nil
}

func (s *SingleTableStrategy) getHighestGlobalOffset(tx Transaction) (int64, error) {
	s.log.Debug("finding highest global offset in event store")
	var offset int64
	if err := tx.QueryRow(fmt.Sprintf(`
		SELECT COALESCE(MAX(global_offset), 0)
		FROM "%s"
	`, s.tableName)).Scan(&offset); err != nil {
		return 0, fmt.Errorf("querying highest global offset: %w", err)
	}

	s.log.Debug("got highest global offset in event store", "offset", offset)
	return offset, nil
}
