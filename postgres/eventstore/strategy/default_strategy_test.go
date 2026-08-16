package strategy_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/go-estoria/estoria-contrib/postgres/eventstore/strategy"
	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
)

// normalizeSQL collapses all runs of whitespace to single spaces so generated
// queries can be compared for logical equality regardless of indentation or
// trailing whitespace on blank lines left by omitted optional clauses.
func normalizeSQL(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

func TestDefaultStrategy_ReadStreamQuery(t *testing.T) {
	for _, tt := range []struct {
		name               string
		withStrategyOpts   []strategy.DefaultStrategyOption
		haveReadStreamOpts eventstore.ReadStreamOptions
		wantQuery          string
		wantNumArgs        int
		wantErr            error
	}{
		{
			name: "forward",
			wantQuery: `
		SELECT
			id,
			stream_id,
			stream_type,
			event_id,
			event_type,
			timestamp,
			stream_offset,
			data,
			data_content_type,
			metadata
		FROM "event"
		WHERE
			stream_type = $1
			AND stream_id = $2

		ORDER BY
			stream_offset ASC

	`,
			wantNumArgs: 2,
		},
		{
			name: "forward (overridden table name)",
			withStrategyOpts: []strategy.DefaultStrategyOption{
				strategy.WithEventsTableName("my_events"),
			},
			wantQuery: `
		SELECT
			id,
			stream_id,
			stream_type,
			event_id,
			event_type,
			timestamp,
			stream_offset,
			data,
			data_content_type,
			metadata
		FROM "my_events"
		WHERE
			stream_type = $1
			AND stream_id = $2

		ORDER BY
			stream_offset ASC

	`,
			wantNumArgs: 2,
		},
		{
			name: "reverse",
			haveReadStreamOpts: eventstore.ReadStreamOptions{
				Direction: eventstore.Reverse,
			},
			wantQuery: `
		SELECT
			id,
			stream_id,
			stream_type,
			event_id,
			event_type,
			timestamp,
			stream_offset,
			data,
			data_content_type,
			metadata
		FROM "event"
		WHERE
			stream_type = $1
			AND stream_id = $2

		ORDER BY
			stream_offset DESC

	`,
			wantNumArgs: 2,
		},
		{
			name: "forward,after_version",
			haveReadStreamOpts: eventstore.ReadStreamOptions{
				Direction:    eventstore.Forward,
				AfterVersion: 10,
			},
			wantQuery: `
		SELECT
			id,
			stream_id,
			stream_type,
			event_id,
			event_type,
			timestamp,
			stream_offset,
			data,
			data_content_type,
			metadata
		FROM "event"
		WHERE
			stream_type = $1
			AND stream_id = $2
			AND stream_offset > $3
		ORDER BY
			stream_offset ASC

	`,
			wantNumArgs: 3,
		},
		{
			name: "reverse,after_version",
			haveReadStreamOpts: eventstore.ReadStreamOptions{
				Direction:    eventstore.Reverse,
				AfterVersion: 10,
			},
			wantQuery: `
		SELECT
			id,
			stream_id,
			stream_type,
			event_id,
			event_type,
			timestamp,
			stream_offset,
			data,
			data_content_type,
			metadata
		FROM "event"
		WHERE
			stream_type = $1
			AND stream_id = $2
			AND stream_offset <= $3
		ORDER BY
			stream_offset DESC

	`,
			wantNumArgs: 3,
		},
		{
			name: "forward,count",
			haveReadStreamOpts: eventstore.ReadStreamOptions{
				Direction: eventstore.Forward,
				Count:     10,
			},
			wantQuery: `
		SELECT
			id,
			stream_id,
			stream_type,
			event_id,
			event_type,
			timestamp,
			stream_offset,
			data,
			data_content_type,
			metadata
		FROM "event"
		WHERE
			stream_type = $1
			AND stream_id = $2

		ORDER BY
			stream_offset ASC
		LIMIT 10
	`,
			wantNumArgs: 2,
		},
		{
			name: "reverse,count",
			haveReadStreamOpts: eventstore.ReadStreamOptions{
				Direction: eventstore.Reverse,
				Count:     10,
			},
			wantQuery: `
		SELECT
			id,
			stream_id,
			stream_type,
			event_id,
			event_type,
			timestamp,
			stream_offset,
			data,
			data_content_type,
			metadata
		FROM "event"
		WHERE
			stream_type = $1
			AND stream_id = $2

		ORDER BY
			stream_offset DESC
		LIMIT 10
	`,
			wantNumArgs: 2,
		},
		{
			name: "forward,after_version,count",
			haveReadStreamOpts: eventstore.ReadStreamOptions{
				Direction:    eventstore.Forward,
				AfterVersion: 10,
				Count:        10,
			},
			wantQuery: `
		SELECT
			id,
			stream_id,
			stream_type,
			event_id,
			event_type,
			timestamp,
			stream_offset,
			data,
			data_content_type,
			metadata
		FROM "event"
		WHERE
			stream_type = $1
			AND stream_id = $2
			AND stream_offset > $3
		ORDER BY
			stream_offset ASC
		LIMIT 10
	`,
			wantNumArgs: 3,
		},
		{
			name: "reverse,after_version,count",
			haveReadStreamOpts: eventstore.ReadStreamOptions{
				Direction:    eventstore.Reverse,
				AfterVersion: 10,
				Count:        10,
			},
			wantQuery: `
		SELECT
			id,
			stream_id,
			stream_type,
			event_id,
			event_type,
			timestamp,
			stream_offset,
			data,
			data_content_type,
			metadata
		FROM "event"
		WHERE
			stream_type = $1
			AND stream_id = $2
			AND stream_offset <= $3
		ORDER BY
			stream_offset DESC
		LIMIT 10
	`,
			wantNumArgs: 3,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			strat, err := strategy.NewDefaultStrategy(tt.withStrategyOpts...)
			if err != nil {
				t.Fatalf("creating strategy: %v", err)
			}

			gotQuery, gotArgs, err := strat.ReadStreamQuery(typeid.NewV4("entity"), tt.haveReadStreamOpts)

			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("expected error %v, got %v", tt.wantErr, err)
				}
				return
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if normalizeSQL(gotQuery) != normalizeSQL(tt.wantQuery) {
				t.Errorf("expected query:\n-----\n%s\n-----\ngot:\n-----\n%s\n-----\n", tt.wantQuery, gotQuery)
			}

			if len(gotArgs) != tt.wantNumArgs {
				t.Errorf("expected %d args, got %d", tt.wantNumArgs, len(gotArgs))
			}
		})
	}
}

func TestDefaultStrategy_AppendStreamStatement(t *testing.T) {
	for _, tt := range []struct {
		name             string
		withStrategyOpts []strategy.DefaultStrategyOption
		wantStmt         string
		wantErr          error
	}{
		{
			name: "default table name",
			wantStmt: `
		INSERT INTO "event" (
			id,
			event_id,
			stream_type,
			stream_id,
			event_type,
			timestamp,
			stream_offset,
			data,
			data_content_type,
			metadata
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		RETURNING id
	`,
		},
		{
			name: "overridden table name",
			withStrategyOpts: []strategy.DefaultStrategyOption{
				strategy.WithEventsTableName("my_events"),
			},
			wantStmt: `
		INSERT INTO "my_events" (
			id,
			event_id,
			stream_type,
			stream_id,
			event_type,
			timestamp,
			stream_offset,
			data,
			data_content_type,
			metadata
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		RETURNING id
	`,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			strat, err := strategy.NewDefaultStrategy(tt.withStrategyOpts...)
			if err != nil {
				t.Fatalf("creating strategy: %v", err)
			}

			gotStmt, err := strat.AppendStreamStatement()

			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("expected error %v, got %v", tt.wantErr, err)
				}
				return
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if normalizeSQL(gotStmt) != normalizeSQL(tt.wantStmt) {
				t.Errorf("expected statement:\n-----\n%s\n-----\ngot:\n-----\n%s\n-----\n", tt.wantStmt, gotStmt)
			}
		})
	}
}

func TestNewDefaultStrategy_RejectsHazardousIdentifiers(t *testing.T) {
	// The longest safe events table name leaves exactly 63 bytes for the
	// derived stream-offset constraint, the longest derived identifier.
	longest := strings.Repeat("e", 63-len("_stream_offset_unique"))

	for _, tt := range []struct {
		name    string
		opts    []strategy.DefaultStrategyOption
		wantErr string
	}{
		{
			name: "longest safe events table name is accepted",
			opts: []strategy.DefaultStrategyOption{strategy.WithEventsTableName(longest)},
		},
		{
			name:    "events table name whose derived constraint would truncate",
			opts:    []strategy.DefaultStrategyOption{strategy.WithEventsTableName(longest + "e")},
			wantErr: "truncates identifiers",
		},
		{
			name: "streams table colliding with the events table",
			opts: []strategy.DefaultStrategyOption{
				strategy.WithEventsTableName("orders"),
				strategy.WithStreamsTableName("orders"),
			},
			wantErr: "distinct",
		},
		{
			name: "streams table colliding with the position allocator",
			opts: []strategy.DefaultStrategyOption{
				strategy.WithEventsTableName("orders"),
				strategy.WithStreamsTableName("orders_position_allocator"),
			},
			wantErr: "position allocator",
		},
		{
			name: "streams table colliding with the stream-offset constraint",
			opts: []strategy.DefaultStrategyOption{
				strategy.WithEventsTableName("orders"),
				strategy.WithStreamsTableName("orders_stream_offset_unique"),
			},
			wantErr: "constraint",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := strategy.NewDefaultStrategy(tt.opts...)

			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}

				return
			}

			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("want an error containing %q, got %v", tt.wantErr, err)
			}
		})
	}
}
