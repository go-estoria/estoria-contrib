package strategy_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/go-estoria/estoria-contrib/sqlite/eventstore/strategy"
	"github.com/go-estoria/estoria/eventstore"
	"github.com/go-estoria/estoria/typeid"
)

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
			metadata
		FROM "event"
		WHERE
			stream_type = ?
			AND stream_id = ?

		ORDER BY
			stream_offset ASC

	`,
			wantNumArgs: 2,
		},
		{
			name: "forward (overriden table name)",
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
			metadata
		FROM "my_events"
		WHERE
			stream_type = ?
			AND stream_id = ?

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
			metadata
		FROM "event"
		WHERE
			stream_type = ?
			AND stream_id = ?

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
			metadata
		FROM "event"
		WHERE
			stream_type = ?
			AND stream_id = ?
			AND stream_offset > ?
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
			metadata
		FROM "event"
		WHERE
			stream_type = ?
			AND stream_id = ?
			AND stream_offset <= ?
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
			metadata
		FROM "event"
		WHERE
			stream_type = ?
			AND stream_id = ?

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
			metadata
		FROM "event"
		WHERE
			stream_type = ?
			AND stream_id = ?

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
			metadata
		FROM "event"
		WHERE
			stream_type = ?
			AND stream_id = ?
			AND stream_offset > ?
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
			metadata
		FROM "event"
		WHERE
			stream_type = ?
			AND stream_id = ?
			AND stream_offset <= ?
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

			if normalizeWhitespace(gotQuery) != normalizeWhitespace(tt.wantQuery) {
				t.Errorf("expected query:\n-----\n%s\n-----\ngot:\n-----\n%s\n-----\n", tt.wantQuery, gotQuery)
			}

			if len(gotArgs) != tt.wantNumArgs {
				t.Errorf("expected %d args, got %d", tt.wantNumArgs, len(gotArgs))
			}
		})
	}
}

// normalizeWhitespace collapses every run of whitespace into a single space and trims
// the result. Query expectations should differ only in whitespace, not structure.
func normalizeWhitespace(s string) string {
	return strings.Join(strings.Fields(s), " ")
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
			event_id,
			stream_type,
			stream_id,
			event_type,
			timestamp,
			stream_offset,
			data,
			metadata
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		RETURNING id
	`,
		},
		{
			name: "overriden table name",
			withStrategyOpts: []strategy.DefaultStrategyOption{
				strategy.WithEventsTableName("my_events"),
			},
			wantStmt: `
		INSERT INTO "my_events" (
			event_id,
			stream_type,
			stream_id,
			event_type,
			timestamp,
			stream_offset,
			data,
			metadata
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
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

			if normalizeWhitespace(gotStmt) != normalizeWhitespace(tt.wantStmt) {
				t.Errorf("expected statement:\n-----\n%s\n-----\ngot:\n-----\n%s\n-----\n", tt.wantStmt, gotStmt)
			}
		})
	}
}

func TestDefaultStrategy_RejectsInvalidTableNames(t *testing.T) {
	for _, name := range []string{
		"",
		"1bad",
		"has space",
		`has"quote`,
		"has;semicolon",
		strings.Repeat("a", 64),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := strategy.NewDefaultStrategy(strategy.WithEventsTableName(name)); err == nil {
				t.Errorf("expected error for events table name %q, got nil", name)
			}
			if _, err := strategy.NewDefaultStrategy(strategy.WithStreamsTableName(name)); err == nil {
				t.Errorf("expected error for streams table name %q, got nil", name)
			}
		})
	}
}
