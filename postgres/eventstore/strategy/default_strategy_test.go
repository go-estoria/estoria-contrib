package strategy_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/go-estoria/estoria-contrib/postgres/eventstore/strategy"
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
			stream_id,
			stream_type,
			event_id,
			event_type,
			timestamp,
			stream_offset,
			data
		FROM "event"
		WHERE
			stream_type = $1
			AND
			stream_id = $2
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
			stream_id,
			stream_type,
			event_id,
			event_type,
			timestamp,
			stream_offset,
			data
		FROM "my_events"
		WHERE
			stream_type = $1
			AND
			stream_id = $2
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
			stream_id,
			stream_type,
			event_id,
			event_type,
			timestamp,
			stream_offset,
			data
		FROM "event"
		WHERE
			stream_type = $1
			AND
			stream_id = $2
		ORDER BY
			stream_offset DESC
	`,
			wantNumArgs: 2,
		},
		{
			name: "forward,offset",
			haveReadStreamOpts: eventstore.ReadStreamOptions{
				Direction: eventstore.Forward,
				Offset:    10,
			},
			wantQuery: `
		SELECT
			stream_id,
			stream_type,
			event_id,
			event_type,
			timestamp,
			stream_offset,
			data
		FROM "event"
		WHERE
			stream_type = $1
			AND
			stream_id = $2
		ORDER BY
			stream_offset ASC
		OFFSET 10
	`,
			wantNumArgs: 2,
		},
		{
			name: "reverse,offset",
			haveReadStreamOpts: eventstore.ReadStreamOptions{
				Direction: eventstore.Reverse,
				Offset:    10,
			},
			wantQuery: `
		SELECT
			stream_id,
			stream_type,
			event_id,
			event_type,
			timestamp,
			stream_offset,
			data
		FROM "event"
		WHERE
			stream_type = $1
			AND
			stream_id = $2
		ORDER BY
			stream_offset DESC
		OFFSET 10
	`,
			wantNumArgs: 2,
		},
		{
			name: "forward,count",
			haveReadStreamOpts: eventstore.ReadStreamOptions{
				Direction: eventstore.Forward,
				Count:     10,
			},
			wantQuery: `
		SELECT
			stream_id,
			stream_type,
			event_id,
			event_type,
			timestamp,
			stream_offset,
			data
		FROM "event"
		WHERE
			stream_type = $1
			AND
			stream_id = $2
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
			stream_id,
			stream_type,
			event_id,
			event_type,
			timestamp,
			stream_offset,
			data
		FROM "event"
		WHERE
			stream_type = $1
			AND
			stream_id = $2
		ORDER BY
			stream_offset DESC
		
		LIMIT 10
	`,
			wantNumArgs: 2,
		},

		{
			name: "forward,offset,count",
			haveReadStreamOpts: eventstore.ReadStreamOptions{
				Direction: eventstore.Forward,
				Offset:    10,
				Count:     10,
			},
			wantQuery: `
		SELECT
			stream_id,
			stream_type,
			event_id,
			event_type,
			timestamp,
			stream_offset,
			data
		FROM "event"
		WHERE
			stream_type = $1
			AND
			stream_id = $2
		ORDER BY
			stream_offset ASC
		OFFSET 10
		LIMIT 10
	`,
			wantNumArgs: 2,
		},
		{
			name: "reverse,offset,count",
			haveReadStreamOpts: eventstore.ReadStreamOptions{
				Direction: eventstore.Reverse,
				Offset:    10,
				Count:     10,
			},
			wantQuery: `
		SELECT
			stream_id,
			stream_type,
			event_id,
			event_type,
			timestamp,
			stream_offset,
			data
		FROM "event"
		WHERE
			stream_type = $1
			AND
			stream_id = $2
		ORDER BY
			stream_offset DESC
		OFFSET 10
		LIMIT 10
	`,
			wantNumArgs: 2,
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

			if strings.TrimSpace(gotQuery) != strings.TrimSpace(tt.wantQuery) {
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
			event_id,
			stream_type,
			stream_id,
			event_type,
			timestamp,
			stream_offset,
			data
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
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
			data
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
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

			if strings.TrimSpace(gotStmt) != strings.TrimSpace(tt.wantStmt) {
				t.Errorf("expected statement:\n-----\n%s\n-----\ngot:\n-----\n%s\n-----\n", tt.wantStmt, gotStmt)
			}
		})
	}
}

func must[T any](val T, err error) T {
	if err != nil {
		panic("unexpected error: " + err.Error())
	}
	return val
}
