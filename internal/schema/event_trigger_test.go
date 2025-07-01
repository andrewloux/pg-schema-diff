package schema

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stripe/pg-schema-diff/internal/pgengine"
)

func TestFetchEventTriggers(t *testing.T) {
	engine, err := pgengine.StartEngine()
	require.NoError(t, err)
	defer engine.Close()

	testCases := []struct {
		name             string
		ddl              []string
		expectedTriggers []EventTrigger
		skipReason       string
	}{
		{
			name: "Simple event trigger",
			ddl: []string{
				`CREATE OR REPLACE FUNCTION log_ddl_command()
				RETURNS event_trigger
				LANGUAGE plpgsql
				AS $$
				BEGIN
					RAISE NOTICE 'DDL command executed: %', tg_tag;
				END;
				$$;`,
				`CREATE EVENT TRIGGER log_ddl_trigger
				ON ddl_command_end
				EXECUTE FUNCTION log_ddl_command();`,
			},
			expectedTriggers: []EventTrigger{
				{
					Name:     "log_ddl_trigger",
					Event:    "ddl_command_end",
					Function: SchemaQualifiedName{SchemaName: "public", EscapedName: "log_ddl_command()"},
					Enabled:  "O", // "O" means enabled
					Tags:     []string{},
				},
			},
		},
		{
			name: "Event trigger with tags",
			ddl: []string{
				`CREATE OR REPLACE FUNCTION track_table_changes()
				RETURNS event_trigger
				LANGUAGE plpgsql
				AS $$
				BEGIN
					RAISE NOTICE 'Table operation: %', tg_tag;
				END;
				$$;`,
				`CREATE EVENT TRIGGER track_tables
				ON ddl_command_end
				WHEN TAG IN ('CREATE TABLE', 'ALTER TABLE', 'DROP TABLE')
				EXECUTE FUNCTION track_table_changes();`,
			},
			expectedTriggers: []EventTrigger{
				{
					Name:     "track_tables",
					Event:    "ddl_command_end",
					Function: SchemaQualifiedName{SchemaName: "public", EscapedName: "track_table_changes()"},
					Enabled:  "O",
					Tags:     []string{"CREATE TABLE", "ALTER TABLE", "DROP TABLE"},
				},
			},
		},
		{
			name: "Disabled event trigger",
			ddl: []string{
				`CREATE OR REPLACE FUNCTION monitor_drops()
				RETURNS event_trigger
				LANGUAGE plpgsql
				AS $$
				BEGIN
					RAISE NOTICE 'Object dropped';
				END;
				$$;`,
				`CREATE EVENT TRIGGER monitor_drop_trigger
				ON sql_drop
				EXECUTE FUNCTION monitor_drops();`,
				`ALTER EVENT TRIGGER monitor_drop_trigger DISABLE;`,
			},
			expectedTriggers: []EventTrigger{
				{
					Name:     "monitor_drop_trigger",
					Event:    "sql_drop",
					Function: SchemaQualifiedName{SchemaName: "public", EscapedName: "monitor_drops()"},
					Enabled:  "D", // "D" means disabled
					Tags:     []string{},
				},
			},
		},
		{
			name: "Multiple event triggers",
			ddl: []string{
				`CREATE OR REPLACE FUNCTION audit_func()
				RETURNS event_trigger
				LANGUAGE plpgsql
				AS $$
				BEGIN
					RAISE NOTICE 'Audit event';
				END;
				$$;`,
				`CREATE EVENT TRIGGER audit_ddl
				ON ddl_command_start
				EXECUTE FUNCTION audit_func();`,
				`CREATE EVENT TRIGGER audit_drops
				ON sql_drop
				EXECUTE FUNCTION audit_func();`,
			},
			expectedTriggers: []EventTrigger{
				{
					Name:     "audit_ddl",
					Event:    "ddl_command_start",
					Function: SchemaQualifiedName{SchemaName: "public", EscapedName: "audit_func()"},
					Enabled:  "O",
					Tags:     []string{},
				},
				{
					Name:     "audit_drops",
					Event:    "sql_drop",
					Function: SchemaQualifiedName{SchemaName: "public", EscapedName: "audit_func()"},
					Enabled:  "O",
					Tags:     []string{},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skipReason != "" {
				t.Skip(tc.skipReason)
			}

			// Create database as superuser to create event triggers
			testDb, err := engine.CreateDatabaseWithSuperuser()
			require.NoError(t, err)
			defer testDb.DropDB()

			// Create SQL connection
			db, err := sql.Open("pgx", testDb.GetDSN())
			require.NoError(t, err)
			defer db.Close()

			// Apply DDL
			for _, ddl := range tc.ddl {
				_, err := db.Exec(ddl)
				require.NoError(t, err)
			}

			// Fetch schema
			schema, err := GetSchema(context.Background(), db)
			require.NoError(t, err)

			// Verify event trigger count
			assert.Equal(t, len(tc.expectedTriggers), len(schema.EventTriggers),
				"Expected %d event triggers but got %d. Event Triggers: %+v",
				len(tc.expectedTriggers), len(schema.EventTriggers), schema.EventTriggers)

			// Verify event trigger details
			for i, expected := range tc.expectedTriggers {
				if i < len(schema.EventTriggers) {
					actual := schema.EventTriggers[i]
					assert.Equal(t, expected.Name, actual.Name, "Event trigger name mismatch")
					assert.Equal(t, expected.Event, actual.Event, "Event trigger event mismatch")
					assert.Equal(t, expected.Enabled, actual.Enabled, "Event trigger enabled state mismatch")
					assert.ElementsMatch(t, expected.Tags, actual.Tags, "Event trigger tags mismatch")
				}
			}
		})
	}
}

func TestEventTriggerNormalize(t *testing.T) {
	tests := []struct {
		name     string
		triggers []EventTrigger
		expected []EventTrigger
	}{
		{
			name: "Sort event triggers alphabetically",
			triggers: []EventTrigger{
				{Name: "trigger_c", Event: "ddl_command_end"},
				{Name: "trigger_a", Event: "sql_drop"},
				{Name: "trigger_b", Event: "ddl_command_start"},
			},
			expected: []EventTrigger{
				{Name: "trigger_a", Event: "sql_drop"},
				{Name: "trigger_b", Event: "ddl_command_start"},
				{Name: "trigger_c", Event: "ddl_command_end"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := Schema{EventTriggers: tt.triggers}
			normalized := schema.Normalize()
			assert.Equal(t, tt.expected, normalized.EventTriggers)
		})
	}
}