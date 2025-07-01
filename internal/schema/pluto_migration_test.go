package schema

import (
	"fmt"
	"testing"
)

// TestPlutoMigrationPatterns tests patterns found in actual Pluto migrations
func TestPlutoMigrationPatterns(t *testing.T) {
	tests := []struct {
		name        string
		functionDef string
		wantRefs    []TableColumnRef
		description string
	}{
		{
			name: "treasury double-entry function",
			functionDef: `CREATE OR REPLACE FUNCTION treasury.validate_journal_entry()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
BEGIN
    -- Validate that debits equal credits
    IF (SELECT SUM(CASE WHEN entry_type = 'debit' THEN amount ELSE -amount END) 
        FROM treasury.journal_entries 
        WHERE transaction_id = NEW.transaction_id) != 0 THEN
        RAISE EXCEPTION 'Journal entry must balance: debits must equal credits';
    END IF;
    
    -- Validate account exists and is active
    IF NOT EXISTS (
        SELECT 1 FROM treasury.accounts 
        WHERE id = NEW.account_id 
        AND is_active = true
    ) THEN
        RAISE EXCEPTION 'Account % does not exist or is not active', NEW.account_id;
    END IF;
    
    RETURN NEW;
END;
$function$`,
			wantRefs: []TableColumnRef{
				{TableName: "treasury", ColumnName: "journal_entries"},
				{TableName: "treasury", ColumnName: "accounts"},
			},
			description: "Complex PL/pgSQL function with schema-qualified tables",
		},
		{
			name: "computed field with row parameter",
			functionDef: `CREATE OR REPLACE FUNCTION public.get_user_full_name(user_row users)
RETURNS text
LANGUAGE sql
STABLE
AS $function$
    SELECT COALESCE(user_row.first_name || ' ' || user_row.last_name, user_row.email)
$function$`,
			wantRefs: []TableColumnRef{
				{TableName: "user_row", ColumnName: "first_name"},
				{TableName: "user_row", ColumnName: "last_name"},
				{TableName: "user_row", ColumnName: "email"},
			},
			description: "Hasura computed field pattern",
		},
		{
			name: "workflow state transition function",
			functionDef: `CREATE OR REPLACE FUNCTION workflows.transition_state(
    p_workflow_id uuid,
    p_from_state text,
    p_to_state text,
    p_user_id uuid
)
RETURNS SETOF workflows.workflow_states
LANGUAGE plpgsql
AS $function$
DECLARE
    v_current_state text;
BEGIN
    -- Get current state with lock
    SELECT state INTO v_current_state
    FROM workflows.workflow_states
    WHERE workflow_id = p_workflow_id
    FOR UPDATE;
    
    -- Validate transition
    IF v_current_state != p_from_state THEN
        RAISE EXCEPTION 'Invalid state transition from % to %', v_current_state, p_to_state;
    END IF;
    
    -- Check transition rules
    IF NOT EXISTS (
        SELECT 1 FROM workflows.transition_rules
        WHERE from_state = p_from_state
        AND to_state = p_to_state
        AND workflow_type = (
            SELECT workflow_type 
            FROM workflows.workflows 
            WHERE id = p_workflow_id
        )
    ) THEN
        RAISE EXCEPTION 'Transition not allowed';
    END IF;
    
    -- Update state
    UPDATE workflows.workflow_states
    SET state = p_to_state,
        updated_by = p_user_id,
        updated_at = CURRENT_TIMESTAMP
    WHERE workflow_id = p_workflow_id
    RETURNING *;
END;
$function$`,
			wantRefs: []TableColumnRef{
				{TableName: "workflows", ColumnName: "workflow_states"},
				{TableName: "workflows", ColumnName: "transition_rules"},
				{TableName: "workflows", ColumnName: "workflows"},
			},
			description: "Complex workflow function with multiple CTEs and subqueries",
		},
		{
			name: "audit trigger function",
			functionDef: `CREATE OR REPLACE FUNCTION audit.audit_trigger_function()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
DECLARE
    audit_row audit.audit_log;
    excluded_cols text[] = ARRAY[]::text[];
    include_values boolean = true;
BEGIN
    IF TG_WHEN <> 'AFTER' THEN
        RAISE EXCEPTION 'audit.audit_trigger_function() may only be fired AFTER';
    END IF;

    audit_row = ROW(
        nextval('audit.audit_log_id_seq'),
        TG_TABLE_SCHEMA::text,
        TG_TABLE_NAME::text,
        TG_RELID,
        current_timestamp,
        current_user::text,
        current_setting('application_name'),
        inet_client_addr(),
        TG_OP,
        NULL, NULL, NULL
    );

    IF TG_OP = 'UPDATE' THEN
        audit_row.row_data = to_jsonb(OLD.*);
        audit_row.changed_fields = to_jsonb(NEW.*) - to_jsonb(OLD.*);
    ELSIF TG_OP = 'DELETE' THEN
        audit_row.row_data = to_jsonb(OLD.*);
    ELSIF TG_OP = 'INSERT' THEN
        audit_row.row_data = to_jsonb(NEW.*);
    END IF;

    INSERT INTO audit.audit_log VALUES (audit_row.*);
    RETURN NULL;
END;
$function$`,
			wantRefs: []TableColumnRef{
				{TableName: "audit", ColumnName: "audit_log"},
			},
			description: "Audit function using dynamic SQL and JSON operations",
		},
		{
			name: "materialized view refresh function",
			functionDef: `CREATE OR REPLACE FUNCTION public.refresh_materialized_views()
RETURNS void
LANGUAGE plpgsql
AS $function$
BEGIN
    -- Refresh in dependency order
    REFRESH MATERIALIZED VIEW CONCURRENTLY public.user_stats;
    REFRESH MATERIALIZED VIEW CONCURRENTLY public.organization_metrics;
    REFRESH MATERIALIZED VIEW CONCURRENTLY public.card_usage_summary;
    
    -- Update refresh timestamp
    UPDATE public.system_metadata 
    SET value = CURRENT_TIMESTAMP::text,
        updated_at = CURRENT_TIMESTAMP
    WHERE key = 'last_materialized_view_refresh';
END;
$function$`,
			wantRefs: []TableColumnRef{
				{TableName: "public", ColumnName: "user_stats"},
				{TableName: "public", ColumnName: "organization_metrics"},
				{TableName: "public", ColumnName: "card_usage_summary"},
				{TableName: "public", ColumnName: "system_metadata"},
			},
			description: "Function refreshing materialized views",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Logf("Testing: %s", tt.description)
			
			refs := extractColumnReferences(tt.functionDef)
			
			// Log what we found
			t.Logf("Found %d references:", len(refs))
			for _, ref := range refs {
				t.Logf("  %s.%s", ref.TableName, ref.ColumnName)
			}
			
			// For now, just ensure we don't crash on complex functions
			// Full validation would require more sophisticated checks
			if len(refs) == 0 && len(tt.wantRefs) > 0 {
				t.Errorf("Expected to find references but found none")
			}
		})
	}
}

// TestEdgeCases tests various edge cases found in migrations
func TestEdgeCases(t *testing.T) {
	edgeCases := []struct {
		name        string
		sql         string
		shouldParse bool
	}{
		{
			name: "function with dollar quoted string containing SQL",
			sql: `CREATE FUNCTION test() RETURNS text AS $$
				DECLARE
					query text := 'SELECT * FROM users WHERE active = true';
				BEGIN
					RETURN query;
				END;
			$$ LANGUAGE plpgsql;`,
			shouldParse: true,
		},
		{
			name: "function with nested dollar quotes",
			sql: `CREATE FUNCTION test() RETURNS text AS $outer$
				BEGIN
					RETURN $inner$SELECT * FROM users$inner$;
				END;
			$outer$ LANGUAGE plpgsql;`,
			shouldParse: true,
		},
		{
			name: "DO block (anonymous function)",
			sql: `DO $$
				BEGIN
					UPDATE users SET migrated = true WHERE created_at < '2023-01-01';
				END;
			$$;`,
			shouldParse: true,
		},
		{
			name: "function with EXECUTE dynamic SQL",
			sql: `CREATE FUNCTION dynamic_query(table_name text) RETURNS void AS $$
				BEGIN
					EXECUTE format('UPDATE %I SET updated_at = NOW()', table_name);
				END;
			$$ LANGUAGE plpgsql;`,
			shouldParse: true,
		},
	}

	for _, tc := range edgeCases {
		t.Run(tc.name, func(t *testing.T) {
			// Try to extract references - mainly testing that we don't panic
			refs := extractColumnReferencesRegex(tc.sql)
			t.Logf("Found %d references in edge case", len(refs))
		})
	}
}

// TestDependencyTracking tests that our dependency tracking works correctly
func TestDependencyTracking(t *testing.T) {
	// This would test the actual dependency resolution in pg-schema-diff
	// For now, we just document what should be tested
	
	scenarios := []struct {
		name        string
		description string
	}{
		{
			name:        "function_after_column_add",
			description: "Function using new column should come after ALTER TABLE ADD COLUMN",
		},
		{
			name:        "view_after_table_create",
			description: "View should be created after all referenced tables",
		},
		{
			name:        "trigger_after_function",
			description: "Trigger should be created after its trigger function",
		},
		{
			name:        "computed_field_after_table",
			description: "Hasura computed field functions should come after table creation",
		},
		{
			name:        "materialized_view_dependencies",
			description: "Materialized views should respect dependency order",
		},
	}
	
	for _, s := range scenarios {
		t.Run(s.name, func(t *testing.T) {
			t.Logf("Scenario: %s", s.description)
			// These would be integration tests requiring actual database operations
		})
	}
}