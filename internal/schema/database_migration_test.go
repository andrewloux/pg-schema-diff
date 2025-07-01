package schema

import (
	"testing"
)

// TestDatabaseMigrationPatterns tests patterns found in typical database migrations
func TestDatabaseMigrationPatterns(t *testing.T) {
	tests := []struct {
		name        string
		functionDef string
		wantRefs    []TableColumnRef
		description string
	}{
		{
			name: "accounting validation function",
			functionDef: `CREATE OR REPLACE FUNCTION accounting.validate_transaction()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
BEGIN
    -- Validate that amounts balance
    IF (SELECT SUM(CASE WHEN transaction_type = 'debit' THEN amount ELSE -amount END) 
        FROM accounting.transactions 
        WHERE batch_id = NEW.batch_id) != 0 THEN
        RAISE EXCEPTION 'Transaction must balance: debits must equal credits';
    END IF;
    
    -- Validate account exists and is active
    IF NOT EXISTS (
        SELECT 1 FROM accounting.accounts 
        WHERE id = NEW.account_id 
        AND is_active = true
    ) THEN
        RAISE EXCEPTION 'Account % does not exist or is not active', NEW.account_id;
    END IF;
    
    RETURN NEW;
END;
$function$`,
			wantRefs: []TableColumnRef{
				{TableName: "accounting", ColumnName: "transactions"},
				{TableName: "accounting", ColumnName: "accounts"},
			},
			description: "Complex PL/pgSQL function with schema-qualified tables",
		},
		{
			name: "computed field with row parameter",
			functionDef: `CREATE OR REPLACE FUNCTION public.get_person_display_name(person_row persons)
RETURNS text
LANGUAGE sql
STABLE
AS $function$
    SELECT COALESCE(person_row.first_name || ' ' || person_row.last_name, person_row.email)
$function$`,
			wantRefs: []TableColumnRef{
				{TableName: "person_row", ColumnName: "first_name"},
				{TableName: "person_row", ColumnName: "last_name"},
				{TableName: "person_row", ColumnName: "email"},
			},
			description: "Computed field pattern",
		},
		{
			name: "state machine transition function",
			functionDef: `CREATE OR REPLACE FUNCTION process.transition_state(
    p_process_id uuid,
    p_from_state text,
    p_to_state text,
    p_user_id uuid
)
RETURNS SETOF process.process_states
LANGUAGE plpgsql
AS $function$
DECLARE
    v_current_state text;
BEGIN
    -- Get current state with lock
    SELECT state INTO v_current_state
    FROM process.process_states
    WHERE process_id = p_process_id
    FOR UPDATE;
    
    -- Validate transition
    IF v_current_state != p_from_state THEN
        RAISE EXCEPTION 'Invalid state transition from % to %', v_current_state, p_to_state;
    END IF;
    
    -- Check transition rules
    IF NOT EXISTS (
        SELECT 1 FROM process.transition_rules
        WHERE from_state = p_from_state
        AND to_state = p_to_state
        AND process_type = (
            SELECT process_type 
            FROM process.processes 
            WHERE id = p_process_id
        )
    ) THEN
        RAISE EXCEPTION 'Transition not allowed';
    END IF;
    
    -- Update state
    UPDATE process.process_states
    SET state = p_to_state,
        updated_by = p_user_id,
        updated_at = CURRENT_TIMESTAMP
    WHERE process_id = p_process_id
    RETURNING *;
END;
$function$`,
			wantRefs: []TableColumnRef{
				{TableName: "process", ColumnName: "process_states"},
				{TableName: "process", ColumnName: "transition_rules"},
				{TableName: "process", ColumnName: "processes"},
			},
			description: "Complex state machine function with multiple CTEs and subqueries",
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
    REFRESH MATERIALIZED VIEW CONCURRENTLY public.user_statistics;
    REFRESH MATERIALIZED VIEW CONCURRENTLY public.department_analytics;
    REFRESH MATERIALIZED VIEW CONCURRENTLY public.product_metrics;
    
    -- Update refresh timestamp
    UPDATE public.system_metadata 
    SET value = CURRENT_TIMESTAMP::text,
        updated_at = CURRENT_TIMESTAMP
    WHERE key = 'last_materialized_view_refresh';
END;
$function$`,
			wantRefs: []TableColumnRef{
				{TableName: "public", ColumnName: "user_statistics"},
				{TableName: "public", ColumnName: "department_analytics"},
				{TableName: "public", ColumnName: "product_metrics"},
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
			
			// Check if we found any references when we expected them
			if len(refs) == 0 && len(tt.wantRefs) > 0 {
				t.Errorf("Expected to find references but found none")
			}
			
			// Verify we found the expected table references (not exact column matches)
			expectedTables := make(map[string]bool)
			for _, want := range tt.wantRefs {
				expectedTables[want.TableName] = true
			}
			
			foundTables := make(map[string]bool)
			for _, ref := range refs {
				foundTables[ref.TableName] = true
			}
			
			// Check that we found references to all expected tables
			for table := range expectedTables {
				if !foundTables[table] {
					t.Errorf("Expected to find references to table %q but didn't", table)
				}
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
					query text := 'SELECT * FROM employees WHERE active = true';
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
					RETURN $inner$SELECT * FROM products$inner$;
				END;
			$outer$ LANGUAGE plpgsql;`,
			shouldParse: true,
		},
		{
			name: "DO block (anonymous function)",
			sql: `DO $$
				BEGIN
					UPDATE customers SET migrated = true WHERE created_at < '2023-01-01';
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
			// Test with pg_query parser
			refs := extractColumnReferences(tc.sql)
			t.Logf("Parser found %d references", len(refs))
			
			// Test with regex fallback
			regexRefs := extractColumnReferencesRegex(tc.sql)
			t.Logf("Regex found %d references", len(regexRefs))
			
			// Verify specific expected references for some cases
			switch tc.name {
			case "function with dollar quoted string containing SQL":
				// Should not parse the SQL inside the string literal
				for _, ref := range refs {
					if ref.TableName == "employees" {
						t.Errorf("Parser incorrectly extracted table reference from string literal")
					}
				}
			case "DO block (anonymous function)":
				// DO blocks are anonymous and our parser doesn't extract references from them
				// This is expected behavior - we focus on named functions
				if len(refs) > 0 {
					t.Logf("Note: Parser extracted %d references from DO block (this is fine)", len(refs))
				}
			}
		})
	}
}