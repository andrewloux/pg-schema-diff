package schema

import (
	"encoding/json"
	"regexp"
	"strings"
	"testing"
	
	pg_query "github.com/pganalyze/pg_query_go/v5"
)

func TestDebugParser(t *testing.T) {
	functionDef := `CREATE OR REPLACE FUNCTION get_employee_info(emp_id integer)
RETURNS text
LANGUAGE sql
STABLE
AS $function$
    SELECT COALESCE(employees.first_name || ' ' || employees.last_name, employees.email)
    FROM employees
    WHERE employees.id = emp_id;
$function$`

	// Extract the function body
	bodyRe := regexp.MustCompile(`(?is)AS\s+\$[^$]*\$(.*)\$[^$]*\$`)
	matches := bodyRe.FindStringSubmatch(functionDef)
	if len(matches) < 2 {
		t.Fatal("Could not extract function body")
	}
	
	body := strings.TrimSpace(matches[1])
	t.Logf("Function body: %s", body)
	
	// Parse the function body
	result, err := pg_query.Parse(body)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	
	// Pretty print the parse tree as JSON
	jsonBytes, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		t.Fatalf("JSON marshal error: %v", err)
	}
	
	t.Logf("Parse tree:\n%s", string(jsonBytes))
	
	// Verify the parse tree contains expected elements
	if len(result.Stmts) != 1 {
		t.Errorf("Expected 1 statement, got %d", len(result.Stmts))
	}
	
	// Test that extractColumnReferences works on this function
	refs := extractColumnReferences(functionDef)
	t.Logf("Found %d column references", len(refs))
	
	// We should find references to employees table columns
	expectedColumns := map[string]bool{
		"first_name": false,
		"last_name":  false,
		"email":      false,
		"id":         false,
	}
	
	for _, ref := range refs {
		if ref.TableName == "employees" {
			if _, ok := expectedColumns[ref.ColumnName]; ok {
				expectedColumns[ref.ColumnName] = true
			}
		}
	}
	
	for col, found := range expectedColumns {
		if !found {
			t.Errorf("Expected to find reference to employees.%s", col)
		}
	}
}