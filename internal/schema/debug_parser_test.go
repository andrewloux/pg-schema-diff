package schema

import (
	"encoding/json"
	"regexp"
	"strings"
	"testing"
	
	pg_query "github.com/pganalyze/pg_query_go/v5"
)

func TestDebugParser(t *testing.T) {
	functionDef := `CREATE OR REPLACE FUNCTION get_full_name(user_id integer)
RETURNS text
LANGUAGE sql
STABLE
AS $function$
    SELECT COALESCE(users.first_name || ' ' || users.last_name, users.email)
    FROM users
    WHERE users.id = user_id;
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
}