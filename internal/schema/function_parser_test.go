package schema

import (
	"testing"
)

func TestExtractColumnReferences(t *testing.T) {
	tests := []struct {
		name        string
		functionDef string
		want        []TableColumnRef
	}{
		{
			name: "simple function with qualified references",
			functionDef: `CREATE OR REPLACE FUNCTION get_full_name(user_id integer)
RETURNS text
LANGUAGE sql
STABLE
AS $function$
    SELECT COALESCE(users.first_name || ' ' || users.last_name, users.email)
    FROM users
    WHERE users.id = user_id;
$function$`,
			want: []TableColumnRef{
				{TableName: "users", ColumnName: "first_name"},
				{TableName: "users", ColumnName: "last_name"},
				{TableName: "users", ColumnName: "email"},
				{TableName: "users", ColumnName: "id"},
			},
		},
		{
			name: "function with unqualified references",
			functionDef: `CREATE OR REPLACE FUNCTION get_user_info(uid integer)
RETURNS text
LANGUAGE sql
STABLE
AS $$
    SELECT COALESCE(first_name, email)
    FROM users
    WHERE id = uid;
$$`,
			want: []TableColumnRef{
				// Note: unqualified references are harder to track without full context
			},
		},
		{
			name: "function with JOIN",
			functionDef: `CREATE OR REPLACE FUNCTION get_user_products(uid integer)
RETURNS TABLE(product_name text, price numeric)
LANGUAGE sql
STABLE
AS $function$
    SELECT p.name, p.price
    FROM users u
    JOIN products p ON p.created_by = u.id
    WHERE u.id = uid;
$function$`,
			want: []TableColumnRef{
				{TableName: "p", ColumnName: "name"},
				{TableName: "p", ColumnName: "price"},
				{TableName: "p", ColumnName: "created_by"},
				{TableName: "u", ColumnName: "id"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractColumnReferences(tt.functionDef)
			
			// Create maps for easy comparison
			gotMap := make(map[string]bool)
			for _, ref := range got {
				key := ref.TableName + "." + ref.ColumnName
				gotMap[key] = true
			}
			
			wantMap := make(map[string]bool)
			for _, ref := range tt.want {
				key := ref.TableName + "." + ref.ColumnName
				wantMap[key] = true
			}
			
			// Check all expected references are found
			for key := range wantMap {
				if !gotMap[key] {
					t.Errorf("Missing expected reference: %s", key)
				}
			}
			
			// Log what we actually found
			t.Logf("Found references:")
			for _, ref := range got {
				t.Logf("  %s.%s", ref.TableName, ref.ColumnName)
			}
		})
	}
}