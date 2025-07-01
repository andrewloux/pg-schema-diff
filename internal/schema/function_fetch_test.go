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

func TestFetchFunctions(t *testing.T) {
	// This test verifies that the fix for function fetching works correctly
	// Previously, fetchFunctions was passing 'f' (rune) instead of "f" (string) to GetProcs
	engine, err := pgengine.StartEngine()
	require.NoError(t, err)
	defer engine.Close()

	testCases := []struct {
		name          string
		ddl           []string
		expectedCount int
	}{
		{
			name: "Fetch multiple functions",
			ddl: []string{
				`CREATE FUNCTION add_numbers(a integer, b integer) RETURNS integer
					LANGUAGE SQL
					IMMUTABLE
					RETURNS NULL ON NULL INPUT
					RETURN a + b;`,
				`CREATE FUNCTION multiply_numbers(a integer, b integer) RETURNS integer
					LANGUAGE SQL
					IMMUTABLE
					RETURNS NULL ON NULL INPUT
					RETURN a * b;`,
				`CREATE FUNCTION greet(name text) RETURNS text
					LANGUAGE SQL
					IMMUTABLE
					RETURNS NULL ON NULL INPUT
					RETURN 'Hello, ' || name;`,
			},
			expectedCount: 3,
		},
		{
			name: "Functions in different schemas",
			ddl: []string{
				`CREATE SCHEMA utils;`,
				`CREATE FUNCTION public.pub_func(i integer) RETURNS integer
					LANGUAGE SQL
					IMMUTABLE
					RETURN i * 2;`,
				`CREATE FUNCTION utils.util_func(i integer) RETURNS integer
					LANGUAGE SQL
					IMMUTABLE
					RETURN i * 3;`,
			},
			expectedCount: 2,
		},
		{
			name: "Functions with same name but different signatures",
			ddl: []string{
				`CREATE FUNCTION process(i integer) RETURNS integer
					LANGUAGE SQL
					IMMUTABLE
					RETURN i * 2;`,
				`CREATE FUNCTION process(t text) RETURNS text
					LANGUAGE SQL
					IMMUTABLE
					RETURN UPPER(t);`,
				`CREATE FUNCTION process(a integer, b integer) RETURNS integer
					LANGUAGE SQL
					IMMUTABLE
					RETURN a + b;`,
			},
			expectedCount: 3,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testDb, err := engine.CreateDatabase()
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

			// Verify function count
			assert.Equal(t, tc.expectedCount, len(schema.Functions), 
				"Expected %d functions but got %d. Functions: %+v", 
				tc.expectedCount, len(schema.Functions), schema.Functions)

			// Verify all functions have definitions
			for _, fn := range schema.Functions {
				assert.NotEmpty(t, fn.FunctionDef, "Function %s should have a definition", fn.GetName())
				assert.NotEmpty(t, fn.EscapedName, "Function %s should have a name", fn.GetName())
			}
		})
	}
}

func TestFetchProcedures(t *testing.T) {
	// This test verifies that procedures are also fetched correctly after the fix
	engine, err := pgengine.StartEngine()
	require.NoError(t, err)
	defer engine.Close()

	testDb, err := engine.CreateDatabase()
	require.NoError(t, err)
	defer testDb.DropDB()

	// Create SQL connection
	db, err := sql.Open("pgx", testDb.GetDSN())
	require.NoError(t, err)
	defer db.Close()

	// Create a procedure
	_, err = db.Exec(`
		CREATE PROCEDURE update_counter(INOUT counter integer)
		LANGUAGE plpgsql
		AS $$
		BEGIN
			counter := counter + 1;
		END;
		$$;
	`)
	require.NoError(t, err)

	// Fetch schema
	schema, err := GetSchema(context.Background(), db)
	require.NoError(t, err)

	// Verify procedure was fetched
	assert.Equal(t, 1, len(schema.Procedures), "Expected 1 procedure but got %d", len(schema.Procedures))
	if len(schema.Procedures) > 0 {
		assert.NotEmpty(t, schema.Procedures[0].Def, "Procedure should have a definition")
	}
}