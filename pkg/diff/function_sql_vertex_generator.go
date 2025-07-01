package diff

import (
	"fmt"

	"github.com/google/go-cmp/cmp"
	"github.com/stripe/pg-schema-diff/internal/schema"
)

type functionSQLVertexGenerator struct {
	// functionsInNewSchemaByName is a map of function name to functions in the new schema.
	// These functions are not necessarily new
	functionsInNewSchemaByName map[string]schema.Function
	
	// Track table alterations happening in this migration so we can ensure
	// functions run after columns they depend on are added
	tableDiffs []tableDiff
}

func newFunctionSqlVertexGenerator(functionsInNewSchemaByName map[string]schema.Function, tableDiffs []tableDiff) sqlVertexGenerator[schema.Function, functionDiff] {
	return legacyToNewSqlVertexGenerator[schema.Function, functionDiff](&functionSQLVertexGenerator{
		functionsInNewSchemaByName: functionsInNewSchemaByName,
		tableDiffs: tableDiffs,
	})
}

func (f *functionSQLVertexGenerator) Add(function schema.Function) ([]Statement, error) {
	var hazards []MigrationHazard
	if !canFunctionDependenciesBeTracked(function) {
		hazards = append(hazards, MigrationHazard{
			Type: MigrationHazardTypeHasUntrackableDependencies,
			Message: "Dependencies, i.e. other functions used in the function body, of non-sql functions cannot be tracked. " +
				"As a result, we cannot guarantee that function dependencies are ordered properly relative to this " +
				"statement. For adds, this means you need to ensure that all functions this function depends on are " +
				"created/altered before this statement.",
		})
	}
	return []Statement{{
		DDL:         function.FunctionDef,
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
		Hazards:     hazards,
	}}, nil
}

func (f *functionSQLVertexGenerator) Delete(function schema.Function) ([]Statement, error) {
	var hazards []MigrationHazard
	if !canFunctionDependenciesBeTracked(function) {
		hazards = append(hazards, MigrationHazard{
			Type: MigrationHazardTypeHasUntrackableDependencies,
			Message: "Dependencies, i.e. other functions used in the function body, of non-sql functions cannot be " +
				"tracked. As a result, we cannot guarantee that function dependencies are ordered properly relative to " +
				"this statement. For drops, this means you need to ensure that all functions this function depends on " +
				"are dropped after this statement.",
		})
	}
	return []Statement{{
		DDL:         fmt.Sprintf("DROP FUNCTION %s", function.GetFQEscapedName()),
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
		Hazards:     hazards,
	}}, nil
}

func (f *functionSQLVertexGenerator) Alter(diff functionDiff) ([]Statement, error) {
	// We are assuming the function has been normalized, i.e., we don't have to worry DependsOnFunctions ordering
	// causing a false positive diff detected.
	if cmp.Equal(diff.old, diff.new) {
		return nil, nil
	}
	return f.Add(diff.new)
}

func canFunctionDependenciesBeTracked(function schema.Function) bool {
	return function.Language == "sql"
}

func (f *functionSQLVertexGenerator) GetSQLVertexId(function schema.Function, diffType diffType) sqlVertexId {
	return buildFunctionVertexId(function.SchemaQualifiedName, diffType)
}

func buildFunctionVertexId(name schema.SchemaQualifiedName, diffType diffType) sqlVertexId {
	return buildSchemaObjVertexId("function", name.GetFQEscapedName(), diffType)
}

func (f *functionSQLVertexGenerator) GetAddAlterDependencies(newFunction, oldFunction schema.Function) ([]dependency, error) {
	// Since functions can just be `CREATE OR REPLACE`, there will never be a case where a function is
	// added and dropped in the same migration. Thus, we don't need a dependency on the delete vertex of a function
	// because there won't be one if it is being added/altered
	var deps []dependency
	for _, depFunction := range newFunction.DependsOnFunctions {
		deps = append(deps, mustRun(f.GetSQLVertexId(newFunction, diffTypeAddAlter)).after(buildFunctionVertexId(depFunction, diffTypeAddAlter)))
	}

	// A function depends on all tables it references
	for _, depTable := range newFunction.DependsOnTables {
		deps = append(deps, mustRun(f.GetSQLVertexId(newFunction, diffTypeAddAlter)).after(
			buildSchemaObjVertexId("table", depTable.GetFQEscapedName(), diffTypeAddAlter),
		))
	}

	// Check if any columns this function references are being added in this migration
	// If so, ensure the function is created after those table alterations
	for _, colRef := range newFunction.ReferencedColumns {
		for _, tableDiff := range f.tableDiffs {
			// Check if this table is being altered and has new columns
			if tableDiff.old.GetName() == colRef.TableName || tableDiff.new.GetName() == colRef.TableName {
				// Check if the referenced column is new (not in old table)
				isNewColumn := false
				if !cmp.Equal(tableDiff.old, schema.Table{}) {
					// Table exists, check if column is new
					found := false
					for _, oldCol := range tableDiff.old.Columns {
						if oldCol.Name == colRef.ColumnName {
							found = true
							break
						}
					}
					if !found {
						// Column not in old table, so it's being added
						isNewColumn = true
					}
				}
				
				if isNewColumn {
					// Make function depend on this table's alteration
					tableName := tableDiff.new.GetFQEscapedName()
					deps = append(deps, mustRun(f.GetSQLVertexId(newFunction, diffTypeAddAlter)).after(
						buildSchemaObjVertexId("table", tableName, diffTypeAddAlter),
					))
				}
			}
		}
	}

	if !cmp.Equal(oldFunction, schema.Function{}) {
		// If the function is being altered:
		// If the old version of the function calls other functions that are being deleted come, those deletions
		// must come after the function is altered, so it is no longer dependent on those dropped functions
		for _, depFunction := range oldFunction.DependsOnFunctions {
			deps = append(deps, mustRun(f.GetSQLVertexId(newFunction, diffTypeAddAlter)).before(buildFunctionVertexId(depFunction, diffTypeDelete)))
		}
	}

	return deps, nil
}

func (f *functionSQLVertexGenerator) GetDeleteDependencies(function schema.Function) ([]dependency, error) {
	var deps []dependency
	for _, depFunction := range function.DependsOnFunctions {
		deps = append(deps, mustRun(f.GetSQLVertexId(function, diffTypeDelete)).before(buildFunctionVertexId(depFunction, diffTypeDelete)))
	}
	return deps, nil
}
