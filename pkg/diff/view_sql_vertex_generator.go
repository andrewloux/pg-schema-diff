package diff

import (
	"fmt"

	"github.com/google/go-cmp/cmp"
	"github.com/stripe/pg-schema-diff/internal/schema"
)

type viewSQLVertexGenerator struct {
	tablesInNewSchemaByName map[string]schema.Table
	viewsInNewSchemaByName  map[string]schema.View
}

func (v *viewSQLVertexGenerator) Add(view schema.View) ([]Statement, error) {
	stmt := fmt.Sprintf("CREATE VIEW %s AS %s", view.GetFQEscapedName(), view.Definition)
	return []Statement{{
		DDL:         stmt,
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
	}}, nil
}

func (v *viewSQLVertexGenerator) Delete(view schema.View) ([]Statement, error) {
	return []Statement{{
		DDL:         fmt.Sprintf("DROP VIEW %s", view.GetFQEscapedName()),
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
		Hazards: []MigrationHazard{{
			Type:    MigrationHazardTypeDeletesData,
			Message: "Deletes the view",
		}},
	}}, nil
}

func (v *viewSQLVertexGenerator) Alter(diff viewDiff) ([]Statement, error) {
	if cmp.Equal(diff.old, diff.new) {
		return nil, nil
	}
	
	// Views cannot be altered directly, they must be dropped and recreated
	var stmts []Statement
	
	// Drop the old view
	dropStmts, err := v.Delete(diff.old)
	if err != nil {
		return nil, fmt.Errorf("generating drop view statements: %w", err)
	}
	stmts = append(stmts, dropStmts...)
	
	// Create the new view
	createStmts, err := v.Add(diff.new)
	if err != nil {
		return nil, fmt.Errorf("generating create view statements: %w", err)
	}
	stmts = append(stmts, createStmts...)
	
	return stmts, nil
}

func (v *viewSQLVertexGenerator) GetSQLVertexId(view schema.View, diffType diffType) sqlVertexId {
	return buildViewVertexId(view.SchemaQualifiedName, diffType)
}

func buildViewVertexId(name schema.SchemaQualifiedName, diffType diffType) sqlVertexId {
	return buildSchemaObjVertexId("view", name.GetFQEscapedName(), diffType)
}

func (v *viewSQLVertexGenerator) GetAddAlterDependencies(newView, oldView schema.View) ([]dependency, error) {
	var deps []dependency
	
	// A view depends on all tables it references
	for _, depTable := range newView.DependsOnTables {
		deps = append(deps, mustRun(v.GetSQLVertexId(newView, diffTypeAddAlter)).after(
			buildSchemaObjVertexId("table", depTable.GetFQEscapedName(), diffTypeAddAlter),
		))
	}
	
	// A view depends on all other views it references
	for _, depView := range newView.DependsOnViews {
		// Skip self-references (shouldn't happen but be safe)
		if depView.GetFQEscapedName() != newView.GetFQEscapedName() {
			deps = append(deps, mustRun(v.GetSQLVertexId(newView, diffTypeAddAlter)).after(
				buildViewVertexId(depView, diffTypeAddAlter),
			))
		}
	}
	
	// If altering, ensure old dependencies are deleted after this view is altered
	if !cmp.Equal(oldView, schema.View{}) {
		for _, depTable := range oldView.DependsOnTables {
			if !contains(newView.DependsOnTables, depTable) {
				deps = append(deps, mustRun(v.GetSQLVertexId(newView, diffTypeAddAlter)).before(
					buildSchemaObjVertexId("table", depTable.GetFQEscapedName(), diffTypeDelete),
				))
			}
		}
		
		for _, depView := range oldView.DependsOnViews {
			if !contains(newView.DependsOnViews, depView) {
				deps = append(deps, mustRun(v.GetSQLVertexId(newView, diffTypeAddAlter)).before(
					buildViewVertexId(depView, diffTypeDelete),
				))
			}
		}
	}
	
	return deps, nil
}

func (v *viewSQLVertexGenerator) GetDeleteDependencies(view schema.View) ([]dependency, error) {
	var deps []dependency
	
	// When deleting a view, it must be deleted before any tables it depends on
	for _, depTable := range view.DependsOnTables {
		deps = append(deps, mustRun(v.GetSQLVertexId(view, diffTypeDelete)).before(
			buildSchemaObjVertexId("table", depTable.GetFQEscapedName(), diffTypeDelete),
		))
	}
	
	// When deleting a view, it must be deleted before any views it depends on
	for _, depView := range view.DependsOnViews {
		// Skip self-references (shouldn't happen but be safe)
		if depView.GetFQEscapedName() != view.GetFQEscapedName() {
			deps = append(deps, mustRun(v.GetSQLVertexId(view, diffTypeDelete)).before(
				buildViewVertexId(depView, diffTypeDelete),
			))
		}
	}
	
	return deps, nil
}

func contains(names []schema.SchemaQualifiedName, name schema.SchemaQualifiedName) bool {
	for _, n := range names {
		if n.GetFQEscapedName() == name.GetFQEscapedName() {
			return true
		}
	}
	return false
}