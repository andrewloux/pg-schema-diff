package diff

import (
	"fmt"
	"strings"

	"github.com/google/go-cmp/cmp"
	"github.com/stripe/pg-schema-diff/internal/schema"
)

type eventTriggerSQLVertexGenerator struct {
	newEventTriggersByName map[string]schema.EventTrigger
	oldEventTriggersByName map[string]schema.EventTrigger
}

func newEventTriggerSQLVertexGenerator(oldEventTriggers, newEventTriggers []schema.EventTrigger) *eventTriggerSQLVertexGenerator {
	oldEventTriggersByName := make(map[string]schema.EventTrigger)
	for _, et := range oldEventTriggers {
		oldEventTriggersByName[et.Name] = et
	}
	
	newEventTriggersByName := make(map[string]schema.EventTrigger)
	for _, et := range newEventTriggers {
		newEventTriggersByName[et.Name] = et
	}
	
	return &eventTriggerSQLVertexGenerator{
		oldEventTriggersByName: oldEventTriggersByName,
		newEventTriggersByName: newEventTriggersByName,
	}
}

func (et *eventTriggerSQLVertexGenerator) Add(e schema.EventTrigger) ([]Statement, error) {
	createStmt := fmt.Sprintf("CREATE EVENT TRIGGER %s ON %s",
		schema.EscapeIdentifier(e.Name),
		e.Event)
	
	// Add WHEN clause with tags if present
	if len(e.Tags) > 0 {
		quotedTags := make([]string, len(e.Tags))
		for i, tag := range e.Tags {
			quotedTags[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(tag, "'", "''"))
		}
		createStmt += fmt.Sprintf("\n    WHEN TAG IN (%s)", strings.Join(quotedTags, ", "))
	}
	
	createStmt += fmt.Sprintf("\n    EXECUTE FUNCTION %s();", e.Function.GetFQEscapedName())
	
	return []Statement{{
		DDL:         createStmt,
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
	}}, nil
}

func (et *eventTriggerSQLVertexGenerator) Delete(e schema.EventTrigger) ([]Statement, error) {
	return []Statement{{
		DDL:         fmt.Sprintf("DROP EVENT TRIGGER IF EXISTS %s", schema.EscapeIdentifier(e.Name)),
		Timeout:     statementTimeoutDefault,
		LockTimeout: lockTimeoutDefault,
	}}, nil
}

func (et *eventTriggerSQLVertexGenerator) Alter(diff eventTriggerDiff) ([]Statement, error) {
	if cmp.Equal(diff.old, diff.new) {
		return nil, nil
	}
	
	// Event triggers cannot be directly altered - must drop and recreate
	stmts := []Statement{}
	
	dropStmts, err := et.Delete(diff.old)
	if err != nil {
		return nil, err
	}
	stmts = append(stmts, dropStmts...)
	
	createStmts, err := et.Add(diff.new)
	if err != nil {
		return nil, err
	}
	stmts = append(stmts, createStmts...)
	
	return stmts, nil
}

func (et *eventTriggerSQLVertexGenerator) GetSQLVertexId(eventTrigger schema.EventTrigger, diffType diffType) sqlVertexId {
	return buildEventTriggerVertexId(eventTrigger, diffType)
}

func buildEventTriggerVertexId(eventTrigger schema.EventTrigger, diffType diffType) sqlVertexId {
	return buildSchemaObjVertexId("event_trigger", eventTrigger.Name, diffType)
}

func (et *eventTriggerSQLVertexGenerator) GetAddAlterDependencies(newET, oldET schema.EventTrigger) ([]dependency, error) {
	var deps []dependency
	
	// Event triggers depend on their functions
	deps = append(deps, mustRun(et.GetSQLVertexId(newET, diffTypeAddAlter)).after(
		buildFunctionVertexId(newET.Function, diffTypeAddAlter),
	))
	
	return deps, nil
}

func (et *eventTriggerSQLVertexGenerator) GetDeleteDependencies(eventTrigger schema.EventTrigger) ([]dependency, error) {
	var deps []dependency
	
	// Must delete event triggers before their functions
	deps = append(deps, mustRun(et.GetSQLVertexId(eventTrigger, diffTypeDelete)).before(
		buildFunctionVertexId(eventTrigger.Function, diffTypeDelete),
	))
	
	return deps, nil
}