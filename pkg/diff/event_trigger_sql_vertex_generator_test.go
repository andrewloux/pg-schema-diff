package diff

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stripe/pg-schema-diff/internal/schema"
)

func TestEventTriggerSQLVertexGenerator_Add(t *testing.T) {
	gen := &eventTriggerSQLVertexGenerator{}
	
	et := schema.EventTrigger{
		Name:  "log_ddl",
		Event: "ddl_command_end",
		Function: schema.SchemaQualifiedName{
			SchemaName:  "public",
			EscapedName: "\"log_ddl_command\"",
		},
		Enabled: "O",
		Tags:    []string{"CREATE TABLE", "ALTER TABLE"},
	}
	
	stmts, err := gen.Add(et)
	assert.NoError(t, err)
	assert.Len(t, stmts, 1)
	
	expectedSQL := `CREATE EVENT TRIGGER "log_ddl" ON ddl_command_end
    WHEN TAG IN ('CREATE TABLE', 'ALTER TABLE')
    EXECUTE FUNCTION "public"."log_ddl_command"();`
	assert.Equal(t, expectedSQL, stmts[0].DDL)
}

func TestEventTriggerSQLVertexGenerator_Delete(t *testing.T) {
	gen := &eventTriggerSQLVertexGenerator{}
	
	et := schema.EventTrigger{
		Name: "log_ddl",
	}
	
	stmts, err := gen.Delete(et)
	assert.NoError(t, err)
	assert.Len(t, stmts, 1)
	assert.Equal(t, `DROP EVENT TRIGGER IF EXISTS "log_ddl"`, stmts[0].DDL)
}

func TestEventTriggerSQLVertexGenerator_Alter(t *testing.T) {
	gen := &eventTriggerSQLVertexGenerator{}
	
	oldET := schema.EventTrigger{
		Name:  "log_ddl",
		Event: "ddl_command_end",
		Function: schema.SchemaQualifiedName{
			SchemaName:  "public",
			EscapedName: "\"old_func\"",
		},
	}
	
	newET := schema.EventTrigger{
		Name:  "log_ddl",
		Event: "ddl_command_end",
		Function: schema.SchemaQualifiedName{
			SchemaName:  "public",
			EscapedName: "\"new_func\"",
		},
		Tags: []string{"CREATE TABLE"},
	}
	
	diff := eventTriggerDiff{
		oldAndNew: oldAndNew[schema.EventTrigger]{
			old: oldET,
			new: newET,
		},
	}
	
	stmts, err := gen.Alter(diff)
	assert.NoError(t, err)
	assert.Len(t, stmts, 2)
	
	// Should drop then recreate
	assert.Equal(t, `DROP EVENT TRIGGER IF EXISTS "log_ddl"`, stmts[0].DDL)
	assert.Contains(t, stmts[1].DDL, `CREATE EVENT TRIGGER "log_ddl"`)
}