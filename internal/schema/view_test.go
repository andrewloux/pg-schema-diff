package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestViewNormalize(t *testing.T) {
	tests := []struct {
		name     string
		views    []View
		expected []View
	}{
		{
			name: "Sort views alphabetically",
			views: []View{
				{
					SchemaQualifiedName: SchemaQualifiedName{
						SchemaName:  "public",
						EscapedName: "\"view_c\"",
					},
					Definition:      "SELECT * FROM table_c",
					DependsOnTables: []SchemaQualifiedName{},
					DependsOnViews:  []SchemaQualifiedName{},
				},
				{
					SchemaQualifiedName: SchemaQualifiedName{
						SchemaName:  "public",
						EscapedName: "\"view_a\"",
					},
					Definition:      "SELECT * FROM table_a",
					DependsOnTables: []SchemaQualifiedName{},
					DependsOnViews:  []SchemaQualifiedName{},
				},
				{
					SchemaQualifiedName: SchemaQualifiedName{
						SchemaName:  "public",
						EscapedName: "\"view_b\"",
					},
					Definition:      "SELECT * FROM table_b",
					DependsOnTables: []SchemaQualifiedName{},
					DependsOnViews:  []SchemaQualifiedName{},
				},
			},
			expected: []View{
				{
					SchemaQualifiedName: SchemaQualifiedName{
						SchemaName:  "public",
						EscapedName: "\"view_a\"",
					},
					Definition:      "SELECT * FROM table_a",
					DependsOnTables: []SchemaQualifiedName{},
					DependsOnViews:  []SchemaQualifiedName{},
				},
				{
					SchemaQualifiedName: SchemaQualifiedName{
						SchemaName:  "public",
						EscapedName: "\"view_b\"",
					},
					Definition:      "SELECT * FROM table_b",
					DependsOnTables: []SchemaQualifiedName{},
					DependsOnViews:  []SchemaQualifiedName{},
				},
				{
					SchemaQualifiedName: SchemaQualifiedName{
						SchemaName:  "public",
						EscapedName: "\"view_c\"",
					},
					Definition:      "SELECT * FROM table_c",
					DependsOnTables: []SchemaQualifiedName{},
					DependsOnViews:  []SchemaQualifiedName{},
				},
			},
		},
		{
			name: "Sort view dependencies",
			views: []View{
				{
					SchemaQualifiedName: SchemaQualifiedName{
						SchemaName:  "public",
						EscapedName: "\"my_view\"",
					},
					Definition: "SELECT * FROM table_a",
					DependsOnTables: []SchemaQualifiedName{
						{SchemaName: "public", EscapedName: "\"table_c\""},
						{SchemaName: "public", EscapedName: "\"table_a\""},
						{SchemaName: "public", EscapedName: "\"table_b\""},
					},
					DependsOnViews: []SchemaQualifiedName{
						{SchemaName: "public", EscapedName: "\"view_z\""},
						{SchemaName: "public", EscapedName: "\"view_x\""},
						{SchemaName: "public", EscapedName: "\"view_y\""},
					},
				},
			},
			expected: []View{
				{
					SchemaQualifiedName: SchemaQualifiedName{
						SchemaName:  "public",
						EscapedName: "\"my_view\"",
					},
					Definition: "SELECT * FROM table_a",
					DependsOnTables: []SchemaQualifiedName{
						{SchemaName: "public", EscapedName: "\"table_a\""},
						{SchemaName: "public", EscapedName: "\"table_b\""},
						{SchemaName: "public", EscapedName: "\"table_c\""},
					},
					DependsOnViews: []SchemaQualifiedName{
						{SchemaName: "public", EscapedName: "\"view_x\""},
						{SchemaName: "public", EscapedName: "\"view_y\""},
						{SchemaName: "public", EscapedName: "\"view_z\""},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := Schema{Views: tt.views}
			normalized := schema.Normalize()
			assert.Equal(t, tt.expected, normalized.Views)
		})
	}
}

func TestViewGetName(t *testing.T) {
	view := View{
		SchemaQualifiedName: SchemaQualifiedName{
			SchemaName:  "reporting",
			EscapedName: "\"sales_summary\"",
		},
		Definition: "SELECT * FROM sales",
	}

	assert.Equal(t, "\"reporting\".\"sales_summary\"", view.GetName())
}