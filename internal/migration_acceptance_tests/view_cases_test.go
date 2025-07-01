package migration_acceptance_tests

import (
	"github.com/stripe/pg-schema-diff/pkg/diff"
)

var viewAcceptanceTestCases = []acceptanceTestCase{
	{
		name: "No-op",
		oldSchemaDDL: []string{
			`
			CREATE TABLE users (id INT PRIMARY KEY, name TEXT);
			CREATE VIEW active_users AS SELECT * FROM users WHERE name IS NOT NULL;
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE users (id INT PRIMARY KEY, name TEXT);
			CREATE VIEW active_users AS SELECT * FROM users WHERE name IS NOT NULL;
			`,
		},
		expectEmptyPlan: true,
	},
	{
		name:         "Create simple view",
		oldSchemaDDL: []string{`CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price DECIMAL);`},
		newSchemaDDL: []string{
			`
			CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price DECIMAL);
			CREATE VIEW expensive_products AS SELECT * FROM products WHERE price > 100;
			`,
		},
	},
	{
		name: "Drop view",
		oldSchemaDDL: []string{
			`
			CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price DECIMAL);
			CREATE VIEW expensive_products AS SELECT * FROM products WHERE price > 100;
			`,
		},
		newSchemaDDL: []string{`CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price DECIMAL);`},
		expectedHazardTypes: []diff.MigrationHazardType{diff.MigrationHazardTypeDeletesData},
	},
	{
		name: "Alter view definition",
		oldSchemaDDL: []string{
			`
			CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price DECIMAL);
			CREATE VIEW expensive_products AS SELECT * FROM products WHERE price > 100;
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price DECIMAL);
			CREATE VIEW expensive_products AS SELECT * FROM products WHERE price > 200;
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{diff.MigrationHazardTypeDeletesData},
	},
	{
		name:         "Create view with dependencies on multiple tables",
		oldSchemaDDL: []string{},
		newSchemaDDL: []string{
			`
			CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, total DECIMAL);
			CREATE TABLE customers (id INT PRIMARY KEY, name TEXT);
			CREATE VIEW customer_orders AS 
				SELECT c.name, o.total 
				FROM orders o 
				JOIN customers c ON o.customer_id = c.id;
			`,
		},
	},
	{
		name:         "Create cascading views",
		oldSchemaDDL: []string{},
		newSchemaDDL: []string{
			`
			CREATE TABLE sales (id INT PRIMARY KEY, amount DECIMAL, sale_date DATE);
			CREATE VIEW monthly_sales AS 
				SELECT DATE_TRUNC('month', sale_date) as month, SUM(amount) as total
				FROM sales 
				GROUP BY month;
			CREATE VIEW quarterly_sales AS 
				SELECT DATE_TRUNC('quarter', month) as quarter, SUM(total) as total
				FROM monthly_sales 
				GROUP BY quarter;
			`,
		},
	},
	{
		name: "Drop view with dependents",
		oldSchemaDDL: []string{
			`
			CREATE TABLE sales (id INT PRIMARY KEY, amount DECIMAL, sale_date DATE);
			CREATE VIEW monthly_sales AS 
				SELECT DATE_TRUNC('month', sale_date) as month, SUM(amount) as total
				FROM sales 
				GROUP BY month;
			CREATE VIEW quarterly_sales AS 
				SELECT DATE_TRUNC('quarter', month) as quarter, SUM(total) as total
				FROM monthly_sales 
				GROUP BY quarter;
			`,
		},
		newSchemaDDL: []string{
			`CREATE TABLE sales (id INT PRIMARY KEY, amount DECIMAL, sale_date DATE);`,
		},
		expectedPlanDDL: []string{
			`DROP VIEW "public"."quarterly_sales"`,
			`DROP VIEW "public"."monthly_sales"`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{diff.MigrationHazardTypeDeletesData},
	},
	{
		name:         "Create view in different schema",
		oldSchemaDDL: []string{`CREATE SCHEMA reporting;`},
		newSchemaDDL: []string{
			`
			CREATE SCHEMA reporting;
			CREATE TABLE public.users (id INT PRIMARY KEY, name TEXT);
			CREATE VIEW reporting.user_report AS SELECT * FROM public.users;
			`,
		},
	},
	{
		name: "Alter view with column changes in base table",
		oldSchemaDDL: []string{
			`
			CREATE TABLE users (id INT PRIMARY KEY, name TEXT);
			CREATE VIEW user_summary AS SELECT id, name FROM users;
			`,
		},
		newSchemaDDL: []string{
			`
			CREATE TABLE users (id INT PRIMARY KEY, name TEXT, email TEXT);
			CREATE VIEW user_summary AS SELECT id, name, email FROM users;
			`,
		},
		expectedHazardTypes: []diff.MigrationHazardType{diff.MigrationHazardTypeDeletesData},
	},
	{
		name:         "Create materialized view-like regular view",
		oldSchemaDDL: []string{},
		newSchemaDDL: []string{
			`
			CREATE TABLE events (id INT PRIMARY KEY, event_type TEXT, created_at TIMESTAMP);
			CREATE VIEW event_counts AS 
				SELECT event_type, COUNT(*) as count 
				FROM events 
				GROUP BY event_type;
			`,
		},
	},
}

func (suite *acceptanceTestSuite) TestViewTestCases() {
	suite.runTestCases(viewAcceptanceTestCases)
}