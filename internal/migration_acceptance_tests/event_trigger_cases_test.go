package migration_acceptance_tests

var eventTriggerAcceptanceTestCases = []acceptanceTestCase{
	{
		name: "No-op with event trigger",
		oldSchemaDDL: []string{
			`CREATE FUNCTION log_ddl_command() RETURNS event_trigger AS $$
			BEGIN
				RAISE NOTICE 'DDL command executed';
			END;
			$$ LANGUAGE plpgsql;`,
			`CREATE EVENT TRIGGER log_ddl ON ddl_command_end EXECUTE FUNCTION log_ddl_command();`,
		},
		newSchemaDDL: []string{
			`CREATE FUNCTION log_ddl_command() RETURNS event_trigger AS $$
			BEGIN
				RAISE NOTICE 'DDL command executed';
			END;
			$$ LANGUAGE plpgsql;`,
			`CREATE EVENT TRIGGER log_ddl ON ddl_command_end EXECUTE FUNCTION log_ddl_command();`,
		},
		// Event triggers require superuser privileges, but we should test the SQL generation
		expectEmptyPlan: true,
	},
	{
		name: "Create event trigger",
		oldSchemaDDL: []string{
			`CREATE FUNCTION log_ddl_command() RETURNS event_trigger AS $$
			BEGIN
				RAISE NOTICE 'DDL command executed';
			END;
			$$ LANGUAGE plpgsql;`,
		},
		newSchemaDDL: []string{
			`CREATE FUNCTION log_ddl_command() RETURNS event_trigger AS $$
			BEGIN
				RAISE NOTICE 'DDL command executed';
			END;
			$$ LANGUAGE plpgsql;`,
			`CREATE EVENT TRIGGER log_ddl ON ddl_command_end EXECUTE FUNCTION log_ddl_command();`,
		},
	},
	{
		name: "Drop event trigger",
		oldSchemaDDL: []string{
			`CREATE FUNCTION log_ddl_command() RETURNS event_trigger AS $$
			BEGIN
				RAISE NOTICE 'DDL command executed';
			END;
			$$ LANGUAGE plpgsql;`,
			`CREATE EVENT TRIGGER log_ddl ON ddl_command_end EXECUTE FUNCTION log_ddl_command();`,
		},
		newSchemaDDL: []string{
			`CREATE FUNCTION log_ddl_command() RETURNS event_trigger AS $$
			BEGIN
				RAISE NOTICE 'DDL command executed';
			END;
			$$ LANGUAGE plpgsql;`,
		},
	},
	{
		name: "Create event trigger with tags",
		oldSchemaDDL: []string{
			`CREATE FUNCTION log_table_ddl() RETURNS event_trigger AS $$
			BEGIN
				RAISE NOTICE 'Table DDL command executed';
			END;
			$$ LANGUAGE plpgsql;`,
		},
		newSchemaDDL: []string{
			`CREATE FUNCTION log_table_ddl() RETURNS event_trigger AS $$
			BEGIN
				RAISE NOTICE 'Table DDL command executed';
			END;
			$$ LANGUAGE plpgsql;`,
			`CREATE EVENT TRIGGER log_table_changes ON ddl_command_end 
			WHEN TAG IN ('CREATE TABLE', 'ALTER TABLE', 'DROP TABLE')
			EXECUTE FUNCTION log_table_ddl();`,
		},
	},
}

func (suite *acceptanceTestSuite) TestEventTriggerTestCases() {
	suite.runTestCases(eventTriggerAcceptanceTestCases)
}