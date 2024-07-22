from tests.functional.relation_tests.base import RelationOperation


class TestTable(RelationOperation):

    def test_get_create_backup_and_rename_intermediate_sql(self, project):
        args = {
            "database": project.database,
            "schema": project.test_schema,
            "identifier": "my_table",
            "relation_type": "table",
        }
        expected_statement = (
            f"alter table {project.database}.{project.test_schema}.my_table "
            f"rename to {project.database}.{project.test_schema}.my_table__dbt_backup"
        )
        self.assert_operation(project, "test__get_create_backup_sql", args, expected_statement)

        expected_statement = (
            f"alter table {project.database}.{project.test_schema}.my_table__dbt_tmp "
            f"rename to {project.database}.{project.test_schema}.my_table"
        )
        self.assert_operation(
            project, "test__get_rename_intermediate_sql", args, expected_statement
        )
