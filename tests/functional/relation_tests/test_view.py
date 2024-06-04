from tests.functional.relation_tests.base import RelationOperation


class TestView(RelationOperation):

    def test_get_create_backup_and_rename_intermediate_sql(self, project):
        args = {
            "database": project.database,
            "schema": project.test_schema,
            "identifier": "my_view",
            "relation_type": "view",
        }
        expected_statement = (
            f"alter view {project.database}.{project.test_schema}.my_view "
            f"rename to {project.database}.{project.test_schema}.my_view__dbt_backup"
        )
        self.assert_operation(project, "test__get_create_backup_sql", args, expected_statement)

        expected_statement = (
            f"alter view {project.database}.{project.test_schema}.my_view__dbt_tmp "
            f"rename to {project.database}.{project.test_schema}.my_view"
        )
        self.assert_operation(
            project, "test__get_rename_intermediate_sql", args, expected_statement
        )
