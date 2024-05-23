from tests.functional.relation_tests.base import RelationOperation


class TestView(RelationOperation):

    def test_get_rename_view_sql(self, project):
        args = {
            "database": project.database,
            "schema": project.test_schema,
            "identifier": "my_view",
            "relation_type": "view",
            "new_name": "my_new_view",
        }
        expected_statement = (
            f"alter view {project.database}.{project.test_schema}.my_view "
            f"rename to {project.database}.{project.test_schema}.my_new_view"
        )
        self.assert_operation(project, "test__get_rename_sql", args, expected_statement)
