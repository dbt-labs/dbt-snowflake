from tests.functional.relation_tests.base import RelationOperation


class TestTable(RelationOperation):

    def test_get_rename_table_sql(self, project):
        args = {
            "database": project.database,
            "schema": project.test_schema,
            "identifier": "my_table",
            "relation_type": "table",
            "new_name": "my_new_table",
        }
        expected_statement = (
            f"alter table {project.database}.{project.test_schema}.my_table "
            f"rename to {project.database}.{project.test_schema}.my_new_table"
        )
        self.assert_operation(project, "test__get_rename_sql", args, expected_statement)
