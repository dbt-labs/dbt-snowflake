import os
import pytest

from dbt.cli.main import dbtRunner


freshness_via_metadata_schema_yml = """version: 2
sources:
  - name: test_source
    freshness:
      warn_after: {count: 10, period: hour}
      error_after: {count: 1, period: day}
    schema: "{{ env_var('DBT_GET_LAST_RELATION_TEST_SCHEMA') }}"
    tables:
      - name: test_table
"""


class TestGetLastRelationModified:
    @pytest.fixture(scope="class", autouse=True)
    def set_env_vars(self, project):
        os.environ["DBT_TEST_SCHEMA_NAME_VARIABLE"] = (
            project.test_schema + "_get_last_relation_modified"
        )
        yield
        del os.environ["DBT_GET_LAST_RELATION_TEST_SCHEMA"]

    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": freshness_via_metadata_schema_yml}

    @pytest.fixture(scope="class")
    def custom_schema(self, project, set_env_vars):
        with project.adapter.connection_named("__test"):
            relation = project.adapter.Relation.create(
                database=project.database, schema=os.environ["DBT_GET_LAST_RELATION_TEST_SCHEMA"]
            )
            project.adapter.drop_schema(relation)
            project.adapter.create_schema(relation)

        yield relation.schema

        with project.adapter.connection_named("__test"):
            project.adapter.drop_schema(relation)

    def test_get_last_relation_modified(self, project, set_env_vars, custom_schema):
        project.run_sql(
            f"create table {os.environ['DBT_GET_LAST_RELATION_TEST_SCHEMA']}.test_table (id integer autoincrement, name varchar(100) not null);"
        )

        warning_or_error = False

        def probe(e):
            nonlocal warning_or_error
            if e.info.level in ["warning", "error"]:
                warning_or_error = True

        runner = dbtRunner(callbacks=[probe])
        runner.invoke(["source", "freshness"])

        # The 'source freshness' command should succeed without warnings or errors.
        assert not warning_or_error
