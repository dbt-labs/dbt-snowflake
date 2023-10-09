import os
import pytest

from dbt.tests.util import run_dbt

freshness_via_metadata_schema_yml = """version: 2
sources:
  - name: test_source
    freshness:
      warn_after: {count: 10, period: hour}
      error_after: {count: 1, period: day}
    schema: "{{ env_var('DBT_TEST_SCHEMA_NAME_VARIABLE') }}"
    tables:
      - name: test_table
"""


class TestGetLastRelationModified:
    @pytest.fixture(scope="class", autouse=True)
    def set_env_vars(self, project):
        os.environ["DBT_TEST_SCHEMA_NAME_VARIABLE"] = project.test_schema
        yield
        del os.environ["DBT_TEST_SCHEMA_NAME_VARIABLE"]

    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": freshness_via_metadata_schema_yml}

    def test_get_last_relation_modified(self, project, set_env_vars):
        project.run_sql(
            f"create table {project.test_schema}.test_table (id integer autoincrement, name varchar(100) not null);"
        )
        run_dbt(["source", "freshness"])
