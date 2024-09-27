import pytest

from dbt.tests.util import run_dbt


BLOCKING_DB_ROLE = "BLOCKING_DB_ROLE"


class TestDatabaseRole:
    """
    This test addresses https://github.com/dbt-labs/dbt-snowflake/issues/1151
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {"my_table.sql": "{{ config(materialized='table') }} select 1 as id"}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"+grants": {"select": [BLOCKING_DB_ROLE]}}}

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project):
        project.run_sql(f"CREATE DATABASE ROLE {BLOCKING_DB_ROLE}")
        sql = f"""
        GRANT
            ALL PRIVILEGES ON FUTURE TABLES
            IN DATABASE {project.database}
            TO DATABASE ROLE {BLOCKING_DB_ROLE}
        """
        project.run_sql(sql)
        yield
        project.run_sql(f"DROP DATABASE ROLE {BLOCKING_DB_ROLE}")

    def test_database_role(self, project):
        run_dbt(["run"])
        run_dbt(["run"])
        run_dbt(["run", "--full-refresh"])
