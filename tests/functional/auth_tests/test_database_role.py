import pytest

from dbt.tests.util import run_dbt


class TestDatabaseRole:
    """
    This test addresses https://github.com/dbt-labs/dbt-snowflake/issues/1151

    Run this manually while investigating:
    CREATE DATABASE ROLE BLOCKING_DB_ROLE;
    GRANT ALL PRIVILEGES ON FUTURE TABLES IN DATABASE DBT_TEST TO DATABASE ROLE BLOCKING_DB_ROLE;
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {"my_table.sql": "{{ config(materialized='table') }} select 1 as id"}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"copy_grants": True}}

    def test_database_role(self, project):
        run_dbt(["run"])
        run_dbt(["run"])
        run_dbt(["run", "--full-refresh"])
