import pytest
from dbt.tests.util import run_dbt, check_relations_equal

import os

models__override_warehouse_sql = """
{{ config(snowflake_warehouse=env_var('SNOWFLAKE_TEST_ALT_WAREHOUSE', 'DBT_TEST_ALT'), materialized='table') }}
select current_warehouse() as warehouse
"""

models__expected_warehouse_sql = """
{{ config(materialized='table') }}
select '{{ env_var("SNOWFLAKE_TEST_ALT_WAREHOUSE", "DBT_TEST_ALT") }}' as warehouse
"""

models__invalid_warehouse_sql = """
{{ config(snowflake_warehouse='DBT_TEST_DOES_NOT_EXIST') }}
select current_warehouse() as warehouse
"""

project_config_models__override_warehouse_sql = """
{{ config(materialized='table') }}
select current_warehouse() as warehouse
"""

project_config_models__expected_warehouse_sql = """
{{ config(materialized='table') }}
select '{{ env_var("SNOWFLAKE_TEST_ALT_WAREHOUSE", "DBT_TEST_ALT") }}' as warehouse
"""

project_config_models__warehouse_sql = """
{{ config(materialized='table') }}
select current_warehouse() as warehouse
"""


class TestModelWarehouse:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "override_warehouse.sql": models__override_warehouse_sql,
            "expected_warehouse.sql": models__expected_warehouse_sql,
            "invalid_warehouse.sql": models__invalid_warehouse_sql,
        }

    def test_snowflake_override_ok(self, project):
        run_dbt(
            [
                "run",
                "--models",
                "override_warehouse",
                "expected_warehouse",
            ]
        )
        check_relations_equal(project.adapter, ["OVERRIDE_WAREHOUSE", "EXPECTED_WAREHOUSE"])

    def test_snowflake_override_noexist(self, project):
        run_dbt(["run", "--models", "invalid_warehouse"], expect_pass=False)


class TestConfigWarehouse:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "override_warehouse.sql": project_config_models__override_warehouse_sql,
            "expected_warehouse.sql": project_config_models__expected_warehouse_sql,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "models": {
                "test": {
                    "snowflake_warehouse": os.getenv(
                        "SNOWFLAKE_TEST_ALT_WAREHOUSE", "DBT_TEST_ALT"
                    ),
                },
            },
        }

    def test_snowflake_override_ok(self, project):
        run_dbt(
            [
                "run",
                "--models",
                "override_warehouse",
                "expected_warehouse",
            ]
        )
        check_relations_equal(project.adapter, ["OVERRIDE_WAREHOUSE", "EXPECTED_WAREHOUSE"])


class TestInvalidConfigWarehouse:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "invalid_warehouse.sql": project_config_models__warehouse_sql,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "models": {
                "test": {"snowflake_warehouse": "DBT_TEST_DOES_NOT_EXIST"},
            },
        }

    def test_snowflake_override_invalid(self, project):
        result = run_dbt(["run", "--models", "invalid_warehouse"], expect_pass=False)
        assert "Object does not exist, or operation cannot be performed" in result[0].message


class TestValidConfigWarehouse:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "valid_warehouse.sql": project_config_models__warehouse_sql,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "models": {
                "test": {
                    "snowflake_warehouse": "DBT_TESTING",
                },
            },
        }

    def test_snowflake_warehouse_valid(self, project):
        result = run_dbt(["run", "--models", "valid_warehouse"])
        assert "DBT_TESTING" in result[0].node.config.get("snowflake_warehouse")
